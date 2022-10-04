import functools
import inspect
import logging
import numbers
from enum import Enum
from typing import Callable, List, Dict, Union, Optional

from brickflow.context import (
    BrickflowBuiltInTaskVariables,
    BrickflowInternalVariables,
    ctx,
    BRANCH_SKIP_EXCEPT,
    SKIP_EXCEPT_HACK,
    TaskComsObjectResult,
    RETURN_VALUE_KEY,
)
from brickflow.engine import ROOT_NODE
from brickflow.engine.compute import Compute
from brickflow.engine.utils import resolve_py4j_logging


def with_brickflow_logger(f):
    @functools.wraps(f)
    def func(*args, **kwargs):
        _self = args[0]
        logger = logging.getLogger()  # Logger
        back_up_logging_handlers = logger.handlers
        logger.handlers = []
        logger_handler = logging.StreamHandler()  # Handler for the logger
        logger.addHandler(logger_handler)

        # First, generic formatter:
        logger_handler.setFormatter(
            logging.Formatter(
                f"[%(asctime)s] [%(levelname)s] [brickflow:{_self.name}] {{%(module)s.py:%(lineno)d}} - %(message)s"
            )
        )
        resolve_py4j_logging()
        resp = f(*args, **kwargs)

        logger.handlers = []
        for handler in back_up_logging_handlers:
            logger.addHandler(handler)

        return resp

    return func


class TaskNotFoundError(Exception):
    pass


class AnotherActiveTaskError(Exception):
    pass


class TaskAlreadyExistsError(Exception):
    pass


class UnsupportedBrickflowTriggerRuleError(Exception):
    pass


class InvalidTaskSignatureDefinition(Exception):
    pass


class TaskValueHandler:
    @staticmethod
    def get_task_value_key(f: Callable):
        pass


class BrickflowTriggerRule(Enum):
    ALL_SUCCESS = "all_success"
    NONE_FAILED = "none_failed"

    @classmethod
    def is_valid(cls, trigger_rule):
        for k in cls:
            if trigger_rule == k.value:
                return True
        return False


class TaskType(Enum):
    NOTEBOOK = "notebook_task"
    SQL = "sql_task"
    AIRFLOW_TASK = "airflow_task"


class TaskParameters:
    def __init__(self, params):
        self._params = params

    @property
    def params(self):
        return self._params


class EmailNotifications:
    def __init__(
        self,
        on_failure: List[str] = None,
        on_success: List[str] = None,
        on_start: List[str] = None,
    ):
        self._on_start = on_start
        self._on_success = on_success
        self._on_failure = on_failure

    def to_tf_dict(self):
        return {
            "on_start": self._on_start,
            "on_failure": self._on_failure,
            "on_success": self._on_success,
        }


class TaskSettings:
    def __init__(
        self,
        email_notifications: EmailNotifications = None,
        timeout_seconds: int = None,
        max_retries: int = None,
        min_retry_interval_millis: int = None,
        retry_on_timeout: int = None,
    ):
        self._retry_on_timeout = retry_on_timeout
        self._min_retry_interval_millis = min_retry_interval_millis
        self._max_retries = max_retries
        self._timeout_seconds = timeout_seconds
        self._email_notifications = email_notifications

    def merge(self, other: "TaskSettings"):
        # overrides top level values
        if other is None:
            return self
        return TaskSettings(
            other._email_notifications or self._email_notifications,
            other._timeout_seconds or self._timeout_seconds or 0,
            other._max_retries or self._max_retries,
            other._min_retry_interval_millis or self._min_retry_interval_millis,
            other._retry_on_timeout or self._retry_on_timeout,
        )

    def to_tf_dict(self):
        email_not = (
            self._email_notifications.to_tf_dict()
            if self._email_notifications is not None
            else {}
        )
        return {
            "email_notifications": email_not,
            "timeout_seconds": self._timeout_seconds,
            "max_retries": self._max_retries,
            "min_retry_interval_millis": self._min_retry_interval_millis,
            "retry_on_timeout": self._retry_on_timeout,
        }


class Task:
    def __init__(
        self,
        task_id,
        task_func: Callable,
        workflow: "Workflow",  # noqa
        compute: "Compute",
        depends_on: Optional[List[Union[Callable, str]]] = None,
        task_type: TaskType = TaskType.NOTEBOOK,
        trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS,
        task_settings: Optional[TaskSettings] = None,
    ):
        self._task_settings = task_settings
        self._trigger_rule = trigger_rule
        self._task_type = task_type
        self._compute = compute
        self._depends_on = depends_on or []
        self._workflow: "Workflow" = workflow  # noqa
        self._task_func = task_func
        self._task_id = task_id

    @property
    def task_settings(self):
        return self._task_settings

    @property
    def parents(self):
        return list(self._workflow.parents(self._task_id))

    @property
    def task_type(self) -> str:
        return self._task_type.value

    @property
    def depends_on(self) -> Optional[List[Union[Callable, str]]]:
        return self._depends_on

    @property
    def builtin_notebook_params(self):
        # 2 braces to escape for 1
        return {i.value: f"{{{{{i.name}}}}}" for i in BrickflowBuiltInTaskVariables}

    @property
    def name(self):
        return self._task_id

    @property
    def brickflow_default_params(self):
        return {
            BrickflowInternalVariables.workflow_id.value: self._workflow.name,
            BrickflowInternalVariables.task_id.value: self.name,
        }

    def get_tf_obj(self, entrypoint):
        from brickflow.tf.databricks import JobTaskNotebookTask

        if self._task_type in [TaskType.NOTEBOOK, TaskType.AIRFLOW_TASK]:
            return JobTaskNotebookTask(
                notebook_path=entrypoint,
                base_parameters={
                    **self.builtin_notebook_params,
                    **self.brickflow_default_params,
                    **(self.custom_task_parameters or {}),
                },
            )

    def is_valid_task_signature(self):
        # only supports kwonlyargs with defaults
        spec: inspect.FullArgSpec = inspect.getfullargspec(self._task_func)
        sig: inspect.Signature = inspect.signature(self._task_func)
        signature_error_msg = (
            "Task signatures only supports kwargs with defaults. or catch all varkw **kwargs"
            "For example def execute(*, variable_a=None, variable_b=None, **kwargs). "
            f"Please fix function def {self._task_func.__name__}{sig}: ..."
        )
        kwargs_default_error_msg = (
            f"Keyword arguments must be either None, String or number. "
            f"Please handle booleans via strings. "
            f"Please fix function def {self._task_func.__name__}{sig}: ..."
        )

        valid_case = spec.args == [] and spec.varargs is None and spec.defaults is None
        for _, v in spec.kwonlydefaults.items():
            if not (isinstance(v, (numbers.Number, str)) or v is None):
                raise InvalidTaskSignatureDefinition(kwargs_default_error_msg)
        if valid_case:
            return

        raise InvalidTaskSignatureDefinition(signature_error_msg)

    @property
    def custom_task_parameters(self) -> Dict[str, Union[str, None, numbers.Number]]:
        spec: inspect.FullArgSpec = inspect.getfullargspec(self._task_func)
        if spec.kwonlydefaults is None:
            return {}
        return {k: str(v) for k, v in spec.kwonlydefaults.items()}

    def should_skip(self):
        node_skip_checks = []
        for parent in self.parents:
            if parent != ROOT_NODE:
                try:
                    task_to_not_skip = ctx.task_coms.get(parent, BRANCH_SKIP_EXCEPT)
                    if self.name != task_to_not_skip:
                        # set this task to skip hack to keep to empty to trigger failure
                        # key look up will fail
                        node_skip_checks.append(True)
                    else:
                        node_skip_checks.append(False)
                except Exception:
                    # ignore errors as it probably doesnt exist
                    # TODO: log errors
                    node_skip_checks.append(False)
        if not node_skip_checks:
            return False
        if self._trigger_rule == BrickflowTriggerRule.ALL_SUCCESS:
            return any(node_skip_checks)
        if self._trigger_rule == BrickflowTriggerRule.NONE_FAILED:
            return all(node_skip_checks)

    @with_brickflow_logger
    def execute(self):
        ctx.set_current_task(self.name)
        if self.should_skip() is True:
            logging.info("Skipping task... %s", self.name)
            ctx.task_coms.put(self.name, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK)
            ctx.reset_current_task()
            return
        return_value = TaskComsObjectResult.NO_RESULTS
        if self._task_type == TaskType.AIRFLOW_TASK:
            resolve_py4j_logging()
            resp = self._workflow.airflow_dag.execute(task_id=self.name)
            if self._workflow.airflow_dag.get_task(self.name).do_xcom_push is True:
                return_value = resp
        else:
            # TODO: Inject context object
            return_value = self._task_func()
        ctx.task_coms.put(self.name, RETURN_VALUE_KEY, return_value)
        ctx.reset_current_task()
