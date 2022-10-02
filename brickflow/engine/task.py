import functools
import inspect
import logging
import numbers
from enum import Enum
from typing import Callable, List, Dict, Union, Optional

from brickflow.adapters import BRANCH_SKIP_EXCEPT
from brickflow.engine import ROOT_NODE
from brickflow.engine.compute import Compute
from brickflow.engine.context import BrickflowBuiltInTaskVariables, BrickflowInternalVariables, ctx


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
        logger_handler.setFormatter(logging.Formatter(
            f'[%(asctime)s] [%(levelname)s] [brickflow:{_self.name}] {{%(module)s.py:%(lineno)d}} - %(message)s'))
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


class UnsupportedBrickflowTriggerRuleError(Exception):
    pass


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


class InvalidTaskSignatureDefinition(Exception):
    pass


class Task:

    def __init__(self, task_id, task_func: Callable, workflow: 'Workflow', compute: 'Compute',
                 depends_on: Optional[List[Union[Callable, str]]] = None,
                 task_type: TaskType = TaskType.NOTEBOOK,
                 trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS):
        self._trigger_rule = trigger_rule
        self._task_type = task_type
        self._compute = compute
        self._depends_on = depends_on or []
        self._workflow: 'Workflow' = workflow
        self._task_func = task_func
        self._task_id = task_id

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
            BrickflowInternalVariables.workflow_id: self._workflow.name,
            BrickflowInternalVariables.task_id: self.name,
        }

    def get_tf_obj(self, entrypoint):
        from brickflow.tf.databricks import JobTaskNotebookTask
        if self._task_type == TaskType.NOTEBOOK or self._task_type == TaskType.AIRFLOW_TASK:
            return JobTaskNotebookTask(
                notebook_path=entrypoint,
                base_parameters={**self.builtin_notebook_params, **self.brickflow_default_params,
                                 **(self.custom_task_parameters or {})}
            )

    def is_valid_task_signature(self):
        # only supports kwonlyargs with defaults
        spec: inspect.FullArgSpec = inspect.getfullargspec(self._task_func)
        sig: inspect.Signature = inspect.signature(self._task_func)
        signature_error_msg = "Task signatures only supports kwargs with defaults. or catch all varkw **kwargs" \
                              "For example def execute(*, variable_a=None, variable_b=None, **kwargs). " \
                              f"Please fix function def {self._task_func.__name__}{sig}: ..."
        kwargs_default_error_msg = f"Keyword arguments must be either None, String or number. " \
                                   f"Please handle booleans via strings. " \
                                   f"Please fix function def {self._task_func.__name__}{sig}: ..."

        valid_case = spec.args == [] and spec.varargs is None and spec.defaults is None
        for k, v in spec.kwonlydefaults.items():
            if not (isinstance(v, numbers.Number) or isinstance(v, str) or v is None):
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
                except Exception as e:
                    # ignore errors as it probably doesnt exist
                    node_skip_checks.append(False)
        if not node_skip_checks:
            return False
        if self._trigger_rule == BrickflowTriggerRule.ALL_SUCCESS:
            return any(node_skip_checks)
        if self._trigger_rule == BrickflowTriggerRule.NONE_FAILED:
            return all(node_skip_checks)

    @with_brickflow_logger
    def execute(self):
        if self.should_skip() is True:
            ctx.task_coms.put(self.name, BRANCH_SKIP_EXCEPT, "brickflow_hack_skip_all")
            return
        if self._task_type == TaskType.AIRFLOW_TASK:
            from brickflow.adapters.airflow_1_10 import resolve_py4j_logging
            resolve_py4j_logging()
            self._workflow.airflow_dag.execute(task_id=self.name)
        else:
            # TODO: Inject context object
            self._task_func()
