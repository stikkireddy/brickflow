from __future__ import annotations

import inspect
import logging
import numbers
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Dict, Union, Optional, Any, Tuple, TYPE_CHECKING
from decouple import config

from brickflow.context import (
    BrickflowBuiltInTaskVariables,
    BrickflowInternalVariables,
    ctx,
    BRANCH_SKIP_EXCEPT,
    SKIP_EXCEPT_HACK,
    TaskComsObjectResult,
    RETURN_VALUE_KEY,
)
from brickflow.engine import ROOT_NODE, with_brickflow_logger
from brickflow.engine.compute import Compute

if TYPE_CHECKING:
    from brickflow.engine.workflow import Workflow  # pragma: no cover


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


class NoCallableTaskError(Exception):
    pass


class BrickflowTaskEnvVars(Enum):
    BRICKFLOW_SELECT_TASKS = "BRICKFLOW_SELECT_TASKS"


class BrickflowTriggerRule(Enum):
    ALL_SUCCESS = "all_success"
    NONE_FAILED = "none_failed"


class TaskType(Enum):
    NOTEBOOK = "notebook_task"
    SQL = "sql_task"
    CUSTOM_PYTHON_TASK = "custom_python_task"


@dataclass(frozen=True)
class EmailNotifications:
    on_failure: Optional[List[str]] = None
    on_success: Optional[List[str]] = None
    on_start: Optional[List[str]] = None

    def to_tf_dict(self) -> Dict[str, Optional[List[str]]]:
        return {
            "on_start": self.on_start,
            "on_failure": self.on_failure,
            "on_success": self.on_success,
        }


@dataclass(frozen=True)
class TaskSettings:
    email_notifications: Optional[EmailNotifications] = None
    timeout_seconds: Optional[int] = None
    max_retries: Optional[int] = None
    min_retry_interval_millis: Optional[int] = None
    retry_on_timeout: Optional[bool] = None

    def merge(self, other: Optional["TaskSettings"]) -> "TaskSettings":
        # overrides top level values
        if other is None:
            return self
        return TaskSettings(
            other.email_notifications or self.email_notifications,
            other.timeout_seconds or self.timeout_seconds or 0,
            other.max_retries or self.max_retries,
            other.min_retry_interval_millis or self.min_retry_interval_millis,
            other.retry_on_timeout or self.retry_on_timeout,
        )

    def to_tf_dict(
        self,
    ) -> Dict[
        str,
        Optional[str]
        | Optional[int]
        | Optional[bool]
        | Optional[Dict[str, Optional[List[str]]]],
    ]:
        email_not = (
            self.email_notifications.to_tf_dict()
            if self.email_notifications is not None
            else {}
        )
        return {
            "email_notifications": email_not,
            "timeout_seconds": self.timeout_seconds,
            "max_retries": self.max_retries,
            "min_retry_interval_millis": self.min_retry_interval_millis,
            "retry_on_timeout": self.retry_on_timeout,
        }


@dataclass
class CustomTaskResponse:
    response: Any
    push_return_value: bool = True


@dataclass(frozen=True)
class Task:
    task_id: str
    task_func: Callable
    workflow: Workflow  # noqa
    compute: Optional[Compute] = None
    description: Optional[str] = None
    depends_on: List[Union[Callable, str]] = field(default_factory=lambda: [])
    task_type: TaskType = TaskType.NOTEBOOK
    trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS
    task_settings: Optional[TaskSettings] = None
    custom_execute_callback: Optional[Callable] = None

    def __post_init__(self) -> None:
        self.is_valid_task_signature()

    @property
    def task_func_name(self) -> str:
        return self.task_func.__name__

    @property
    def parents(self) -> List[str]:
        return list(self.workflow.parents(self.task_id))

    @property
    def task_type_str(self) -> str:
        return self.task_type.value

    @property
    def builtin_notebook_params(self) -> Dict[str, str]:
        # 2 braces to escape for 1
        return {i.value: f"{{{{{i.name}}}}}" for i in BrickflowBuiltInTaskVariables}

    @property
    def name(self) -> str:
        return self.task_id

    @property
    def brickflow_default_params(self) -> Dict[str, str]:
        return {
            BrickflowInternalVariables.workflow_id.value: self.workflow.name,
            # 2 braces to escape 1
            BrickflowInternalVariables.task_id.value: f"{{{{{BrickflowBuiltInTaskVariables.task_key.name}}}}}",
            BrickflowInternalVariables.only_run_tasks.value: "",
        }

    def get_tf_obj(self, entrypoint) -> "JobTaskNotebookTask":  # type: ignore  # noqa
        from brickflow.tf.databricks import JobTaskNotebookTask

        if self.task_type in [TaskType.NOTEBOOK, TaskType.CUSTOM_PYTHON_TASK]:
            return JobTaskNotebookTask(
                notebook_path=entrypoint,
                base_parameters={
                    **self.builtin_notebook_params,
                    **self.brickflow_default_params,
                    **(self.custom_task_parameters or {}),  # type: ignore
                },
            )

    # TODO: error if star isn't there
    def is_valid_task_signature(self) -> None:
        # only supports kwonlyargs with defaults
        spec: inspect.FullArgSpec = inspect.getfullargspec(self.task_func)
        sig: inspect.Signature = inspect.signature(self.task_func)
        signature_error_msg = (
            "Task signatures only supports kwargs with defaults. or catch all varkw **kwargs"
            "For example def execute(*, variable_a=None, variable_b=None, **kwargs). "
            f"Please fix function def {self.task_func_name}{sig}: ..."
        )
        kwargs_default_error_msg = (
            f"Keyword arguments must be either None, String or number. "
            f"Please handle booleans via strings. "
            f"Please fix function def {self.task_func_name}{sig}: ..."
        )

        valid_case = spec.args == [] and spec.varargs is None and spec.defaults is None
        for _, v in (spec.kwonlydefaults or {}).items():
            if not (isinstance(v, (numbers.Number, str)) or v is None):
                raise InvalidTaskSignatureDefinition(kwargs_default_error_msg)
        if valid_case:
            return

        raise InvalidTaskSignatureDefinition(signature_error_msg)

    @property
    def custom_task_parameters(self) -> Dict[str, Union[str, None, numbers.Number]]:
        spec: inspect.FullArgSpec = inspect.getfullargspec(self.task_func)
        if spec.kwonlydefaults is None:
            return {}
        return {k: str(v) for k, v in spec.kwonlydefaults.items()}

    @staticmethod
    def _get_skip_with_reason(cond: bool, reason: str) -> Tuple[bool, Optional[str]]:
        if cond is True:
            return cond, reason
        return cond, None

    def should_skip(self) -> Tuple[bool, Optional[str]]:
        # return true or false and reason
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
            return False, None
        if self.trigger_rule == BrickflowTriggerRule.NONE_FAILED:
            # by default a task failure automatically skips
            return self._get_skip_with_reason(
                all(node_skip_checks),
                "At least one task before this were not successful",
            )
        # default is BrickflowTriggerRule.ALL_SUCCESS
        return self._get_skip_with_reason(
            any(node_skip_checks), "All tasks before this were not successful"
        )

    def _skip_because_not_selected(self) -> Tuple[bool, Optional[str]]:
        selected_tasks = ctx.dbutils_widget_get_or_else(
            BrickflowInternalVariables.only_run_tasks.value,
            config(BrickflowTaskEnvVars.BRICKFLOW_SELECT_TASKS.value, ""),
        )
        if selected_tasks is None or selected_tasks == "":
            return False, None
        selected_task_list = selected_tasks.split(",")
        if self.name not in selected_task_list:
            return (
                True,
                f"This task: {self.name} is not a selected task: {selected_task_list}",
            )
        return False, None

    @with_brickflow_logger
    def execute(self) -> Any:
        # Workflow is:
        #   1. Check to see if there selected tasks and if there are is this task in the list
        #   2. Check to see if the previous task is skipped and trigger rule.
        #   3. Check to see if this a custom python task and execute it
        #   4. Execute the task function
        ctx.set_current_task(self.name)
        _select_task_skip, _select_task_skip_reason = self._skip_because_not_selected()
        if _select_task_skip is True:
            # check if this task is skipped due to task selection
            logging.info(
                "Skipping task... %s for reason: %s",
                self.name,
                _select_task_skip_reason,
            )
            ctx.reset_current_task()
            return
        _skip, reason = self.should_skip()
        if _skip is True:
            logging.info("Skipping task... %s for reason: %s", self.name, reason)
            ctx.task_coms.put(self.name, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK)
            ctx.reset_current_task()
            return
        return_value = TaskComsObjectResult.NO_RESULTS
        if (
            self.task_type == TaskType.CUSTOM_PYTHON_TASK
            and self.custom_execute_callback is not None
        ):
            resp: CustomTaskResponse = self.custom_execute_callback(self)
            if resp.push_return_value is True:
                return_value = resp.response
        else:
            # TODO: Inject context object
            return_value = self.task_func()
        ctx.task_coms.put(self.name, RETURN_VALUE_KEY, return_value)
        ctx.reset_current_task()
        return return_value
