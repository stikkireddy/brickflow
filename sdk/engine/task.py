import inspect
import numbers
from enum import Enum
from typing import Callable, List, Dict, Union

from sdk.engine.compute import Compute
from sdk.engine.context import BrickflowBuiltInTaskVariables, BrickflowInternalVariables
from sdk.tf.databricks import JobTaskNotebookTask


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


class TaskType(Enum):
    NOTEBOOK = "notebook_task"
    SQL = "sql_task"


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
                 depends_on: List[str] = None,
                 task_type: TaskType = TaskType.NOTEBOOK):
        self._task_type = task_type
        self._compute = compute
        self._depends_on = depends_on
        self._workflow = workflow
        self._task_func = task_func
        self._task_id = task_id

    @property
    def task_type(self) -> str:
        return self._task_type.value

    @property
    def builtin_notebook_params(self):
        return {i.value: f"{{{i.name}}}" for i in BrickflowBuiltInTaskVariables}

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
        if self._task_type == TaskType.NOTEBOOK:
            return JobTaskNotebookTask(
                notebook_path=entrypoint,
                base_parameters={**self.builtin_notebook_params, **self.builtin_notebook_params,
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

    def execute(self):
        # TODO: Inject context object
        self._task_func()
