import base64
import binascii
import functools
import pickle
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any, Union, Callable, Hashable, Dict

import attr

BRANCH_SKIP_EXCEPT = "branch_skip_except"
SKIP_EXCEPT_HACK = "brickflow_hack_skip_all"
RETURN_VALUE_KEY = "return_value"


# TODO: remove
class ContextMode(Enum):
    databricks = "databricks"
    not_databricks = "not_databricks"


class BrickflowBuiltInTaskVariables(Enum):
    # key is the {{ }} and value is the key
    job_id = "brickflow_job_id"
    run_id = "brickflow_run_id"
    start_date = "brickflow_start_date"
    start_time = "brickflow_start_time"
    task_retry_count = "brickflow_task_retry_count"
    parent_run_id = "brickflow_parent_run_id"
    task_key = "brickflow_task_key"


class BrickflowInternalVariables(Enum):
    workflow_id = "brickflow_internal_workflow_name"
    task_id = "brickflow_internal_task_name"
    only_run_tasks = "brickflow_internal_only_run_tasks"


def bind_variable(builtin: BrickflowBuiltInTaskVariables) -> Callable:
    def wrapper(f: Callable) -> Callable:
        @functools.wraps(f)
        def func(*args, **kwargs):  # type: ignore
            _self: Context = args[0]
            debug = kwargs["debug"]
            f(*args, **kwargs)  # no-op
            if _self.dbutils is not None:
                return _self.dbutils_widget_get_or_else(builtin.value, debug)
            return debug

        return func

    return wrapper


class TaskComsObjectResult(Enum):
    NO_RESULTS = "NO_RESULTS"


@dataclass
class BrickflowTaskComsObject:
    # Use to encode any hashable value into bytes and then pickle.unloads
    @dataclass(frozen=True)
    class _TaskComsObject:
        value: Hashable

    _value: Any = attr.field(on_setattr=attr.setters.frozen)
    _task_coms_obj: _TaskComsObject = field(init=False)

    def __post_init__(self) -> None:
        self._task_coms_obj = self._TaskComsObject(self._value)

    @property
    def value(self) -> Any:
        return self._task_coms_obj.value

    @property
    def to_encoded_value(self) -> str:
        results_bytes = pickle.dumps(self._task_coms_obj)
        return base64.b64encode(results_bytes).decode("utf-8")

    @classmethod
    def from_encoded_value(
        cls, encoded_value: Union[str, bytes]
    ) -> "BrickflowTaskComsObject":
        try:
            _encoded_value = (
                encoded_value
                if isinstance(encoded_value, bytes)
                else encoded_value.encode("utf-8")
            )
            b64_bytes = base64.b64decode(_encoded_value)
            return cls(pickle.loads(b64_bytes).value)
        except binascii.Error:
            _decoded_value = (
                encoded_value.decode("utf-8")
                if isinstance(encoded_value, bytes)
                else encoded_value
            )
            return cls(_decoded_value)


@dataclass
class BrickflowTaskComsDict:
    task_id: str
    task_coms: "BrickflowTaskComs"

    def __getitem__(self, key: str) -> Any:
        # fake behavior in airflow for: {{ ti.xcom_pull(task_ids='task_id')['arg'] }}
        return self.task_coms.get(task_id=self.task_id, key=key)


@dataclass(frozen=True)
class BrickflowTaskComs:
    dbutils: Optional[Any] = None
    storage: Dict[str, Any] = field(init=False, default_factory=lambda: {})

    @staticmethod
    def _key(task_id: str, key: str) -> str:
        return f"{task_id}::{key}"

    def put(self, task_id: str, key: str, value: Any) -> None:
        encoded_value = BrickflowTaskComsObject(value).to_encoded_value
        if self.dbutils is not None:
            self.dbutils.jobs.taskValues.set(key, encoded_value)
        else:
            # TODO: logging using local task coms
            self.storage[self._key(task_id, key)] = encoded_value

    def get(self, task_id: str, key: str = None) -> Any:
        if key is None:
            return BrickflowTaskComsDict(task_id=task_id, task_coms=self)
        if self.dbutils is not None:
            encoded_value = self.dbutils.jobs.taskValues.get(
                key=key, taskKey=task_id, debugValue="debug"
            )
            return BrickflowTaskComsObject.from_encoded_value(encoded_value).value
        else:
            # TODO: logging using local task coms
            encoded_value = self.storage[self._key(task_id, key)]
            return BrickflowTaskComsObject.from_encoded_value(encoded_value).value


class Context:
    def __init__(self) -> None:
        # Order of init matters todo: fix this

        self._dbutils: Optional[Any] = None
        self._spark: Optional[Any] = None
        self._task_coms: BrickflowTaskComs
        self._current_task: Optional[str] = None
        self._configure()

    def __new__(cls) -> "Context":
        if not hasattr(cls, "instance"):
            cls.instance = super(Context, cls).__new__(cls)
        return cls.instance  # noqa

    def _configure(self) -> None:
        # testing purposes only
        self._set_spark_session()
        self._configure_dbutils()
        self._task_coms = BrickflowTaskComs(self._dbutils)

    @property
    def current_task(self) -> Optional[str]:
        return self._current_task

    def set_current_task(self, task_key: str) -> None:
        self._current_task = task_key

    def reset_current_task(self) -> None:
        self._current_task = None

    def get_return_value(self, task_key: Union[str, Callable]) -> Any:
        task_key = task_key.__name__ if callable(task_key) else task_key
        return self.task_coms.get(task_key, RETURN_VALUE_KEY)

    def skip_all_except(self, branch_task: Union[Callable, str]) -> None:
        if self._current_task is None:
            raise RuntimeError("Current task is empty unable to skip...")
        branch_task_key = (
            branch_task.__name__
            if callable(branch_task) and hasattr(branch_task, "__name__") is True
            else branch_task
        )
        self._task_coms.put(self._current_task, BRANCH_SKIP_EXCEPT, branch_task_key)

    def skip_all_following(self) -> None:
        if self._current_task is None:
            raise RuntimeError("Current task is empty unable to skip...")
        self._task_coms.put(self._current_task, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK)

    @property
    def task_coms(self) -> BrickflowTaskComs:
        return self._task_coms

    @bind_variable(BrickflowBuiltInTaskVariables.task_key)
    def task_key(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.task_retry_count)
    def task_retry_count(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.run_id)
    def run_id(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.job_id)
    def job_id(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.parent_run_id)
    def parent_run_id(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.start_date)
    def start_date(self, *, debug: Optional[str]) -> str:
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.start_time)
    def start_time(self, *, debug: Optional[str]) -> str:
        pass

    @property
    def dbutils(self) -> "DBUtils":  # type: ignore # noqa
        return self._dbutils

    @property
    def spark(self) -> "SparkSession":  # type: ignore # noqa
        return self._spark

    def dbutils_widget_get_or_else(
        self, key: str, debug: Optional[str]
    ) -> Optional[str]:
        try:
            return self.dbutils.widgets.get(key)
        except Exception:
            # todo: log error
            return debug

    def _set_spark_session(self) -> None:
        try:
            from pyspark.sql import SparkSession

            self._spark = SparkSession.getActiveSession()
        except ImportError:
            # todo: log error
            pass

    def _configure_dbutils(self) -> ContextMode:
        try:
            from pyspark.dbutils import DBUtils

            self._dbutils = DBUtils(self.spark)
            return ContextMode.databricks
        except ImportError:
            # todo: log error
            return ContextMode.not_databricks


ctx = Context()
