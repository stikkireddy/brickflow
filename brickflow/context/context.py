import base64
import binascii
import functools
import pickle
from enum import Enum
from typing import Optional, Any, Union, Callable

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


def bind_variable(builtin: BrickflowBuiltInTaskVariables):
    def wrapper(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
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


class BrickflowTaskComsObject:
    # Use to encode any hashable value into bytes and then pickle.unloads
    class _TaskComsObject:
        def __init__(self, value: Any):
            self._value = value

        @property
        def return_value(self):
            return self._value

    def __init__(self, value):
        self._task_results = self._TaskComsObject(value)

    @property
    def return_value(self):
        return self._task_results.return_value

    @property
    def to_encoded_value(self) -> str:
        results_bytes = pickle.dumps(self._task_results)
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
            return cls(pickle.loads(b64_bytes).return_value)
        except binascii.Error:
            return cls(encoded_value)


class BrickflowTaskComsDict:
    def __init__(self, task_id, task_coms: "BrickflowTaskComs"):
        self._task_id = task_id
        self._task_coms = task_coms

    def __getitem__(self, key):
        # fake behavior in airflow for: {{ ti.xcom_pull(task_ids='task_id')['arg'] }}
        return self._task_coms.get(task_id=self._task_id, key=key)


class BrickflowTaskComs:
    def __init__(self, dbutils=None):
        self._storage = {}
        self._dbutils = dbutils

    @staticmethod
    def _key(task_id, key):
        return f"{task_id}::{key}"

    def put(self, task_id, key, value: Any):
        encoded_value = BrickflowTaskComsObject(value).to_encoded_value
        if self._dbutils is not None:
            self._dbutils.jobs.taskValues.set(key, encoded_value)
        else:
            # TODO: logging using local task coms
            self._storage[self._key(task_id, key)] = encoded_value

    def get(self, task_id, key=None):
        if key is None:
            return BrickflowTaskComsDict(task_id=task_id, task_coms=self)
        if self._dbutils is not None:
            encoded_value = self._dbutils.jobs.taskValues.get(
                key=key, taskKey=task_id, debugValue="debug"
            )
            return BrickflowTaskComsObject.from_encoded_value(
                encoded_value
            ).return_value
        else:
            # TODO: logging using local task coms
            encoded_value = self._storage[self._key(task_id, key)]
            return BrickflowTaskComsObject.from_encoded_value(
                encoded_value
            ).return_value


class Context:
    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(Context, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        # Order of init matters todo: fix this
        self._dbutils: Optional[Any] = None
        self._task_coms = None
        self._current_task = None
        self._configure()

    def _configure(self):
        # testing purposes only
        self._configure_dbutils()
        self._task_coms = BrickflowTaskComs(self._dbutils)

    @property
    def current_task(self):
        return self._current_task

    def set_current_task(self, task_key):
        self._current_task = task_key

    def reset_current_task(self):
        self._current_task = None

    def get_return_value(self, task_key: Union[str, Callable]):
        task_key = task_key.__name__ if callable(task_key) else task_key
        return self.task_coms.get(task_key, RETURN_VALUE_KEY)

    def skip_all_except(self, branch_task: Union[Callable, str]):
        branch_task_key = (
            branch_task.__name__ if callable(branch_task) is True else branch_task
        )
        self._task_coms.put(self._current_task, BRANCH_SKIP_EXCEPT, branch_task_key)

    def skip_all_following(self):
        self._task_coms.put(self._current_task, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK)

    @property
    def task_coms(self) -> BrickflowTaskComs:
        return self._task_coms

    @bind_variable(BrickflowBuiltInTaskVariables.task_key)
    def task_key(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.task_retry_count)
    def task_retry_count(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.run_id)
    def run_id(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.job_id)
    def job_id(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.parent_run_id)
    def parent_run_id(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.start_date)
    def start_date(self, *, debug=None):
        pass

    @bind_variable(BrickflowBuiltInTaskVariables.start_time)
    def start_time(self, *, debug=None):
        pass

    @property
    def dbutils(self):
        return self._dbutils

    def dbutils_widget_get_or_else(self, key, debug):
        try:
            return self.dbutils.widgets.get(key)
        except Exception:
            # todo: log error
            return debug

    def _configure_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            self._dbutils = DBUtils(spark)
            return ContextMode.databricks
        except ImportError:
            # todo: log error
            return ContextMode.not_databricks


ctx = Context()
