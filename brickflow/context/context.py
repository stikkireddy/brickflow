import functools
from enum import Enum
from typing import Optional, Any, Union, Callable

BRANCH_SKIP_EXCEPT = "branch_skip_except"
SKIP_EXCEPT_HACK = "brickflow_hack_skip_all"


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


def bind_variable(builtin: BrickflowBuiltInTaskVariables):
    def wrapper(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
            _self: Context = args[0]
            debug = kwargs["debug"]
            if _self.dbutils is not None:
                return _self.dbutils_widget_get_or_else(builtin.value, debug)
            return debug

        return func

    return wrapper


class BrickflowTaskComsDict:
    def __init__(self, task_id, task_coms: "BrickflowTaskComs"):
        self._task_id = task_id
        self._task_coms = task_coms

    def __getitem__(self, key):
        # fake behavior in airflow for: {{ ti.xcom_pull(task_ids='task_id')['arg'] }}
        return self._task_coms.get(task_id=self._task_id, key=key)


class BrickflowTaskComs:
    def __init__(self, dbutils):
        self._storage = {}
        self._dbutils = dbutils

    @staticmethod
    def _key(task_id, key):
        return f"{task_id}::{key}"

    def put(self, task_id, key, value):
        if self._dbutils is not None:
            self._dbutils.jobs.taskValues.set(key, value)
        else:
            # TODO: logging using local task coms
            self._storage[self._key(task_id, key)] = value

    def get(self, task_id, key=None):
        if key is None:
            return BrickflowTaskComsDict(task_id=task_id, task_coms=self)
        if self._dbutils is not None:
            return self._dbutils.jobs.taskValues.get(
                key=key, taskKey=task_id, debugValue="debug"
            )
        else:
            # TODO: logging using local task coms
            return self._storage[self._key(task_id, key)]


class Context:
    def __init__(self):
        # Order of init matters todo: fix this
        self._dbutils: Optional[Any] = None

        self._mode = self._get_context_mode()
        self._task_coms = BrickflowTaskComs(self._dbutils)
        self._current_task = None

    @property
    def current_task(self):
        return self._current_task

    def set_current_task(self, task_key):
        self._current_task = task_key

    def reset_current_task(self, task_key):
        self._current_task = task_key

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

    def dbutils_widget_get_or_else(self, key, default):
        try:
            return self.dbutils.widgets.get(key)
        except Exception:
            # todo: log error
            return default

    def _get_context_mode(self):
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
