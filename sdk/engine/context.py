import functools
from enum import Enum
from typing import Optional, Any


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


class BrickflowInternalVariables:
    workflow_id = "brickflow_internal_workflow_name"
    task_id = "brickflow_internal_task_name"


def bind_variable(builtin: BrickflowBuiltInTaskVariables):
    def wrapper(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
            _self: Context = args[0]
            print(_self.dbutils)
            print(builtin.value)
            if _self.dbutils is not None:
                return _self._dbutils.widgets.get(builtin.value)
            return kwargs["default"]

        return func

    return wrapper


class Context:

    def __init__(self):
        self._mode = self._get_context_mode()
        self._dbutils: Optional[Any] = None

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

    def _get_context_mode(self):
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            self._dbutils = DBUtils(spark)
            return ContextMode.databricks
        except ImportError as e:
            return ContextMode.not_databricks
