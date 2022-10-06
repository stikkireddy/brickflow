import base64
import functools
import pickle
import sys
from unittest.mock import Mock

from brickflow.context import (
    BrickflowTaskComsObject,
    BrickflowTaskComs,
    ctx,
    Context,
    RETURN_VALUE_KEY,
    BRANCH_SKIP_EXCEPT,
    SKIP_EXCEPT_HACK,
    BrickflowBuiltInTaskVariables,
)


def reset_ctx(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        resp = f(*args, **kwargs)
        ctx._configure()
        return resp

    return wrapper


def fake_task():
    pass


class TestContext:
    def test_brickflow_task_comms_obj(self):
        task_value = "hello-world"
        hidden_comms_obj = BrickflowTaskComsObject._TaskComsObject(task_value)
        b64_data = base64.b64encode(pickle.dumps(hidden_comms_obj)).decode("utf-8")
        comms_obj = BrickflowTaskComsObject(task_value)
        assert comms_obj.to_encoded_value == b64_data
        assert comms_obj.return_value == task_value
        assert (
            BrickflowTaskComsObject.from_encoded_value(b64_data).return_value
            == task_value
        )
        assert (
            BrickflowTaskComsObject.from_encoded_value(task_value).return_value
            == task_value
        )

    def test_brickflow_task_comms(self):
        task_comms = BrickflowTaskComs()
        task_id = "test-task"
        key = "test-key"
        value1 = "value 1"
        value2 = "value 2"
        task_comms.put(task_id, key, value1)
        assert task_comms.get(task_id, key) == value1
        task_comms.put(task_id, key, value2)
        assert task_comms.get(task_id, key) == value2
        assert task_comms.get(task_id)[key] == value2

    def test_brickflow_task_comms_dbutils(self):
        dbutils_mock = Mock()
        task_comms = BrickflowTaskComs(dbutils_mock)
        task_id = "test-task"
        key = "test-key"
        value1 = "value 1"
        value2 = "value 2"
        task_comms_v1 = BrickflowTaskComsObject(value1)
        task_comms_v2 = BrickflowTaskComsObject(value2)
        dbutils_mock.jobs.taskValues.get.return_value = task_comms_v1.to_encoded_value
        task_comms.put(task_id, key, value1)
        dbutils_mock.jobs.taskValues.set.assert_called_once_with(
            f"{key}", task_comms_v1.to_encoded_value
        )
        assert task_comms.get(task_id, key) == value1
        task_comms.put(task_id, key, value2)
        dbutils_mock.jobs.taskValues.set.assert_called_with(
            f"{key}", task_comms_v2.to_encoded_value
        )
        dbutils_mock.jobs.taskValues.get.return_value = task_comms_v2.to_encoded_value
        assert task_comms.get(task_id, key) == value2
        assert task_comms.get(task_id)[key] == value2

    @reset_ctx
    def test_context_obj(self):
        task_key = "hello-world"
        # assert that its a singleton
        dbutils_mock = Mock()
        assert id(ctx) == id(Context())
        ctx._dbutils = dbutils_mock
        assert ctx.current_task is None
        ctx.set_current_task(task_key)
        assert ctx.current_task == task_key
        ctx.reset_current_task()
        assert ctx.current_task is None

        for e in BrickflowBuiltInTaskVariables:
            dbutils_mock.widgets.get.return_value = "actual"
            assert getattr(ctx, e.name)(debug="test") == "actual"
            dbutils_mock.widgets.get.assert_called_with(e.value)

        ctx._dbutils = None
        assert ctx.task_key(debug=task_key) == task_key

    @reset_ctx
    def test_context_obj_task_coms(self):
        task_coms_mock = Mock()
        task_key = "some_task"
        some_return_value = "some_value"
        ctx._task_coms = task_coms_mock
        task_coms_mock.get.return_value = some_return_value
        assert ctx.get_return_value(task_key) == some_return_value
        task_coms_mock.get.assert_called_once_with(task_key, RETURN_VALUE_KEY)

        ctx.skip_all_except(task_key)
        task_coms_mock.put.assert_called_with(None, BRANCH_SKIP_EXCEPT, task_key)
        ctx.skip_all_except(fake_task)
        task_coms_mock.put.assert_called_with(
            None, BRANCH_SKIP_EXCEPT, fake_task.__name__
        )

        ctx.skip_all_following()
        task_coms_mock.put.assert_called_with(
            None, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK
        )

    def test_dbutils_widget_get_or_else(self):
        key = "random-key"
        value = "default"
        assert ctx.dbutils_widget_get_or_else(key, value) == value

    def test_configure_dbutils(self):
        dbutils_class_mock = Mock()
        spark_mock = Mock()
        pyspark = Mock()
        pyspark_dbutils = Mock()
        pyspark_sql = Mock()
        sys.modules["pyspark"] = pyspark
        sys.modules["pyspark.dbutils"] = pyspark_dbutils
        sys.modules["pyspark.sql"] = pyspark_sql
        sys.modules["pyspark.dbutils.DBUtils"] = dbutils_class_mock
        sys.modules["pyspark.sql.SparkSession"] = spark_mock
        ctx._configure_dbutils()
        sys.modules.pop("pyspark")
        sys.modules.pop("pyspark.dbutils")
        sys.modules.pop("pyspark.sql")
        sys.modules.pop("pyspark.dbutils.DBUtils")
        sys.modules.pop("pyspark.sql.SparkSession")
