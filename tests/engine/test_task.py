from unittest.mock import Mock, patch

import pytest

from brickflow.context import (
    ctx,
    BRANCH_SKIP_EXCEPT,
    SKIP_EXCEPT_HACK,
    RETURN_VALUE_KEY,
    BrickflowInternalVariables,
)
from brickflow.engine.task import (
    Task,
    InvalidTaskSignatureDefinition,
    EmailNotifications,
    TaskSettings,
    JarTaskLibrary,
    EggTaskLibrary,
    WheelTaskLibrary,
    PypiTaskLibrary,
    MavenTaskLibrary,
    CranTaskLibrary,
    InvalidTaskLibraryError,
    TaskLibrary,
)
from brickflow.tf.databricks import JobTaskNotebookTask
from tests.engine.sample_workflow import (
    wf,
    task_function,
    task_function_nokwargs,
    task_function_2,
    task_function_3,
    task_function_4,
    custom_python_task_push,
)


class TestTask:
    builtin_task_params = {
        "brickflow_job_id": "{{job_id}}",
        "brickflow_run_id": "{{run_id}}",
        "brickflow_start_date": "{{start_date}}",
        "brickflow_start_time": "{{start_time}}",
        "brickflow_task_retry_count": "{{task_retry_count}}",
        "brickflow_parent_run_id": "{{parent_run_id}}",
        "brickflow_task_key": "{{task_key}}",
    }

    def test_builtin_notebook_params(self):
        assert (
            wf.get_task(task_function.__name__).builtin_notebook_params
            == self.builtin_task_params
        )

    def test_builtin_default_params(self):
        assert wf.get_task(task_function.__name__).brickflow_default_params == {
            "brickflow_internal_workflow_name": wf.name,
            "brickflow_internal_task_name": "{{task_key}}",
            "brickflow_internal_only_run_tasks": "",
        }

    def test_custom_task_params(self):
        assert wf.get_task(task_function.__name__).custom_task_parameters == {
            "test": "var"
        }
        assert wf.get_task(task_function_nokwargs.__name__).custom_task_parameters == {}

    def test_task_settings(self):
        assert wf.get_task(task_function.__name__).task_settings is None

    def test_parents(self):
        assert wf.get_task(task_function_2.__name__).parents == ["task_function"]

    def test_task_type(self):
        assert wf.get_task(task_function_2.__name__).task_type_str == "notebook_task"

    def test_depends_on(self):
        assert wf.get_task(task_function_3.__name__).depends_on == ["task_function_2"]
        assert wf.get_task(task_function_2.__name__).depends_on == [task_function]
        assert wf.get_task(task_function.__name__).depends_on == []

    def test_invalid_task_signature(self):
        with pytest.raises(InvalidTaskSignatureDefinition):
            # missing * and kwargs
            @wf.task
            def _fake_task(test):  # noqa
                pass

        with pytest.raises(InvalidTaskSignatureDefinition):
            # missing *
            @wf.task
            def _fake_task(test="test"):  # noqa
                pass

        with pytest.raises(InvalidTaskSignatureDefinition):
            # doesnt support bytes-like
            @wf.task
            def _fake_task(*, test=b"test"):  # noqa
                pass

    @patch("brickflow.context.ctx._task_coms")
    def test_should_skip_false(self, task_coms_mock: Mock):
        task_coms_mock.get.return_value = task_function_3.__name__
        skip, reason = wf.get_task(task_function_3.__name__).should_skip()
        assert skip is False
        assert reason is None
        task_coms_mock.get.assert_called_once()
        ctx._configure()

        task_coms_mock.get.value = task_function.__name__
        task_coms_mock.get.side_effect = Exception("error")
        skip, reason = wf.get_task(task_function_3.__name__).should_skip()
        assert skip is False
        assert reason is None
        ctx._configure()

        skip, reason = wf.get_task(task_function.__name__).should_skip()
        assert skip is False
        assert reason is None
        ctx._configure()

        skip, reason = wf.get_task(task_function_4.__name__).should_skip()
        assert skip is False
        assert reason is None
        ctx._configure()

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_skip_not_selected_task(self, dbutils):
        dbutils.value = "sometihngelse"
        skip, reason = wf.get_task(
            task_function_4.__name__
        )._skip_because_not_selected()
        dbutils.assert_called_once_with(
            BrickflowInternalVariables.only_run_tasks.value, ""
        )
        assert skip is True
        assert reason.startswith(
            f"This task: {task_function_4.__name__} is not a selected task"
        )
        assert wf.get_task(task_function_4.__name__).execute() is None

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_no_skip_selected_task(self, dbutils: Mock):
        dbutils.return_value = task_function_4.__name__
        skip, reason = wf.get_task(
            task_function_4.__name__
        )._skip_because_not_selected()
        dbutils.assert_called_once_with(
            BrickflowInternalVariables.only_run_tasks.value, ""
        )
        assert skip is False
        assert reason is None
        assert wf.get_task(task_function_4.__name__).execute() == task_function_4()

    @patch("brickflow.engine.task.Task._skip_because_not_selected")
    @patch("brickflow.context.ctx._task_coms")
    def test_should_skip_true(
        self, task_coms_mock: Mock, task_skip_selected_mock: Mock
    ):
        task_skip_selected_mock.return_value = (False, None)
        task_coms_mock.get.value = task_function_2.__name__
        skip, reason = wf.get_task(task_function_3.__name__).should_skip()
        assert skip is True
        assert reason == "All tasks before this were not successful"
        task_coms_mock.get.assert_called_once()
        assert wf.get_task(task_function_3.__name__).execute() is None
        task_coms_mock.put.assert_called_once_with(
            task_function_3.__name__, BRANCH_SKIP_EXCEPT, SKIP_EXCEPT_HACK
        )

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    @patch("brickflow.context.ctx._task_coms")
    def test_execute(self, task_coms_mock: Mock, dbutils: Mock):
        dbutils.return_value = ""
        resp = wf.get_task(task_function.__name__).execute()
        task_coms_mock.put.assert_called_once_with(
            task_function.__name__, RETURN_VALUE_KEY, task_function()
        )

        assert resp is task_function()

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    @patch("brickflow.context.ctx._task_coms")
    def test_execute_custom(self, task_coms_mock: Mock, dbutils: Mock):
        dbutils.return_value = ""
        resp = wf.get_task(custom_python_task_push.__name__).execute()
        task_coms_mock.put.assert_called_once_with(
            custom_python_task_push.__name__,
            RETURN_VALUE_KEY,
            custom_python_task_push.__name__,
        )

        assert resp is custom_python_task_push.__name__

    def test_get_tf_obj(self):
        entry_point = "some_entry_point"
        expected_tf_obj = JobTaskNotebookTask(
            notebook_path=entry_point,
            base_parameters={
                "brickflow_job_id": "{{job_id}}",
                "brickflow_run_id": "{{run_id}}",
                "brickflow_start_date": "{{start_date}}",
                "brickflow_start_time": "{{start_time}}",
                "brickflow_task_retry_count": "{{task_retry_count}}",
                "brickflow_parent_run_id": "{{parent_run_id}}",
                "brickflow_task_key": "{{task_key}}",
                "brickflow_internal_workflow_name": "test",
                "brickflow_internal_task_name": "{{task_key}}",
                "brickflow_internal_only_run_tasks": "",
                "test": "var",
            },
        )
        t: Task = wf.get_task(task_function.__name__)
        assert expected_tf_obj == t.get_tf_obj(entry_point)

    def test_email_notifications(self):
        email_arr = ["abc@abc.com"]
        en = EmailNotifications(
            on_start=email_arr,
            on_failure=email_arr,
            on_success=email_arr,
        )
        assert en.to_tf_dict() == {
            "on_start": email_arr,
            "on_failure": email_arr,
            "on_success": email_arr,
        }

    def test_task_settings_tf_dict(self):
        email_arr = ["abc@abc.com"]
        default_int = 10
        default_bool = True
        en = EmailNotifications(
            on_start=email_arr,
            on_failure=email_arr,
            on_success=email_arr,
        )
        ts = TaskSettings(
            email_notifications=en,
            timeout_seconds=default_int,
            max_retries=default_int,
            min_retry_interval_millis=default_int,
            retry_on_timeout=default_bool,
        )
        assert ts.to_tf_dict() == {
            "email_notifications": en.to_tf_dict(),
            "timeout_seconds": default_int,
            "max_retries": default_int,
            "min_retry_interval_millis": default_int,
            "retry_on_timeout": default_bool,
        }

    def test_task_settings_merge(self):
        email_arr = ["abc@abc.com"]
        other_email_arr = ["new@new.com"]
        default_int = 10
        other_default_int = 20
        default_bool = True
        en = EmailNotifications(
            on_start=email_arr,
            on_failure=email_arr,
            on_success=email_arr,
        )
        other_en = EmailNotifications(
            on_start=other_email_arr,
            on_failure=other_email_arr,
        )
        ts = TaskSettings(
            email_notifications=en,
            timeout_seconds=default_int,
            max_retries=default_int,
            min_retry_interval_millis=default_int,
            retry_on_timeout=default_bool,
        )
        other_ts = TaskSettings(
            email_notifications=other_en,
            timeout_seconds=other_default_int,
            max_retries=other_default_int,
            retry_on_timeout=default_bool,
        )

        final_ts = ts.merge(other_ts)
        assert final_ts.to_tf_dict() == {
            "email_notifications": other_en.to_tf_dict(),
            "timeout_seconds": other_default_int,
            "max_retries": other_default_int,
            "min_retry_interval_millis": default_int,
            "retry_on_timeout": default_bool,
        }

        final_ts = ts.merge(None)
        assert final_ts.to_tf_dict() == {
            "email_notifications": en.to_tf_dict(),
            "timeout_seconds": default_int,
            "max_retries": default_int,
            "min_retry_interval_millis": default_int,
            "retry_on_timeout": default_bool,
        }

    def test_task_libraries(self):
        s3_path = "s3://somepath-in-s3"
        repo = "somerepo"
        package = "somepackage"
        coordinates = package
        exclusions = None
        assert JarTaskLibrary(s3_path).dict == {"jar": s3_path}
        assert EggTaskLibrary(s3_path).dict == {"egg": s3_path}
        assert WheelTaskLibrary(s3_path).dict == {"whl": s3_path}
        assert PypiTaskLibrary(package, repo).dict == {
            "pypi": {"package": package, "repo": repo}
        }
        assert MavenTaskLibrary(coordinates, repo, exclusions).dict == {
            "maven": {
                "coordinates": coordinates,
                "repo": repo,
                "exclusions": exclusions,
            }
        }
        assert CranTaskLibrary(package, repo).dict == {
            "cran": {"package": package, "repo": repo}
        }

    def test_invalid_storage_library_path(self):
        with pytest.raises(InvalidTaskLibraryError):
            JarTaskLibrary("somebadpath")

    def test_task_library_unique_list(self):
        s3_path = "s3://somepath-in-s3"
        assert TaskLibrary.unique_libraries(
            [
                JarTaskLibrary(s3_path),
                JarTaskLibrary(s3_path),
            ]
        ) == [JarTaskLibrary(s3_path)]

        assert not TaskLibrary.unique_libraries(None)
