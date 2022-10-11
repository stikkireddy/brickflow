import os
from unittest import mock
from unittest.mock import Mock, call, patch

import pytest

from brickflow.context import ctx, BrickflowInternalVariables
from brickflow.engine.compute import Cluster
from brickflow.engine.project import (
    Project,
    Stage,
    GitRepoIsDirtyError,
    WorkflowAlreadyExistsError,
    get_caller_info,
)
from brickflow.engine import BrickflowEnvVars
from brickflow.engine.workflow import Workflow
from tests.engine.sample_workflow import wf, task_function


def side_effect(a, b):  # noqa
    if a == BrickflowInternalVariables.workflow_id.value:
        return wf.name
    if a == BrickflowInternalVariables.task_id.value:
        return task_function.__name__


class TestProject:
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_execute(self, dbutils):
        dbutils.side_effect = side_effect
        with Project("test-project") as f:
            f.add_workflow(wf)
        assert ctx.get_return_value(task_key=task_function) == task_function()

    @mock.patch.dict(
        os.environ, {BrickflowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    @patch("subprocess.check_output")
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_deploy(self, dbutils: Mock, subproc: Mock):
        dbutils.side_effect = side_effect
        git_ref_b = b"a"
        git_repo = "https://github.com/"
        git_provider = "github"
        subproc.return_value = git_ref_b

        with Project("test-project1", git_repo=git_repo, provider=git_provider) as f:
            f.add_workflow(wf)

        # default path uses commit
        assert f.git_reference == "commit/" + git_ref_b.decode("utf-8")
        assert f.git_repo == git_repo
        assert f.provider == git_provider

        subproc.assert_has_calls(
            [  # noqa
                call(['git log -n 1 --pretty=format:"%H"'], shell=True),
                call(["git diff --stat"], shell=True),
            ]
        )
        assert ctx.get_return_value(task_key=task_function) == task_function()

    @mock.patch.dict(
        os.environ, {BrickflowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    @patch("subprocess.check_output")
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_deploy_is_git_dirty_error(self, dbutils: Mock, subproc: Mock):
        dbutils.side_effect = side_effect
        resp = b"some really long path must return git dirty error"
        git_repo = "https://github.com/"
        git_provider = "github"
        subproc.return_value = resp

        with pytest.raises(GitRepoIsDirtyError):
            with Project(
                "test-project1", git_repo=git_repo, provider=git_provider
            ) as f:
                f.add_workflow(wf)

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_workflow_already_exists_error(self, dbutils):
        dbutils.side_effect = side_effect
        with pytest.raises(WorkflowAlreadyExistsError):
            with Project("test-project") as f:
                f.add_workflow(wf)
                f.add_workflow(wf)

    def test_project_workflow_no_workflows_skip(self):
        with Project("test-project"):
            pass

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_workflow_no_workflow_task_id_skip(self, dbutils):
        dbutils.return_value = None

        with Project(
            "test-project",
        ) as f:
            f.add_workflow(wf)

    @mock.patch.dict(
        os.environ, {BrickflowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    @patch("subprocess.check_output")
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_deploy_workflow_no_schedule(self, dbutils: Mock, subproc: Mock):
        dbutils.return_value = None

        with Project(
            "test-project",
        ) as f:
            f.add_workflow(
                Workflow(
                    "my-workflow",
                    default_cluster=Cluster.from_existing_cluster("someid"),
                )
            )
        subproc.assert_called()

    @mock.patch.dict(
        os.environ,
        {
            BrickflowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value,
            BrickflowEnvVars.BRICKFLOW_LOCAL_MODE.value: "true",
        },
    )
    @patch("subprocess.check_output")
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_deploy_local_mode(self, dbutils: Mock, subproc: Mock):
        dbutils.return_value = None

        with Project(
            "test-project",
        ) as f:
            f.add_workflow(
                Workflow(
                    "my-workflow",
                    default_cluster=Cluster.from_existing_cluster("someid"),
                )
            )
        subproc.assert_called()

    @mock.patch.dict(
        os.environ, {BrickflowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    @patch("subprocess.check_output")
    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_project_workflow_deploy_batch_false(
        self, dbutils: Mock, sub_proc_mock: Mock
    ):
        dbutils.return_value = None
        sub_proc_mock.return_value = b""
        with Project("test-project", batch=False) as f:
            f.add_workflow(wf)

    @patch("brickflow.context.ctx.dbutils_widget_get_or_else")
    def test_adding_pkg(self, dbutils):
        import sample_workflows

        dbutils.side_effect = side_effect
        with Project("test-project") as f:
            f.add_pkg(sample_workflows)

    def test_adding_pkg_err(self):
        fake_pkg = Mock()
        setattr(fake_pkg, "__file__", None)
        with pytest.raises(ImportError):
            with Project("test-project") as f:
                f.add_pkg(fake_pkg)

    @patch("inspect.stack")
    def test_get_caller_info(self, inspect_mock: Mock):
        inspect_mock.return_value = []
        assert get_caller_info() is None
        inspect_mock.assert_called_once()
