import os
import subprocess
from unittest import mock
from unittest.mock import Mock, call

import pytest

from brickflow.context import ctx, BrickflowInternalVariables
from brickflow.engine.project import (
    Project,
    BrickFlowEnvVars,
    Stage,
    GitRepoIsDirtyError,
    WorkflowAlreadyExistsError,
)
from tests.engine.sample_workflow import wf, task_function


def side_effect(a, b):
    print(a, b)
    if a == BrickflowInternalVariables.workflow_id.value:
        return wf.name
    if a == BrickflowInternalVariables.task_id.value:
        return task_function.__name__


class TestProject:
    def test_project_execute(self):
        dbutils = Mock(side_effect=side_effect)
        ctx.dbutils_widget_get_or_else = dbutils
        with Project("test-project") as f:
            f.add_workflow(wf)
        assert ctx.get_return_value(task_key=task_function) == task_function()

    @mock.patch.dict(
        os.environ, {BrickFlowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    def test_project_deploy(self, mocker):
        dbutils = Mock(side_effect=side_effect)
        ctx.dbutils_widget_get_or_else = dbutils
        mocker.patch("subprocess.check_output")
        git_ref_b = b"a"
        git_repo = "https://github.com/"
        git_provider = "github"
        subprocess.check_output.return_value = git_ref_b

        with Project("test-project1", git_repo=git_repo, provider=git_provider) as f:
            f.add_workflow(wf)

        # default path uses commit
        assert f._git_reference == "commit/" + git_ref_b.decode("utf-8")
        assert f._git_repo == git_repo
        assert f._provider == git_provider

        subprocess.check_output.assert_has_calls(
            [  # noqa
                call(['git log -n 1 --pretty=format:"%H"'], shell=True),
                call(["git diff --stat"], shell=True),
            ]
        )
        assert ctx.get_return_value(task_key=task_function) == task_function()

    @mock.patch.dict(
        os.environ, {BrickFlowEnvVars.BRICKFLOW_MODE.value: Stage.deploy.value}
    )
    def test_project_deploy_is_git_dirty_error(self, mocker):
        dbutils = Mock(side_effect=side_effect)
        ctx.dbutils_widget_get_or_else = dbutils
        mocker.patch("subprocess.check_output")
        resp = b"some really long path must return git dirty error"
        git_repo = "https://github.com/"
        git_provider = "github"
        subprocess.check_output.return_value = resp

        with pytest.raises(GitRepoIsDirtyError):
            with Project(
                "test-project1", git_repo=git_repo, provider=git_provider
            ) as f:
                f.add_workflow(wf)

    def test_project_workflow_already_exists_error(self):
        dbutils = Mock(side_effect=side_effect)
        ctx.dbutils_widget_get_or_else = dbutils
        with pytest.raises(WorkflowAlreadyExistsError):
            with Project("test-project") as f:
                f.add_workflow(wf)
                f.add_workflow(wf)
