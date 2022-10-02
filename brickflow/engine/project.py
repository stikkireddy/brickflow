from enum import Enum
from typing import Dict, Callable

from decouple import config

from brickflow.engine import is_git_dirty, get_current_commit
from brickflow.engine.context import ctx, BrickflowInternalVariables
from brickflow.engine.utils import wraps_keyerror
from brickflow.engine.workflow import Workflow


class WorkflowAlreadyExistsError(Exception):
    pass


class WorkflowNotFoundError(Exception):
    pass


class GitRepoIsDirtyError(Exception):
    pass


class BrickFlowEnvVars(Enum):
    BRICKFLOW_FORCE_DEPLOY = "BRICKFLOW_FORCE_DEPLOY"
    BRICKFLOW_MODE = "BRICKFLOW_MODE"
    BRICKFLOW_GIT_REPO = "BRICKFLOW_GIT_REPO"
    BRICKFLOW_GIT_REF = "BRICKFLOW_GIT_REF"
    BRICKFLOW_GIT_PROVIDER = "BRICKFLOW_GIT_PROVIDER"

# TODO: Logging

class _Project:
    # The goal of a project is to contain a bunch of workflows and convert this to a stack.
    def __init__(self,
                 git_repo: str = None,
                 provider: str = None,
                 git_reference: str = None,
                 s3_backend: str = None,
                 entry_point_path: str = None
                 ):
        self._entry_point_path = entry_point_path
        self._s3_backend = s3_backend
        self._git_reference = git_reference
        self._provider = provider
        self._git_repo = git_repo
        self._workflows: Dict[str, Workflow] = {}

    def add_workflow(self, workflow: Workflow):
        if self.workflow_exists(workflow) is True:
            raise WorkflowAlreadyExistsError(f"Workflow with name: {workflow.name} already exists!")
        self._workflows[workflow.name] = workflow

    def workflow_exists(self, workflow: Workflow):
        return workflow.name in self._workflows

    @wraps_keyerror(WorkflowNotFoundError, "Unable to find workflow: ")
    def get_workflow(self, workflow_id):
        return self._workflows[workflow_id]

    def generate_tf(self, app, id_):
        if is_git_dirty() and config(BrickFlowEnvVars.BRICKFLOW_FORCE_DEPLOY.value, default="false") == "false":
            raise GitRepoIsDirtyError("Please commit all your changes before attempting to deploy.")

        # Avoid node reqs
        from cdktf import TerraformStack
        from brickflow.tf.databricks import DatabricksProvider, Job, JobGitSource, JobTask, JobTaskDependsOn

        stack = TerraformStack(app, id_)
        DatabricksProvider(
            stack, "Databricks",
            host=config("DATABRICKS_HOST", default=None),
            token=config("DATABRICKS_TOKEN", default=None)
        )
        for workflow_name, workflow in self._workflows.items():
            ref_type = self._git_reference.split("/")[0]
            ref_value = "/".join(self._git_reference.split("/")[1:])
            git_conf = JobGitSource(url=self._git_repo, provider=self._provider, **{ref_type: ref_value})
            tasks = []
            for task_name, task in workflow.tasks.items():
                depends_on = [JobTaskDependsOn(task_key=f.__name__ if isinstance(f, Callable) else f)
                              for f in task.depends_on]
                tasks.append(JobTask(
                    **{
                        task.task_type: task.get_tf_obj(self._entry_point_path),
                    },
                    depends_on=depends_on,
                    task_key=task_name,
                    existing_cluster_id=workflow.existing_cluster_id))
            tasks.sort(key=lambda t: t.task_key)
            Job(stack, id_=workflow_name, name=workflow_name, task=tasks, git_source=git_conf)


class Stage(Enum):
    deploy = "deploy"
    execute = "execute"


class Project:
    def __init__(self, name,
                 debug_execute_workflow: str = None,
                 debug_execute_task: str = None,
                 git_repo: str = None,
                 provider: str = None,
                 git_reference: str = None,
                 s3_backend: str = None,
                 entry_point_path: str = None
                 ):
        self._mode = Stage[config(BrickFlowEnvVars.BRICKFLOW_MODE.value, default=Stage.execute.value)]

        self._entry_point_path = entry_point_path
        self._s3_backend = s3_backend
        if self._mode == Stage.deploy:
            git_ref_default = git_reference if git_reference is not None else f"commit/{get_current_commit()}"
        else:
            git_ref_default = git_reference
        self._git_reference = config(BrickFlowEnvVars.BRICKFLOW_GIT_REF.value, default=git_ref_default)
        self._provider = config(BrickFlowEnvVars.BRICKFLOW_GIT_PROVIDER.value, default=provider)
        self._git_repo = config(BrickFlowEnvVars.BRICKFLOW_GIT_REPO.value, default=git_repo)
        self._debug_execute_task = debug_execute_task
        self._debug_execute_workflow = debug_execute_workflow
        self._name = name
        # self._app: Optional['App'] = None
        self._project = None

    def __enter__(self):
        self._project = _Project(self._git_repo,
                                 self._provider,
                                 self._git_reference,
                                 self._s3_backend,
                                 self._entry_point_path)
        return self._project
        # return _Project()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._mode.value == Stage.deploy.value:
            # local import to avoid node req
            from cdktf import App
            app = App()
            self._project.generate_tf(app,
                                      self._name, )
            app.synth()
        if self._mode.value == Stage.execute.value:
            wf_id = ctx.dbutils_widget_get_or_else(BrickflowInternalVariables.workflow_id, self._debug_execute_workflow)
            t_id = ctx.dbutils_widget_get_or_else(BrickflowInternalVariables.task_id, self._debug_execute_task)
            workflow = self._project.get_workflow(wf_id)
            task = workflow.get_task(t_id)
            task.execute()