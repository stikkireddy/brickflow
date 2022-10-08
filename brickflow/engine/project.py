import importlib
import inspect
import os
from dataclasses import field, dataclass
from enum import Enum
from types import ModuleType
from typing import Dict, Callable, Optional

import attr
from decouple import config

from brickflow.context import ctx, BrickflowInternalVariables
from brickflow.engine import is_git_dirty, get_current_commit
from brickflow.engine.task import TaskType
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
@dataclass(frozen=True)
class _Project:
    git_repo: Optional[str] = None
    provider: Optional[str] = None
    git_reference: Optional[str] = None
    s3_backend: Optional[str] = None
    entry_point_path: Optional[str] = None
    workflows: Dict[str, Workflow] = field(default_factory=lambda: {})

    def add_pkg(self, pkg: ModuleType):
        for module in os.listdir(os.path.dirname(pkg.__file__)):
            # only find python files and ignore __init__.py
            if module == "__init__.py" or module[-3:] != ".py":
                continue
            module_name = module.replace(".py", "")
            # import all the modules into the mod object and not actually import them using __import__
            mod = importlib.import_module(f"{pkg.__name__}.{module_name}")
            for obj in dir(mod):
                module_item = getattr(mod, obj)
                if isinstance(module_item, Workflow):
                    # checked to see if this is a workflow object
                    self.add_workflow(module_item)

    def add_workflow(self, workflow: Workflow):
        if self.workflow_exists(workflow) is True:
            raise WorkflowAlreadyExistsError(
                f"Workflow with name: {workflow.name} already exists!"
            )
        self.workflows[workflow.name] = workflow

    def workflow_exists(self, workflow: Workflow):
        return workflow.name in self.workflows

    @wraps_keyerror(WorkflowNotFoundError, "Unable to find workflow: ")
    def get_workflow(self, workflow_id):
        return self.workflows[workflow_id]

    def _create_workflow_tasks(self, workflow: Workflow):
        # Avoid node reqs
        from brickflow.tf.databricks import (
            JobTask,
            JobTaskDependsOn,
        )

        tasks = []
        for task_name, task in workflow.tasks.items():
            depends_on = [
                JobTaskDependsOn(task_key=f.__name__ if isinstance(f, Callable) else f)
                for f in task.depends_on
            ]
            tf_task_type = (
                task.task_type_str
                if task.task_type_str != TaskType.CUSTOM_PYTHON_TASK.value
                else TaskType.NOTEBOOK.value
            )

            task_settings = workflow.default_task_settings.merge(task.task_settings)
            tasks.append(
                JobTask(
                    **{
                        tf_task_type: task.get_tf_obj(self.entry_point_path),
                        **task_settings.to_tf_dict(),
                    },
                    depends_on=depends_on,
                    task_key=task_name,
                    existing_cluster_id=workflow.existing_cluster_id,
                )
            )
        tasks.sort(key=lambda t: t.task_key)
        return tasks

    def _create_workflow_permissions(
        self, stack, workflow: Workflow, job_obj: "Job"  # noqa
    ):
        # Avoid node reqs
        from brickflow.tf.databricks import (
            Permissions,
            PermissionsAccessControl,
        )

        return Permissions(
            stack,
            id_=f"{workflow.name}_permissions",
            job_id=job_obj.id,
            access_control=[
                PermissionsAccessControl(**i)
                for i in workflow.permissions.to_access_controls()
            ],
        )

    def generate_tf(self, app, id_):
        if (
            is_git_dirty()
            and config(BrickFlowEnvVars.BRICKFLOW_FORCE_DEPLOY.value, default="false")
            == "false"
        ):
            raise GitRepoIsDirtyError(
                "Please commit all your changes before attempting to deploy."
            )

        # Avoid node reqs
        from cdktf import TerraformStack
        from brickflow.tf.databricks import (
            DatabricksProvider,
            Job,
            JobGitSource,
        )

        stack = TerraformStack(app, id_)
        DatabricksProvider(
            stack,
            "Databricks",
            profile=config("DATABRICKS_PROFILE", default=None),
            host=config("DATABRICKS_HOST", default=None),
            token=config("DATABRICKS_TOKEN", default=None),
        )
        for workflow_name, workflow in self.workflows.items():
            ref_type = self.git_reference.split("/", maxsplit=1)[0]
            ref_value = "/".join(self.git_reference.split("/")[1:])
            git_conf = JobGitSource(
                url=self.git_repo, provider=self.provider, **{ref_type: ref_value}
            )
            # tasks = []
            tasks = self._create_workflow_tasks(workflow)
            job = Job(
                stack,
                id_=workflow_name,
                name=workflow_name,
                task=tasks,
                git_source=git_conf,
                tags=workflow.tags,
                max_concurrent_runs=workflow.max_concurrent_runs,
            )
            if workflow.permissions.to_access_controls():
                self._create_workflow_permissions(stack, workflow, job)


class Stage(Enum):
    deploy = "deploy"
    execute = "execute"


def get_caller_info():
    # First get the full filename which isnt project.py (the first area where this caller info is called.
    # This should work most of the time.
    _cwd = str(os.getcwd())
    for i in inspect.stack():
        if i.filename not in [__file__, ""]:
            return os.path.splitext(os.path.relpath(i.filename, _cwd))[0]


# TODO: See if project can just be a directory path and scan for all "Workflow" instances
@dataclass
class Project:
    name: str = attr.field(on_setattr=attr.setters.frozen)
    debug_execute_workflow: Optional[str] = None
    debug_execute_task: Optional[str] = None
    git_repo: Optional[str] = None
    provider: Optional[str] = None
    git_reference: Optional[str] = None
    s3_backend: Optional[str] = None
    entry_point_path: Optional[str] = None
    mode: Optional[str] = None

    _project: Optional[_Project] = field(init=False)

    def __post_init__(self):
        self._mode = Stage[
            config(BrickFlowEnvVars.BRICKFLOW_MODE.value, default=Stage.execute.value)
        ]
        self.entry_point_path = self.entry_point_path or get_caller_info()

        if self._mode == Stage.deploy:
            git_ref_default = (
                self.git_reference
                if self.git_reference is not None
                else f"commit/{get_current_commit()}"
            )
        else:
            git_ref_default = self.git_reference
        self.git_reference = config(
            BrickFlowEnvVars.BRICKFLOW_GIT_REF.value, default=git_ref_default
        )
        self.provider = config(
            BrickFlowEnvVars.BRICKFLOW_GIT_PROVIDER.value, default=self.provider
        )
        self.git_repo = config(
            BrickFlowEnvVars.BRICKFLOW_GIT_REPO.value, default=self.git_repo
        )

    def __enter__(self):
        self._project = _Project(
            self.git_repo,
            self.provider,
            self.git_reference,
            self.s3_backend,
            self.entry_point_path,
        )
        return self._project

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._mode.value == Stage.deploy.value:
            # local import to avoid node req
            from cdktf import App

            app = App()
            self._project.generate_tf(
                app,
                self.name,
            )
            app.synth()
        if self._mode.value == Stage.execute.value:
            wf_id = ctx.dbutils_widget_get_or_else(
                BrickflowInternalVariables.workflow_id.value,
                self.debug_execute_workflow,
            )
            t_id = ctx.dbutils_widget_get_or_else(
                BrickflowInternalVariables.task_id.value, self.debug_execute_task
            )
            workflow = self._project.get_workflow(wf_id)
            task = workflow.get_task(t_id)
            task.execute()
