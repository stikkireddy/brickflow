import builtins
from enum import Enum
from typing import Optional, Dict

import constructs
from cdktf import TerraformStack, App
from decouple import config

from sdk.engine.utils import wraps_keyerror
from sdk.engine.workflow import Workflow
from sdk.tf.databricks import DatabricksProvider, Job, JobGitSource, JobTask


class WorkflowAlreadyExistsError(Exception):
    pass


class WorkflowNotFoundError(Exception):
    pass


class _Project(TerraformStack):
    # The goal of a project is to contain a bunch of workflows and convert this to a stack.
    def __init__(self, scope: constructs.Construct, id: builtins.str,
                 git_repo: str = None,
                 provider: str = None,
                 git_reference: str = None,
                 s3_backend: str = None,
                 entry_point_path: str = None
                 ):
        super().__init__(scope, id)
        self._entry_point_path = entry_point_path
        self._s3_backend = s3_backend
        self._git_reference = git_reference
        self._provider = provider
        self._git_repo = git_repo
        DatabricksProvider(
            self, "Databricks",
        )
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

    def generate_tf(self):
        for workflow_name, workflow in self._workflows.items():
            ref_type = self._git_reference.split("/")[0]
            ref_value = "/".join(self._git_reference.split("/")[1:])
            git_conf = JobGitSource(url=self._git_repo, provider=self._provider, **{ref_type: ref_value})
            tasks = []
            for task_name, task in workflow.tasks.items():
                tasks.append(JobTask(**{
                    task.task_type: task.get_tf_obj(self._entry_point_path),
                }, existing_cluster_id=workflow.existing_cluster_id))
            Job(self, id_=workflow_name, name=workflow_name, task=tasks, git_source=git_conf)



class Stage(Enum):
    deploy = "deploy"
    execute = "execute"


class Project:
    def __init__(self, name,
                 mode: Stage = Stage[config("BRICKFLOW_MODE", "execute")],
                 execute_workflow: str = None,
                 execute_task: str = None,
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
        self._execute_task = execute_task
        self._execute_workflow = execute_workflow
        self._mode = mode
        self._name = name
        self._app: Optional[App] = None
        self._project = None

    def __enter__(self):
        self._app = App()
        self._project = _Project(self._app,
                                 self._name,
                                 self._git_repo,
                                 self._provider,
                                 self._git_reference,
                                 self._s3_backend,
                                 self._entry_point_path)
        return self._project
        # return _Project()

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(self._mode)
        if self._mode == Stage.deploy:
            self._project.generate_tf()
            self._app.synth()
        if self._mode == Stage.execute:
            workflow = self._project.get_workflow(self._execute_workflow)
            task = workflow.get_task(self._execute_task)
            task.execute()
