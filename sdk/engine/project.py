from enum import Enum
from typing import Optional, Dict

from decouple import config

from sdk.engine.utils import wraps_keyerror
from sdk.engine.workflow import Workflow


class WorkflowAlreadyExistsError(Exception):
    pass


class WorkflowNotFoundError(Exception):
    pass


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
        # Avoid node reqs
        from cdktf import TerraformStack
        from sdk.tf.databricks import DatabricksProvider, Job, JobGitSource, JobTask

        stack = TerraformStack(app, id_)
        DatabricksProvider(
            stack, "Databricks",
        )
        for workflow_name, workflow in self._workflows.items():
            ref_type = self._git_reference.split("/")[0]
            ref_value = "/".join(self._git_reference.split("/")[1:])
            git_conf = JobGitSource(url=self._git_repo, provider=self._provider, **{ref_type: ref_value})
            tasks = []
            for task_name, task in workflow.tasks.items():
                tasks.append(JobTask(**{

                    task.task_type: task.get_tf_obj(self._entry_point_path),
                }, task_key=task_name, existing_cluster_id=workflow.existing_cluster_id))
            Job(stack, id_=workflow_name, name=workflow_name, task=tasks, git_source=git_conf)


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
        self._app: Optional['App'] = None
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

        if self._mode == Stage.deploy:
            # local import to avoid node req
            from cdktf import App
            app = App()
            self._project.generate_tf(app,
                                      self._name, )
            app.synth()
        if self._mode == Stage.execute:
            workflow = self._project.get_workflow(self._execute_workflow)
            task = workflow.get_task(self._execute_task)
            task.execute()
