import pytest

from brickflow.engine.compute import Cluster, DuplicateClustersDefinitionError
from brickflow.engine.task import (
    Task,
    TaskType,
    BrickflowTriggerRule,
    TaskAlreadyExistsError,
    AnotherActiveTaskError,
    NoCallableTaskError,
    TaskNotFoundError,
)
from brickflow.engine.workflow import (
    User,
    Group,
    ServicePrincipal,
    Workflow,
    NoWorkflowComputeError,
)
from tests.engine.sample_workflow import wf, task_function


class TestWorkflow:
    def test_add_task(self):
        t = wf.get_task(task_function.__name__)
        assert t.name == task_function.__name__
        assert t.task_func is not None
        assert t.workflow == wf
        # task compute is workflow default compute
        assert t.cluster == wf.default_cluster
        assert t.depends_on == []
        assert t.task_type == TaskType.NOTEBOOK
        assert t.trigger_rule == BrickflowTriggerRule.ALL_SUCCESS
        assert t.custom_execute_callback is None

    def test_create_workflow_no_compute(self):
        with pytest.raises(NoWorkflowComputeError):
            Workflow("test")

    def test_create_workflow_with_duplicate_compute(self):
        with pytest.raises(DuplicateClustersDefinitionError):
            compute = [
                Cluster("name", "spark"),
                Cluster("name2", "spark1"),
                Cluster("name", "spark"),
                Cluster("name2", "spark"),
                Cluster("name3", "spark"),
            ]
            this_wf = Workflow("test", clusters=compute)
            this_wf.validate_new_clusters_with_unique_names()

    def test_default_cluster_isnt_empty(self):
        with pytest.raises(RuntimeError):
            compute = [
                Cluster("name", "spark"),
            ]
            this_wf = Workflow("test", clusters=compute)
            this_wf.default_cluster = None
            this_wf._add_task(f=lambda: 123, task_id="taskid")

    def test_create_workflow_set_default_cluster(self):
        this_wf = Workflow("test", clusters=[Cluster("name", "spark")])
        assert this_wf.default_cluster == this_wf.clusters[0]

    def test_add_existing_task_name(self):
        with pytest.raises(TaskAlreadyExistsError):

            @wf.task(name=task_function.__name__)
            def _(abc):
                return abc

            wf.pop_task(task_function.__name__)

    def test_another_active_task_error(self):
        task_name = "_some_task"
        with pytest.raises(AnotherActiveTaskError):

            @wf.task(name=task_name)
            def error(*, abc="def"):
                task_function()
                return abc

            error()
        wf.pop_task(task_name)

    def test_deco_no_args(self):
        with pytest.raises(NoCallableTaskError):
            wf.task("hello world")

    def test_get_tasks(self):
        assert len(wf.tasks) == 7

    def test_task_iter(self):
        arr = []
        for t in wf.task_iter():
            assert isinstance(t, Task)
            assert callable(t.task_func)
            arr.append(t)
        assert len(arr) == 7, print([t.name for t in arr])

    def test_permissions(self):
        assert wf.permissions.to_access_controls() == [
            {"permission_level": "IS_OWNER", "user_name": "abc@abc.com"},
            {"permission_level": "CAN_MANAGE", "user_name": "abc@abc.com"},
            {"permission_level": "CAN_MANAGE_RUN", "user_name": "abc@abc.com"},
            {"permission_level": "CAN_VIEW", "user_name": "abc@abc.com"},
        ]

    def test_max_concurrent_runs(self):
        assert wf.max_concurrent_runs == 1

    def test_tags(self):
        assert wf.tags == {"test": "test2"}

    def test_default_task_settings(self):
        assert wf.default_task_settings is not None

    def test_user(self):
        principal = "abc@abc.com"
        u = User(principal)
        assert u.to_access_control() == {"user_name": principal}

    def test_group(self):
        principal = "abc"
        g = Group(principal)
        assert g.to_access_control() == {"group_name": principal}

    def test_service_principal(self):
        principal = "abc-123-456-678"
        sp = ServicePrincipal(principal)
        assert sp.to_access_control() == {"service_principal_name": principal}

    def test_scim_entity(self):
        principal = "abc"
        principal2 = "def"
        user1 = User(principal)
        user2 = User(principal)
        user3 = User(principal2)
        assert user2 == user1
        assert len({user1, user2}) == 1
        assert user1 != user3

    def test_key_error(self):
        with pytest.raises(TaskNotFoundError):
            wf.get_task("some_task_that_doesnt_exist")
