import functools
from typing import Callable, List, Optional, Dict, Union

import networkx as nx

from brickflow.engine import ROOT_NODE
from brickflow.engine.compute import Compute
from brickflow.engine.context import ctx
from brickflow.engine.task import TaskNotFoundError, AnotherActiveTaskError, Task, TaskType, TaskAlreadyExistsError, \
    BrickflowTriggerRule, UnsupportedBrickflowTriggerRuleError, TaskSettings
from brickflow.engine.utils import wraps_keyerror


class ScimEntity:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return self.name != other

    def __hash__(self):
        return hash(self.__class__.__name__ + self.name)

    def to_access_control(self):
        pass


class User(ScimEntity):
    def to_access_control(self):
        return {"user_name": self.name}


class Group(ScimEntity):
    def to_access_control(self):
        return {"group_name": self.name}


class ServicePrincipal(ScimEntity):
    def to_access_control(self):
        return {"service_principal_name": self.name}


class WorkflowPermissions:
    def __init__(self,
                 owner: User = None,
                 can_manage_run: List[ScimEntity] = None,
                 can_view: List[ScimEntity] = None,
                 can_manage: List[ScimEntity] = None):
        self.owner = owner
        self.can_manage = can_manage or []
        self.can_view = can_view or []
        self.can_manage_run = can_manage_run or []

    def to_access_controls(self):
        access_controls = []
        if self.owner is not None:
            access_controls.append({"permission_level": "IS_OWNER", **self.owner.to_access_control()})
        for principal in list(set(self.can_manage)):
            access_controls.append({"permission_level": "CAN_MANAGE", **principal.to_access_control()})
        for principal in list(set(self.can_manage_run)):
            access_controls.append({"permission_level": "CAN_MANAGE", **principal.to_access_control()})
        for principal in list(set(self.can_view)):
            access_controls.append({"permission_level": "CAN_MANAGE", **principal.to_access_control()})
        return access_controls


class Workflow:

    def __init__(self, name,
                 default_compute: Compute = Compute(compute_id="default"),
                 compute: List[Compute] = None,
                 existing_cluster=None,
                 airflow_110_dag: 'DAG' = None,
                 default_task_settings: Optional[TaskSettings] = None,
                 tags: Dict[str, str] = None,
                 max_concurrent_runs: int = 1,
                 permissions: WorkflowPermissions = None,
                 ):
        # todo: add defaults
        self._airflow_110_dag = self._get_airflow_dag(airflow_110_dag)

        self._existing_cluster = existing_cluster
        self._name = name
        default_compute.set_to_default()
        self._compute = {"default": default_compute}
        self._compute.update((compute and {c.compute_id: c for c in compute}) or {})
        self._default_task_settings = default_task_settings
        # self._compute = (compute and {c.compute_id: c for c in compute}) or {"default": default_compute}
        self._tasks = {}
        self._active_task = None
        self._graph = nx.DiGraph()
        self._graph.add_node(ROOT_NODE)
        self._permissions = permissions or WorkflowPermissions()
        self._max_concurrent_runs = max_concurrent_runs
        self._tags = tags

    @property
    def permissions(self) -> WorkflowPermissions:
        return self._permissions

    @property
    def max_concurrent_runs(self) -> int:
        return self._max_concurrent_runs

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @property
    def default_task_settings(self):
        return self._default_task_settings

    @property
    def bfs_layers(self):
        return list(nx.bfs_layers(self._graph, ROOT_NODE))[1:]

    def bfs_task_iter(self):
        for layer in self.bfs_layers:
            for task_key in layer:
                yield self.get_task(task_key)

    def parents(self, node):
        return self._graph.predecessors(node)

    @staticmethod
    def _get_airflow_dag(dag110):
        if dag110 is None:
            return None
        try:
            from airflow import DAG
            from brickflow.adapters.airflow_1_10 import Airflow110DagAdapter
            return Airflow110DagAdapter(dag110, ctx.dbutils)
        except ImportError as e:
            return None

    @property
    def airflow_dag(self):
        return self._airflow_110_dag

    @property
    def tasks(self) -> Dict[str, Task]:
        return self._tasks

    @property
    def existing_cluster_id(self):
        return self._existing_cluster

    @property
    def name(self):
        return self._name

    def get_default_compute(self):
        return self._compute["default"]

    def check_no_active_task(self):
        if self._active_task is not None:
            raise AnotherActiveTaskError("You are calling another active task in another task. "
                                         "Please abstract the code more.")

    @wraps_keyerror(TaskNotFoundError, "Unable to find task: ")
    def get_task(self, task_id) -> Task:
        return self._tasks[task_id]

    def task_exists(self, task_id):
        return task_id in self._tasks

    def _set_active_task(self, task_id):
        self._active_task = task_id

    def _reset_active_task(self):
        self._active_task = None

    def get_return_value(self, f: Callable, default=None):
        return default

    def bind_airflow_task(self, name: str, compute: Optional[Compute] = None,
                          depends_on: Optional[List[Union[Callable, str]]] = None,
                          ):
        self._airflow_110_dag.exists(name)
        trigger_rule = self._airflow_110_dag.get_task(name).trigger_rule
        if BrickflowTriggerRule.is_valid(trigger_rule) is False:
            raise UnsupportedBrickflowTriggerRuleError(f"Unsupported trigger rule: {trigger_rule} for task: {name}")
        self._airflow_110_dag.validate_task(name)
        return self.task(name=name, compute=compute, task_type=TaskType.AIRFLOW_TASK,
                         depends_on=depends_on, trigger_rule=BrickflowTriggerRule[trigger_rule.upper()])

    def task(self, name: str = None, compute: Optional[Compute] = None,
             task_type: Optional[TaskType] = TaskType.NOTEBOOK,
             depends_on: Optional[List[Union[Callable, str]]] = None,
             trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS,
             ):

        def task_wrapper(f: Callable):
            task_id = name or f.__name__
            if self.task_exists(task_id):
                raise TaskAlreadyExistsError(f"Task: {task_id} already exists, please rename your function.")

            self._tasks[task_id] = Task(task_id,
                                        f,
                                        self,
                                        compute or self._compute,
                                        depends_on,
                                        task_type,
                                        trigger_rule)
            if depends_on is None:
                self._graph.add_edge(ROOT_NODE, task_id)
            elif type(depends_on) == list:
                for t in depends_on:
                    self._graph.add_edge(t.__name__, task_id)
            elif type(depends_on) == str:
                self._graph.add_edge(depends_on, task_id)

            @functools.wraps(f)
            def func(*args, **kwargs):
                try:
                    self.check_no_active_task()
                    self._set_active_task(task_id)
                    resp = f(*args, **kwargs)
                    return resp
                except Exception as e:
                    self._reset_active_task()
                    raise e
                finally:
                    self._reset_active_task()

            return func

        return task_wrapper
