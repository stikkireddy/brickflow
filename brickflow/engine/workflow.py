import abc
import functools
from typing import Callable, List, Optional, Dict, Union, Iterator

import networkx as nx

from brickflow.engine import ROOT_NODE
from brickflow.engine.compute import Compute
from brickflow.engine.task import (
    TaskNotFoundError,
    AnotherActiveTaskError,
    Task,
    TaskType,
    TaskAlreadyExistsError,
    BrickflowTriggerRule,
    TaskSettings,
    NoCallableTaskError,
)
from brickflow.engine.utils import wraps_keyerror


# TODO: replace with dataclasses
class ScimEntity(abc.ABC):
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

    @abc.abstractmethod
    def to_access_control(self):  # pragma: no cover
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
    def __init__(
        self,
        owner: User = None,
        can_manage_run: List[ScimEntity] = None,
        can_view: List[ScimEntity] = None,
        can_manage: List[ScimEntity] = None,
    ):
        self.owner = owner
        self.can_manage = can_manage or []
        self.can_view = can_view or []
        self.can_manage_run = can_manage_run or []

    def to_access_controls(self):
        access_controls = []
        # TODO: Permissions as ENUM
        if self.owner is not None:
            access_controls.append(
                {"permission_level": "IS_OWNER", **self.owner.to_access_control()}
            )
        for principal in list(set(self.can_manage)):
            access_controls.append(
                {"permission_level": "CAN_MANAGE", **principal.to_access_control()}
            )
        for principal in list(set(self.can_manage_run)):
            access_controls.append(
                {"permission_level": "CAN_MANAGE_RUN", **principal.to_access_control()}
            )
        for principal in list(set(self.can_view)):
            access_controls.append(
                {"permission_level": "CAN_VIEW", **principal.to_access_control()}
            )
        return access_controls


class Workflow:
    def __init__(
        self,
        name,
        default_compute: Compute = Compute(compute_id="default"),
        compute: List[Compute] = None,
        existing_cluster=None,
        default_task_settings: Optional[TaskSettings] = None,
        tags: Dict[str, str] = None,
        max_concurrent_runs: int = 1,
        permissions: WorkflowPermissions = None,
    ):
        self._existing_cluster = existing_cluster
        self._name = name
        default_compute.set_to_default()
        self._compute = {"default": default_compute}
        self._compute.update((compute and {c.compute_id: c for c in compute}) or {})
        self._default_task_settings = default_task_settings or TaskSettings()
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

    def task_iter(self) -> Iterator[Task]:
        for task in self.bfs_task_iter():
            yield task

    def bfs_task_iter(self) -> Iterator[Task]:
        for layer in self.bfs_layers:
            for task_key in layer:
                yield self.get_task(task_key)

    def parents(self, node):
        return self._graph.predecessors(node)

    @property
    def tasks(self) -> Dict[str, Task]:
        return self._tasks

    @property
    def existing_cluster_id(self):
        return self._existing_cluster

    @property
    def name(self):
        return self._name

    @property
    def default_compute(self):
        return self._compute["default"]

    def check_no_active_task(self):
        if self._active_task is not None:
            raise AnotherActiveTaskError(
                "You are calling another active task in another task. "
                "Please abstract the code more."
            )

    @wraps_keyerror(TaskNotFoundError, "Unable to find task: ")
    def get_task(self, task_id) -> Task:
        return self._tasks[task_id]

    @wraps_keyerror(TaskNotFoundError, "Unable to find task: ")
    def pop_task(self, task_id):
        # Pop from dict and graph
        self._tasks.pop(task_id)
        self._graph.remove_node(task_id)

    def task_exists(self, task_id):
        return task_id in self._tasks

    def _set_active_task(self, task_id):
        self._active_task = task_id

    def _reset_active_task(self):
        self._active_task = None

    # TODO: is this even needed?
    # def get_return_value(self, f: Callable, default=None):
    #     return default

    @functools.singledispatch
    def _add_edge_to_graph(self, depends_on, task_id):
        depends_on_list = depends_on if isinstance(depends_on, list) else [depends_on]
        for t in depends_on_list:
            if isinstance(t, str):
                self._graph.add_edge(t, task_id)
            else:
                self._graph.add_edge(t.__name__, task_id)

    def _add_task(
        self,
        f: Callable,
        task_id: str,
        description: Optional[str] = None,
        compute: Optional[Compute] = None,
        task_type: Optional[TaskType] = TaskType.NOTEBOOK,
        depends_on: Optional[Union[Callable, str, List[Union[Callable, str]]]] = None,
        trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS,
        custom_execute_callback: Callable = None,
    ):
        if self.task_exists(task_id):
            raise TaskAlreadyExistsError(
                f"Task: {task_id} already exists, please rename your function."
            )

        _depends_on = (
            [depends_on]
            if isinstance(depends_on, str) or callable(depends_on)
            else depends_on
        )
        self._tasks[task_id] = Task(
            task_id=task_id,
            task_func=f,
            workflow=self,
            description=description,
            compute=compute or self._compute,
            depends_on=_depends_on,
            task_type=task_type,
            trigger_rule=trigger_rule,
            custom_execute_callback=custom_execute_callback,
        )

        # attempt to create task object before adding to graph
        if _depends_on is None:
            self._graph.add_edge(ROOT_NODE, task_id)
        else:
            self._add_edge_to_graph(_depends_on, task_id)

    def task(
        self,
        task_func: Callable = None,
        name: str = None,
        compute: Optional[Compute] = None,
        task_type: Optional[TaskType] = TaskType.NOTEBOOK,
        depends_on: Optional[Union[Callable, str, List[Union[Callable, str]]]] = None,
        trigger_rule: BrickflowTriggerRule = BrickflowTriggerRule.ALL_SUCCESS,
        custom_execute_callback: Callable = None,
    ):
        def task_wrapper(f: Callable):
            task_id = name or f.__name__

            self._add_task(
                f,
                task_id,
                compute=compute,
                task_type=task_type,
                depends_on=depends_on,
                trigger_rule=trigger_rule,
                custom_execute_callback=custom_execute_callback,
            )

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

        if task_func is not None:
            if callable(task_func):
                return task_wrapper(task_func)
            else:
                raise NoCallableTaskError(
                    "Please use task decorator against a callable function."
                )

        return task_wrapper
