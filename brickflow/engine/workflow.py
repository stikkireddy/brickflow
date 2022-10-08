import abc
import functools
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Union, Iterator

import attr
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


@dataclass(frozen=True)
class ScimEntity(abc.ABC):
    name: str

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


@dataclass(frozen=True)
class WorkflowPermissions:
    owner: Optional[User] = (None,)
    can_manage_run: Optional[List[ScimEntity]] = (None,)
    can_view: Optional[List[ScimEntity]] = (None,)
    can_manage: Optional[List[ScimEntity]] = (None,)

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


@dataclass
class Workflow:
    name: str = attr.field(on_setattr=attr.setters.frozen)
    default_compute: Compute = Compute(compute_id="default")
    compute: Dict[str, Compute] = field(default_factory=lambda: {})
    existing_cluster_id: Optional[str] = None
    default_task_settings: Optional[TaskSettings] = TaskSettings()
    tags: Optional[Dict[str, str]] = None
    max_concurrent_runs: int = 1
    permissions: WorkflowPermissions = WorkflowPermissions()
    active_task: Optional[str] = None
    graph: nx.DiGraph = nx.DiGraph()
    tasks: Dict[str, Task] = field(default_factory=lambda: {})

    def __post_init__(self):
        self.graph.add_node(ROOT_NODE)
        self.default_task_settings = self.default_task_settings or TaskSettings()

        self.default_compute.set_to_default()
        self.compute = {"default": self.default_compute}
        # self.compute = (compute and {c.compute_id: c for c in compute}) or {"default": self.default_compute}

    @property
    def bfs_layers(self):
        return list(nx.bfs_layers(self.graph, ROOT_NODE))[1:]

    def task_iter(self) -> Iterator[Task]:
        for task in self.bfs_task_iter():
            yield task

    def bfs_task_iter(self) -> Iterator[Task]:
        for layer in self.bfs_layers:
            for task_key in layer:
                yield self.get_task(task_key)

    def parents(self, node):
        return self.graph.predecessors(node)

    def check_no_active_task(self):
        if self.active_task is not None:
            raise AnotherActiveTaskError(
                "You are calling another active task in another task. "
                "Please abstract the code more."
            )

    @wraps_keyerror(TaskNotFoundError, "Unable to find task: ")
    def get_task(self, task_id) -> Task:
        return self.tasks[task_id]

    @wraps_keyerror(TaskNotFoundError, "Unable to find task: ")
    def pop_task(self, task_id):
        # Pop from dict and graph
        self.tasks.pop(task_id)
        self.graph.remove_node(task_id)

    def task_exists(self, task_id):
        return task_id in self.tasks

    def _set_active_task(self, task_id):
        self.active_task = task_id

    def _reset_active_task(self):
        self.active_task = None

    # TODO: is this even needed?
    # def get_return_value(self, f: Callable, default=None):
    #     return default

    @functools.singledispatch
    def _add_edge_to_graph(self, depends_on, task_id):
        depends_on_list = depends_on if isinstance(depends_on, list) else [depends_on]
        for t in depends_on_list:
            if isinstance(t, str):
                self.graph.add_edge(t, task_id)
            else:
                self.graph.add_edge(t.__name__, task_id)

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
        self.tasks[task_id] = Task(
            task_id=task_id,
            task_func=f,
            workflow=self,
            description=description,
            compute=compute or self.compute,
            depends_on=_depends_on or [],
            task_type=task_type,
            trigger_rule=trigger_rule,
            custom_execute_callback=custom_execute_callback,
        )

        # attempt to create task object before adding to graph
        if _depends_on is None:
            self.graph.add_edge(ROOT_NODE, task_id)
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
