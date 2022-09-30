import functools
from typing import Callable, List, Optional, Dict, Union

from sdk.engine.compute import Compute
from sdk.engine.task import TaskNotFoundError, AnotherActiveTaskError, Task, TaskType, TaskAlreadyExistsError
from sdk.engine.utils import wraps_keyerror


class Workflow:

    def __init__(self, name,
                 default_compute: Compute = Compute(compute_id="default"),
                 compute: List[Compute] = None,
                 existing_cluster=None):
        # todo: add defaults
        self._existing_cluster = existing_cluster
        self._name = name
        default_compute.set_to_default()
        self._compute = {"default": default_compute}
        self._compute.update((compute and {c.compute_id: c for c in compute}) or {})
        # self._compute = (compute and {c.compute_id: c for c in compute}) or {"default": default_compute}
        self._tasks = {}
        self._active_task = None

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

    def task(self, name: str = None, compute: Optional[Compute] = None,
             task_type: Optional[TaskType] = TaskType.NOTEBOOK,
             depends_on: Optional[List[Union[Callable, str]]] = None):

        def task_wrapper(f: Callable):
            task_id = name or f.__name__
            if self.task_exists(task_id):
                raise TaskAlreadyExistsError(f"Task: {task_id} already exists, please rename your function.")

            self._tasks[task_id] = Task(task_id, f, self, compute or self._compute, depends_on, task_type)

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
