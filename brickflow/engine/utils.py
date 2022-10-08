import functools
from typing import Callable, Type

from brickflow.hints import propagate_hint


@propagate_hint
def wraps_keyerror(error_class: Type[Exception], msg: str) -> Callable:
    def wrapper(f: Callable) -> Callable:
        @functools.wraps(f)
        def func(*args, **kwargs):  # type: ignore
            try:
                return f(*args, **kwargs)
            except KeyError as e:
                raise error_class(
                    f"{msg}; err: {str(e)}; args: {args}; kwargs: {kwargs}"
                )

        return func

    return wrapper
