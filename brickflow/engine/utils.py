import functools


def wraps_keyerror(error_class, msg):
    def wrapper(f):
        @functools.wraps(f)
        def func(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except KeyError as e:
                raise error_class(msg, e, *args, **kwargs)
        return func
    return wrapper