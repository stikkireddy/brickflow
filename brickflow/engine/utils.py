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


def resolve_py4j_logging():
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        import logging
        logger = spark._jvm.org.apache.log4j
        logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
        logging.getLogger("py4j").setLevel(logging.ERROR)
    except Exception as e:
        # Ignore when running locally
        pass