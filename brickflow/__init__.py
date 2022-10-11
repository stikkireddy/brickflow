import logging
import sys


def get_brickflow_version(package_name: str = "brickflow") -> str:
    try:
        from importlib import metadata  # type: ignore
    except ImportError:
        # Python < 3.8
        import importlib_metadata as metadata  # type: ignore

    try:
        return metadata.version(package_name)  # type: ignore
    except metadata.PackageNotFoundError:  # type: ignore
        return "unknown"


def setup_logger():
    _log = logging.getLogger(__name__)  # Logger
    _log.setLevel(logging.INFO)
    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [brickflow-framework] {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"
        )
    )
    _log.addHandler(logger_handler)
    return _log


log = setup_logger()
