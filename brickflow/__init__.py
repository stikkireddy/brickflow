import logging


def setup_logger():
    _log = logging.getLogger(__name__)  # Logger
    _log.setLevel(logging.INFO)
    logger_handler = logging.StreamHandler()
    logger_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [brickflow-framework] {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"
        )
    )
    _log.addHandler(logger_handler)
    return _log


log = setup_logger()
