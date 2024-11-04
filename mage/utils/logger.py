import logging


def setup_logger(logger_name, log_level=logging.DEBUG):
    logger = logging.getLogger(logger_name)

    # Check if any handlers are already attached to the logger and remove them
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(log_level)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger
