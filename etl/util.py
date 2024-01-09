"""Miscellaneous utility functions"""
import base64
import logging
import uuid

from etl.config import settings

# this has to be set once
configured_logging_level = getattr(logging, settings.log_level.upper(), None)
logging.basicConfig(level=configured_logging_level)


def short_uuid() -> str:
    """Creates a short unique ID string"""
    return base64.b64encode(uuid.uuid4().bytes).decode("utf-8").rstrip("=")


def process_file_stub(data: str, **kwargs):
    """A do-nothing-method to use as an example for the Python process config"""
    print(
        f"Received the data file {data} and the named arguments {kwargs}. Doing nothing."
    )


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(configured_logging_level)
    return logger
