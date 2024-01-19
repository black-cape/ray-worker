"""Miscellaneous utility functions"""
import base64
import logging
import uuid

from lib_ray_worker.config import adapters

# this has to be set once
configured_logging_level = getattr(logging, adapters.LOG_LEVEL.upper(), None)
logging.basicConfig(level=configured_logging_level)


def short_uuid() -> str:
    """Creates a short unique ID string"""
    return base64.b64encode(uuid.uuid4().bytes).decode("utf-8").rstrip("=")


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(configured_logging_level)
    return logger
