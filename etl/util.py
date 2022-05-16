"""Miscellaneous utility functions"""
import base64
import logging
import uuid
from typing import Optional, Tuple

from requests import Response
from requests.adapters import HTTPAdapter
from requests.sessions import Session

from etl.config import settings


def short_uuid() -> str:
    """Creates a short unique ID string"""
    return base64.b64encode(uuid.uuid4().bytes).decode('utf-8').rstrip('=')


def process_file_stub(data: str, **kwargs):
    """A do-nothing-method to use as an example for the Python process config"""
    print(f'Received the data file {data} and the named arguments {kwargs}. Doing nothing.')


def get_logger(name: str) -> logging.Logger:
    log_level = settings.log_level
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    return logger


class IgnoreHostnameVerification(HTTPAdapter):
    """HTTP adapter to skip the check for hostname verification with the CA cert."""

    def init_poolmanager(self, *args, **kwargs) -> None:  # pylint: disable=signature-differs
        kwargs['assert_hostname'] = False
        super().init_poolmanager(*args, **kwargs)


class RestClient:
    """REST Wrapper for hitting other services.

    Will handle authentication and setting up common request resources.
    """

    def __init__(
        self,
        client_cert: Tuple[str, str],
        connection_url: Optional[str] = None,
        ca_cert: Optional[str] = None,
        verify_ca: bool = True
    ):
        self.session = Session()
        if connection_url and not verify_ca:
            self.session.mount(connection_url, IgnoreHostnameVerification())
        self.ca_cert = ca_cert
        self.client_cert = client_cert

    def make_request(self, url: str, method: str, **kwargs) -> Response:
        session_method = getattr(self.session, method)
        if not session_method:
            raise ValueError(f'Invalid session method {method}')
        if self.client_cert:
            kwargs['cert'] = self.client_cert
        if self.ca_cert:
            kwargs['verify'] = self.ca_cert
        else:
            kwargs['verify'] = False
        return session_method(url, **kwargs)


def create_rest_client() -> RestClient:
    connection_params = settings.connection_params
    client_cert = settings.client_cert
    client_key = settings.client_key

    rest_client = RestClient(
        client_cert=(client_cert, client_key) if client_cert and client_key else None,
        **(connection_params if connection_params else {})
    )

    return rest_client
