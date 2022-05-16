"""Helper functions for ObjectIds and paths"""
import logging
import re
from http import HTTPStatus
from pathlib import PurePosixPath
from typing import Optional

from etl.file_processor_config import FileProcessorConfig
from etl.object_store.interfaces import ObjectStore
from etl.object_store.object_id import ObjectId
from etl.util import RestClient

LOGGER = logging.getLogger(__name__)


def _compute_config_path(config_object_id: ObjectId, *args) -> ObjectId:
    """Computes a bucket directory relative to a processor config path.

    :param config_file_name: The configuration file name.
    :return: The bucket path.
    """
    path = str(PurePosixPath(config_object_id.path).parent.joinpath(*args))
    return ObjectId(config_object_id.namespace, path)


def get_inbox_path(config_object_id: ObjectId, cfg: FileProcessorConfig, file_object_id: ObjectId = None) -> ObjectId:
    """Gets an ObjectId of the inbox directory for a given config or an object below it
    :param config_object_id: The ObjectId of the processor config file
    :param cfg: The deserialized processor config
    :param file_object_id: An optional object to locate in the directory
    """
    return (_compute_config_path(config_object_id, cfg.inbox_dir, filename(file_object_id))
            if file_object_id is not None
            else _compute_config_path(config_object_id, cfg.inbox_dir))


def get_processing_path(config_object_id: ObjectId, cfg: FileProcessorConfig, file_object_id: ObjectId = None) -> ObjectId:
    """Gets an ObjectId of the processing directory for a given config or an object below it
    :param config_object_id: The ObjectId of the processor config file
    :param cfg: The deserialized processor config
    :param file_object_id: An optional object to locate in the directory
    """
    return (_compute_config_path(config_object_id, cfg.processing_dir, filename(file_object_id))
            if file_object_id is not None
            else _compute_config_path(config_object_id, cfg.processing_dir))


def get_archive_path(config_object_id: ObjectId, cfg: FileProcessorConfig, file_object_id: ObjectId = None) -> Optional[ObjectId]:
    """Gets an ObjectId of the archive directory for a given config or an object below it
    :param config_object_id: The ObjectId of the processor config file
    :param cfg: The deserialized processor config
    :param file_object_id: An optional object to locate in the directory
    if archive_dir is not specified, return none
    """
    if cfg.archive_dir:
        return (_compute_config_path(config_object_id, cfg.archive_dir, filename(file_object_id))
                if file_object_id is not None
                else _compute_config_path(config_object_id, cfg.archive_dir))
    return None


def get_error_path(config_object_id: ObjectId, cfg: FileProcessorConfig, file_object_id: ObjectId = None) -> ObjectId:
    """Gets an ObjectId of the error directory for a given config or an object below it
    :param config_object_id: The ObjectId of the processor config file
    :param cfg: The deserialized processor config
    :param file_object_id: An optional object to locate in the directory
    """
    return (_compute_config_path(config_object_id, cfg.error_dir, filename(file_object_id))
            if file_object_id is not None
            else _compute_config_path(config_object_id, cfg.error_dir))


def rename(object_id: ObjectId, new_filename: str) -> ObjectId:
    """Compute a sibling ObjectId in the same namespace and directory
    :param object_id: The starting point object
    :param new_filename: The name to replace the base object's with
    """
    path = str(PurePosixPath(object_id.path).parent.joinpath(new_filename))
    return ObjectId(object_id.namespace, path)


def parent(object_id: ObjectId) -> ObjectId:
    """Computes the directory containing the given object"""
    return ObjectId(object_id.namespace, str(PurePosixPath(object_id.path).parent))


def glob_matches(object_id: ObjectId, config_object_id: ObjectId, cfg: FileProcessorConfig) -> bool:
    """Checks if the configured glob pattern matches the object path relative to the config directory"""
    if object_id.namespace != config_object_id.namespace:
        return False

    try:
        file_path = str(PurePosixPath(object_id.path).relative_to(parent(config_object_id).path))
        globs = cfg.handled_file_glob.replace(" ", "")
        regex = re.compile(f".*({globs})$")
        result = bool(regex.match(file_path))
        LOGGER.info("File {%s} regex match result: %s", file_path, result)
        return result
    except re.error as regex_error:
        LOGGER.error("Regex compilation error: %s", regex_error)
        return False
    except ValueError as value_error:
        LOGGER.error("Value error during glob matching: %s", value_error)
        return False


def mimetype_matches(
    object_id: ObjectId, config_object_id: ObjectId, cfg: FileProcessorConfig, object_store: ObjectStore,
    rest_client: RestClient, tika_host: str
) -> bool:
    """Checks if the configured mimetype matches the inferred mimetype of the uploaded file"""
    if object_id.namespace != config_object_id.namespace:
        return False

    try:
        file_path = str(PurePosixPath(object_id.path).relative_to(parent(config_object_id).path))
        file_name = file_path.split('/')[-1]
        file_object = object_store.read_object(object_id)
        mimetypes = cfg.handled_mimetypes.split(",") if cfg.handled_mimetypes else []

        try:
            response = rest_client.make_request(
                f'{tika_host}/detect/stream', method='put', data=file_object,
                headers={'Content-Type': 'application/octet-stream', 'file-name': file_name}
            )
            if not response or response.status_code != HTTPStatus.OK:
                LOGGER.warning(f'Unexpected response from Tika service: {response.content}')
            else:
                detected_mimetype = response.text
                LOGGER.info(f'Detected mimetype {detected_mimetype}')
                # TODO maybe handle more complex mimetype lookups from mime.types file to provide mapping
                # TODO from several mimetypes to single file extension types, allowing user to specify file type only.
                for mimetype in mimetypes:
                    if detected_mimetype == mimetype:
                        return True
        except Exception as e:
            LOGGER.error('Failed to query Tika service for file mimetype', e)
    except Exception as e:
        LOGGER.error('Failed to read object from object store', e)

    return False


def processor_matches(
    object_id: ObjectId, config_object_id: ObjectId, cfg: FileProcessorConfig, object_store: ObjectStore,
    rest_client: RestClient, tika_host: str, enable_tika: bool
) -> bool:
    matches = False

    if enable_tika:
        matches = mimetype_matches(object_id, config_object_id, cfg, object_store, rest_client, tika_host)

    if not matches:
        matches = glob_matches(object_id, config_object_id, cfg)

    return matches


def filename(object_id: ObjectId) -> str:
    """Gets just the leaf filename of the object"""
    return str(PurePosixPath(object_id.path).name)
