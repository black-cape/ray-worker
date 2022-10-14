"""Contains the implementation of the EventProcessor class"""
import logging
import re
from typing import Dict, Optional

import ray

from etl.config import settings
from etl.file_processor_config import (FileProcessorConfig, try_loads)
from etl.object_store.interfaces import EventType
from etl.object_store.minio import MinioObjectStore
from etl.object_store.object_id import ObjectId
from etl.path_helpers import (get_archive_path, get_error_path, get_inbox_path, get_processing_path)
from etl.util import get_logger

ERROR_LOG_SUFFIX = '_error_log_.txt'
file_suffix_to_ignore = ['.toml', '.keep', ERROR_LOG_SUFFIX]


@ray.remote
class TOMLProcessor:
    """A service that processes individual object events"""

    def __init__(self):
        self._object_store = MinioObjectStore()
        self.processors: Dict[ObjectId, FileProcessorConfig] = {}
        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)

        # Load existing config files
        for obj in self._object_store.list_objects(settings.minio_etl_bucket, None, recursive=True):
            if obj.path.endswith('.toml'):
                self._toml_put(obj)

    def get_processors(self):
        return self.processors

    def process(self, evt_data: Dict) -> None:
        """Object event process entry point"""
        evt = self._object_store.parse_notification(evt_data)

        if evt.object_id.path.endswith('.toml'):
            self.logger.info(f'processing ETL processing Config file with path  {evt.object_id.path}')
            if evt.event_type == EventType.Delete:
                self._toml_delete(evt.object_id)
            elif evt.event_type == EventType.Put:
                self._toml_put(evt.object_id)
            self.logger.info(f'finished ETL processing Config file with path  {evt.object_id.path}')

    def _toml_put(self, toml_object_id: ObjectId) -> bool:
        """Handle put event with TOML extension.
        :return: True if the operation is successful.
        """
        try:
            obj = self._object_store.read_object(toml_object_id)
            data: str = obj.decode('utf-8')
            cfg: FileProcessorConfig = try_loads(data)

            if cfg.enabled:
                if cfg.handled_file_glob:
                    #Validate regex expression if provided
                    try:
                        re.compile(cfg.handled_file_glob)
                    except re.error as e:
                        self.logger.error(f"Invalid regex provided.  Unable to register TOML.  Error: {e}")
                        return False

                # Register processor
                self.processors[toml_object_id] = cfg
                self.logger.info('number of processor configs: %s', len(self.processors))
                for processor_key in self.processors.keys():
                    self.logger.info(processor_key)

                self._object_store.ensure_directory_exists(get_inbox_path(toml_object_id, cfg))
                self._object_store.ensure_directory_exists(get_processing_path(toml_object_id, cfg))
                self._object_store.ensure_directory_exists(get_error_path(toml_object_id, cfg))
                archive_object_id: Optional[ObjectId] = get_archive_path(toml_object_id, cfg)
                if archive_object_id:
                    self._object_store.ensure_directory_exists(get_archive_path(toml_object_id, cfg))
            return True
        except ValueError as exc:
            self.logger.error(f'Failed to process toml {exc}')
            # Raised if we fail to parse and validate config
            return False

    def _toml_delete(self, toml_object_id: ObjectId) -> bool:
        """Handle remove event with TOML extension.
        :return: True if the object was deleted
        """
        return bool(self.processors.pop(toml_object_id, None))
