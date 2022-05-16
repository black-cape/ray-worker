import tempfile

from pathlib import Path, PurePosixPath
from typing import Dict, Optional
from uuid import uuid4

from etl.config import settings
from etl.database.database import PGDatabase
from etl.toml_processor import TOMLProcessor
from etl.file_processor_config import FileProcessorConfig, load_python_processor
from etl.messaging.interfaces import MessageProducer
from etl.object_store.interfaces import EventType, ObjectStore
from etl.object_store.object_id import ObjectId
from etl.path_helpers import (filename, get_archive_path, get_error_path,
                              get_inbox_path, get_processing_path, parent,
                              processor_matches, rename)
from etl.util import create_rest_client, get_logger, short_uuid
from etl.pizza_tracker import PizzaTracker


LOGGER = get_logger(__name__)
ERROR_LOG_SUFFIX = '_error_log_.txt'
file_suffix_to_ignore = ['.toml', '.keep', ERROR_LOG_SUFFIX]


class GeneralEventProcessor:
    """A class that is executed internal to an existing Ray actor to run a previously defined method"""

    def __init__(self, object_store: ObjectStore, message_producer: MessageProducer, toml_processor: TOMLProcessor):
        self._object_store = object_store
        self._message_producer = message_producer
        self._toml_processor = toml_processor
        self._rest_client = create_rest_client()
        self._database = PGDatabase()
        self._database_active = False

    async def process(self, evt_data: Dict) -> None:
        """Object event process entry point"""

        # Check for database connection
        if not self._database_active:
            self._database_active = await self._database.create_table()
        db_evt = {}

        evt = self._object_store.parse_notification(evt_data)
        # this processor would get TOML config as well as regular file upload, there doesn't seem to be a way
        # to filter bucket notification to exclude by file path
        if any([evt.object_id.path.endswith(i) for i in file_suffix_to_ignore]):
            pass
        else:
            if evt.event_type == EventType.Delete:
                pass
            elif evt.event_type == EventType.Put:
                if evt.original_filename:
                    if not evt.event_status:
                        if self._database_active:
                            db_evt = self._database.parse_notification(evt_data)
                            try:
                                await self._database.insert_file(db_evt)
                            except Exception as e:
                                LOGGER.error(f'Database error.  Unable to process/track file.  Exception: {e}')
                        await self._file_put(evt.object_id, db_evt.get('id', None))
                else:
                    # New object. "Rename" object.
                    obj_path = Path(evt.object_id.path)
                    dirpath = obj_path.parent
                    filename = obj_path.name
                    obj_uuid = str(uuid4())
                    new_path = f'{dirpath}/{obj_uuid}-{filename}'
                    dest_object_id = ObjectId(evt.object_id.namespace, f'{new_path}')

                    metadata = evt_data['Records'][0]['s3']['object'].get('userMetadata', {})
                    metadata['originalFilename'] = filename
                    metadata['id'] = obj_uuid

                    self._object_store.move_object(evt.object_id, dest_object_id, metadata)


    async def _file_put(self, object_id: ObjectId, uuid: str) -> bool:
        """Handle possible data file puts.
        :return: True if successful.
        """
        # pylint: disable=too-many-locals,too-many-branches, too-many-statements
        LOGGER.error(f'received file put event for path {object_id}')

        processor_dict: Dict[ObjectId, FileProcessorConfig] = await self._toml_processor.get_processors.remote()
        for config_object_id, processor in processor_dict.items():
            if (
                parent(object_id) != get_inbox_path(config_object_id, processor) or
                not processor_matches(
                    object_id, config_object_id, processor, self._object_store, self._rest_client,
                    settings.tika_host, settings.enable_tika
                )
            ):
                # File isn't in our inbox directory or filename doesn't match our glob pattern
                continue

            LOGGER.error(f'matching ETL processing config found, processing {config_object_id}')

            # Hypothetical file paths for each directory
            processing_file = get_processing_path(config_object_id, processor, object_id)
            archive_file: Optional[ObjectId] = get_archive_path(config_object_id, processor, object_id)
            error_file = get_error_path(config_object_id, processor, object_id)
            error_log_file_name = f'{filename(object_id).replace(".", "_")}{ERROR_LOG_SUFFIX}'
            error_log_file = get_error_path(config_object_id, processor, rename(object_id, error_log_file_name))

            job_id = short_uuid()
            self._message_producer.job_created(job_id, filename(object_id), filename(config_object_id), 'castiron')

            # mv to processing
            metadata = self._object_store.retrieve_object_metadata(object_id)
            metadata['status'] = 'Processing'
            self._object_store.move_object(object_id, processing_file, metadata)
            if self._database_active:
                await self._database.update_status(uuid, 'Processing', processing_file.path)

            with tempfile.TemporaryDirectory() as work_dir:
                # Download to local temp working directory
                base_path = PurePosixPath(work_dir)
                local_data_file = base_path / filename(object_id)
                self._object_store.download_object(processing_file, str(local_data_file))

                with open(base_path / 'out.txt', 'w') as out, \
                        PizzaTracker(self._message_producer, work_dir, job_id) as pizza_tracker:

                    success = True

                    # pizza tracker has to be called in main thread as it needs things like Kafka connector
                    run_method = load_python_processor(processor.python)
                    method_kwargs = {}
                    if processor.python.supports_pizza_tracker:
                        method_kwargs['pizza_tracker'] = pizza_tracker.pipe_file_name
                        method_kwargs['pizza_job_id'] = job_id
                    if processor.python.supports_metadata:
                        method_kwargs['file_metadata'] = metadata

                    success = True
                    try:
                        results = run_method(*(str(local_data_file),), **method_kwargs)
                    except Exception as e:
                        LOGGER.error(f"Error response from configured processor {processor.python}: {e}")
                        success = False

                    # [WS] check once here to avoid spamming logs
                    if processor.python.supports_pizza_tracker:
                        LOGGER.debug('processor supports pizza tracker, will start tracker process')
                    else:
                        LOGGER.warning(f'processor {config_object_id} does not support pizza tracker')

                    if success:
                        # Success. mv to archive
                        if archive_file:
                            metadata['status'] = 'Success'
                            self._object_store.move_object(processing_file, archive_file, metadata)
                        if self._database_active:
                            await self._database.update_status(uuid, 'Success', archive_file.path)
                        self._message_producer.job_evt_status(job_id, 'success')
                    else:
                        # Failure. mv to failed
                        metadata['status'] = 'Failed'
                        self._object_store.move_object(processing_file, error_file, metadata)
                        if self._database_active:
                            await self._database.update_status(uuid, 'Failed', error_file.path)
                        self._message_producer.job_evt_status(job_id, 'failure')

                        # Optionally save error log to failed, use same metadata as original file
                        if processor.save_error_log:
                            self._object_store.upload_object(dest=error_log_file,
                                                             src_file=base_path / 'out.txt',
                                                             metadata=metadata)

                # Success or not, we handled this
                LOGGER.info(f'finished processing {object_id}')
                return True

        # Not our table
        return False
