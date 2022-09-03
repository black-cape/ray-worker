import json
import tempfile
from pathlib import Path, PurePosixPath
from typing import Dict, Optional
from uuid import uuid4
from typing import List

import ray

from etl.config import settings
from etl.database.database import ClickHouseDatabase
from etl.database.interfaces import FileObject, STATUS_FAILED, STATUS_PROCESSING, STATUS_SUCCESS
from etl.file_processor_config import FileProcessorConfig, load_python_processor
from etl.messaging.kafka_producer import KafkaMessageProducer
from etl.object_store.interfaces import EventType
from etl.object_store.minio import MinioObjectStore
from etl.object_store.object_id import ObjectId
from etl.path_helpers import (filename, get_archive_path, get_error_path,
                              get_inbox_path, get_processing_path, parent,
                              processor_matches, rename)
from etl.pizza_tracker import PizzaTracker
from etl.toml_processor import TOMLProcessor
from etl.util import create_rest_client, short_uuid, get_logger

ERROR_LOG_SUFFIX = '_error_log_.txt'
file_suffix_to_ignore = ['.toml', '.keep', ERROR_LOG_SUFFIX]

@ray.remote
class GeneralEventProcessor:
    """A class that is executed internal to an existing Ray actor to run a previously defined method"""

    def __init__(self, toml_processor: TOMLProcessor):
        self._message_producer = KafkaMessageProducer()
        self._object_store = MinioObjectStore()
        self._toml_processor = toml_processor
        self._rest_client = create_rest_client()
        self._database = ClickHouseDatabase()
        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)


    async def checkForProcessingRecords(self):
        missed_files:Optional[List[FileObject]] = await self._database.query(status=STATUS_PROCESSING)

        if missed_files:
            self.logger.error(f"Number of missed files: {len(missed_files)}")

            for stalled_file in missed_files:
                    # New object. "Rename" object.
                    filename = stalled_file.original_filename
                    new_path = f'01_inbox/{filename}'
                    src_object_id = ObjectId(stalled_file.bucket_name, stalled_file.file_name)
                    dest_object_id = ObjectId(stalled_file.bucket_name, f'{new_path}')

                    metadata = json.loads(stalled_file.metadata)
                    metadata.pop('originalFilename', None)
                    metadata.pop('X-Amz-Meta-Originalfilename', None)
                    metadata.pop('X-Amz-Meta-Id', None)

                    self.logger.error(f"Moving file: {stalled_file.file_name} back to original filename: {new_path} to restart processing...")

                    try:
                        self._object_store.move_object(src_object_id, dest_object_id, metadata)
                        await self._database.delete_file(rowid=stalled_file.id)
                    except Exception as e:
                        self.logger.error(f"Error restoring file.  Possible database/Minio mismatch: {e}")


    async def process(self, evt_data: Dict) -> None:
        """Object event process entry point"""
        evt = self._object_store.parse_notification(evt_data)
        # this processor would get TOML config as well as regular file upload, there doesn't seem to be a way
        # to filter bucket notification to exclude by file path
        if any([evt.object_id.path.endswith(i) for i in file_suffix_to_ignore]):
            pass
        else:
            obj_path = Path(evt.object_id.path)
            if evt.event_type == EventType.Delete:
                #even a object rename is equivalent to a delete and re-insert, if we set status delete each time this
                #occurs we might get status out of sync, for example a move and processing both attempt to set the status
                #and we can end up with status "Deleted" even if it really should be processed
                pass
            elif evt.event_type == EventType.Put:
                if evt.original_filename:
                    if not evt.event_status:
                        self.logger.info(f'a file is renamed and placed back in Minio again at {obj_path}, renaming file')
                        db_evt:FileObject = self._database.parse_notification(evt_data)
                        await self._database.insert_file(db_evt)
                        await self._file_put(evt.object_id, db_evt.id)
                else:
                    self.logger.info(f'new file drop detected at {obj_path}, renaming file')
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
        """Handle possible data file puts, look for matching toml processor to process it
        :return: True if successful.
        """
        # pylint: disable=too-many-locals,too-many-branches, too-many-statements
        self.logger.info(f'received file put event for path {object_id}')

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

            self.logger.info(f'matching ETL processing config found, processing {config_object_id}')

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
            metadata['status'] = STATUS_PROCESSING
            self._object_store.move_object(object_id, processing_file, metadata)
            await self._database.update_status_and_fileName(uuid, STATUS_PROCESSING, processing_file.path)

            with tempfile.TemporaryDirectory() as work_dir:
                # Download to local temp working directory
                base_path = PurePosixPath(work_dir)
                local_data_file = base_path / filename(object_id)
                self._object_store.download_object(processing_file, str(local_data_file))

                with open(base_path / 'out.txt', 'w') as out, \
                        PizzaTracker(self._message_producer, work_dir, job_id) as pizza_tracker:

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
                        run_method(*(str(local_data_file),), **method_kwargs)
                    except Exception as e:
                        self.logger.error(f"Error response from configured processor {processor.python}: {e}")
                        success = False

                    # [WS] check once here to avoid spamming logs
                    if processor.python.supports_pizza_tracker:
                        self.logger.debug('processor supports pizza tracker, will start tracker process')
                    else:
                        self.logger.warning(f'processor {config_object_id} does not support pizza tracker')

                    if success:
                        # Success. mv to archive
                        if archive_file:
                            metadata['status'] = STATUS_SUCCESS
                            self._object_store.move_object(processing_file, archive_file, metadata)

                            await self._database.update_status_and_fileName(uuid, STATUS_SUCCESS, archive_file.path)
                            self.logger.info(f'file processing success, moving to archive {archive_file.path}')

                        self._message_producer.job_evt_status(job_id, 'success')
                    else:
                        # Failure. mv to failed
                        metadata['status'] = STATUS_FAILED
                        self._object_store.move_object(processing_file, error_file, metadata)

                        self.logger.warn(f'file processing failed, moving to error location {error_file.pat}')
                        await self._database.update_status_and_fileName(uuid, STATUS_FAILED, error_file.path)
                        self._message_producer.job_evt_status(job_id, 'failure')

                        # Optionally save error log to failed, use same metadata as original file
                        if processor.save_error_log:
                            self._object_store.upload_object(dest=error_log_file,
                                                             src_file=base_path / 'out.txt',
                                                             metadata=metadata)

                # Success or not, we handled this
                self.logger.info(f'finished processing {object_id}')
                return True

        # Not our table
        return False
