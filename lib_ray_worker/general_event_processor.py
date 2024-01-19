import json
import tempfile
from asyncio import gather, run
from pathlib import Path, PurePosixPath
from threading import Thread
from time import sleep
from typing import Dict, Optional
from uuid import uuid4

import ray
from ray.exceptions import RayTaskError, TaskCancelledError, WorkerCrashedError

from lib_ray_worker.adapters.post_gres_wrapper import PostGresWrapper
from lib_ray_worker.file_processor_config import (
    FileProcessorConfig,
    load_python_processor,
)
from lib_ray_worker.messaging.kafka_producer import KafkaMessageProducer
from lib_ray_worker.models.file_status_record import FileStatusRecord
from lib_ray_worker.object_store.interfaces import EventType
from lib_ray_worker.object_store.minio import MinioObjectStore
from lib_ray_worker.object_store.object_id import ObjectId
from lib_ray_worker.pizza_tracker import PizzaTracker
from lib_ray_worker.task_manager import TaskManager
from lib_ray_worker.toml_processor import TOMLProcessor
from lib_ray_worker.utils import short_uuid, get_logger
from lib_ray_worker.utils.constants import (
    STATUS_CANCELED,
    STATUS_FAILED,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    STATUS_SUCCESS,
)
from lib_ray_worker.utils.path_helpers import (
    filename,
    get_archive_path,
    get_canceled_path,
    get_error_path,
    get_inbox_path,
    get_processing_path,
    parent,
    processor_matches,
    rename,
)

ERROR_LOG_SUFFIX = "_error_log_.txt"
file_suffix_to_ignore = [".toml", ".keep", ERROR_LOG_SUFFIX]


@ray.remote
class GeneralEventProcessor:
    """A class that is executed internal to an existing Ray actor to run a previously defined method"""

    def __init__(self, toml_processor: TOMLProcessor, task_manager: TaskManager):
        self._message_producer = KafkaMessageProducer()
        self._object_store = MinioObjectStore()
        self._toml_processor = toml_processor
        self._postgresWrapper = PostGresWrapper()
        self._task_params = {}
        self._task_manager = task_manager
        self._pending_tasks = {}
        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)

        # Start event loop for handling processing results
        event_loop = Thread(target=run, args=(self._event_loop(),))
        event_loop.start()

    async def _restart_stuck_records(self, status: str) -> None:
        stuck_files = await self._postgresWrapper.query(status=status)

        if stuck_files:
            self.logger.debug("Number of %s files: %s", status, len(stuck_files))

            for stuck_file in stuck_files:
                # New object. "Rename" object.
                file_name = stuck_file.original_filename
                new_path = f"01_inbox/{file_name}"
                src_object_id = ObjectId(stuck_file.bucket_name, stuck_file.file_name)
                dest_object_id = ObjectId(stuck_file.bucket_name, new_path)

                metadata = json.loads(stuck_file.minio_metadata)
                metadata.pop("originalFilename", None)
                metadata.pop("X-Amz-Meta-Originalfilename", None)
                metadata.pop("X-Amz-Meta-Id", None)

                self.logger.debug(
                    "Moving file: %s back to original filename: %s to restart processing...",
                    stuck_file.file_name,
                    new_path,
                )

                try:
                    self._object_store.move_object(
                        src_object_id, dest_object_id, metadata
                    )

                    await self._postgresWrapper.delete_by_id(id=stuck_file.id)
                except Exception as e:
                    self.logger.error(
                        "Error restoring file. Possible database/Minio mismatch: %s", e
                    )

    async def restart_processing_records(self) -> None:
        await self._restart_stuck_records(STATUS_PROCESSING)

    async def restart_queued_records(self) -> None:
        await self._restart_stuck_records(STATUS_QUEUED)

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
                # even a object rename is equivalent to a delete and re-insert, if we set status delete each time this
                # occurs we might get status out of sync, for example a move and processing both attempt to set the
                # status and we can end up with status "Deleted" even if it really should be processed
                pass
            elif evt.event_type == EventType.Put:
                if evt.original_filename:
                    if not evt.event_status:
                        self.logger.info(
                            "a file is renamed and placed back in Minio again at %s, renaming file",
                            obj_path,
                        )
                        file_status_record: FileStatusRecord = (
                            self._postgresWrapper.parse_notification(evt_data)
                        )

                        await self._postgresWrapper.insert(
                            file_status_record=file_status_record
                        )
                        await self._file_put(evt.object_id, file_status_record)
                else:
                    self.logger.info(
                        "new file drop detected at %s, renaming file", obj_path
                    )
                    dirpath = obj_path.parent
                    file_name = obj_path.name
                    obj_uuid = str(uuid4())
                    new_path = f"{dirpath}/{obj_uuid}-{file_name}"
                    dest_object_id = ObjectId(evt.object_id.namespace, f"{new_path}")

                    metadata = evt_data["Records"][0]["s3"]["object"].get(
                        "userMetadata", {}
                    )
                    metadata["originalFilename"] = file_name
                    metadata["id"] = obj_uuid

                    self._object_store.move_object(
                        evt.object_id, dest_object_id, metadata
                    )

    async def _file_put(
        self, object_id: ObjectId, file_status_record: FileStatusRecord
    ):
        """
        Handle possible data file puts, look for matching toml processor to process it.
        """
        # pylint: disable=too-many-locals,too-many-branches, too-many-statements
        self.logger.info("received file put event for path %s", object_id)

        processor_dict: Dict[
            ObjectId, FileProcessorConfig
        ] = await self._toml_processor.get_processors.remote()
        for config_object_id, processor in processor_dict.items():
            if parent(object_id) != get_inbox_path(
                config_object_id, processor
            ) or not processor_matches(
                object_id=object_id, config_object_id=config_object_id, cfg=processor
            ):
                # File isn't in our inbox directory or filename doesn't match our glob pattern
                continue

            self.logger.info(
                "matching ETL processing config found, processing %s", config_object_id
            )

            # Hypothetical file paths for each directory
            processing_file_path = get_processing_path(
                config_object_id, processor, object_id
            )

            job_id = short_uuid()
            self._message_producer.job_created(
                job_id, filename(object_id), filename(config_object_id), "castiron"
            )

            # mv to processing
            metadata = self._object_store.retrieve_object_metadata(object_id)
            metadata["status"] = STATUS_PROCESSING
            self._object_store.move_object(object_id, processing_file_path, metadata)

            file_status_record.status = STATUS_PROCESSING
            file_status_record.file_name = processing_file_path.path
            await self._postgresWrapper.update(file_status_record=file_status_record)

            # kick off processing and then return after first match
            task_reference = process_file.remote(
                processing_file_path,
                job_id,
                config_object_id,
                processor,
                metadata,
                processing_file_path,
            )
            # Update the Ray shared memory TaskManager with this task's uuid and reference
            await self._task_manager.add_task.remote(
                file_status_record.id, [task_reference]
            )
            # Update our local task_reference->uuid lookup for tracking when it completes
            self._pending_tasks[task_reference] = file_status_record.id
            # Keep track of the other params for a task for use in _file_put_followup
            self._task_params[file_status_record.id] = (
                processing_file_path,
                file_status_record.id,
                job_id,
                config_object_id,
                processor,
                metadata,
            )
            return

    async def _file_put_followup(
        self,
        object_id: ObjectId,
        uuid: str,
        job_id: str,
        config_object_id: ObjectId,
        processor: FileProcessorConfig,
        metadata: Dict,
        status: str,
        postgres_wrapper: PostGresWrapper,
    ):
        """
        Handle followup steps after processing completes for a file.
        """
        # Hypothetical file paths for each directory
        processing_file = get_processing_path(config_object_id, processor, object_id)
        archive_file: Optional[ObjectId] = get_archive_path(
            config_object_id, processor, object_id
        )
        error_file = get_error_path(config_object_id, processor, object_id)
        canceled_file: ObjectId = get_canceled_path(
            config_object_id, processor, object_id
        )
        error_log_file_name = (
            f'{filename(object_id).replace(".", "_")}{ERROR_LOG_SUFFIX}'
        )
        error_log_file = get_error_path(
            config_object_id, processor, rename(object_id, error_log_file_name)
        )

        if status == STATUS_FAILED:
            with tempfile.TemporaryDirectory() as work_dir:
                base_path = PurePosixPath(work_dir)
                with open(base_path / "out.txt", "w") as out:
                    # Failure. mv to failed
                    metadata["status"] = STATUS_FAILED
                    self._object_store.move_object(
                        processing_file, error_file, metadata
                    )

                    self.logger.warning(
                        "file processing failed, moving to error location %s",
                        error_file.path,
                    )

                    await postgres_wrapper.update_by_id(
                        id=uuid, status=STATUS_FAILED, file_name=error_file.path
                    )

                    self._message_producer.job_evt_status(job_id, "failure")

                    # Optionally save error log to failed, use same metadata as original file
                    if processor.save_error_log:
                        self._object_store.upload_object(
                            dest=error_log_file,
                            src_file=base_path / "out.txt",
                            metadata=metadata,
                        )
        else:
            # Success, Canceled, or something unexpected. move to archive
            destination = canceled_file if status == STATUS_CANCELED else archive_file
            if destination:
                metadata["status"] = status
                self._object_store.move_object(processing_file, destination, metadata)

                await postgres_wrapper.update_by_id(
                    id=uuid, status=status, file_name=destination.path
                )

                self.logger.info(
                    f"file processing %s, moving to %s", status, destination.path
                )

            kafka_status = (
                "success"
                if status == STATUS_SUCCESS
                else "canceled"
                if status == STATUS_CANCELED
                else status
            )
            self._message_producer.job_evt_status(job_id, kafka_status)

        # Success or not, we handled this
        self.logger.info("finished processing %s", object_id)

    async def _event_loop(self):
        postgres_wrapper: PostGresWrapper = PostGresWrapper()
        while True:
            # make sure ray.wait gets ObjectRefs, not DictKeys
            unfinished = [object_ref for object_ref in self._pending_tasks.keys()]
            while unfinished:
                self.logger.debug("Found unfinished tasks to wait for")
                finished, unfinished = ray.wait(unfinished, num_returns=1)
                for task_reference in finished:
                    task_uuid = self._pending_tasks[task_reference]
                    task_params = self._task_params[task_uuid]
                    status = STATUS_FAILED
                    try:
                        results = await gather(task_reference)
                        self.logger.debug(
                            "got results from awaited task reference: %s", results
                        )
                        success = results[0]
                        status = STATUS_SUCCESS if success else status
                    except (TaskCancelledError, WorkerCrashedError, RayTaskError) as e:
                        self.logger.warning(
                            "Task with UUID %s was canceled or worker crashed: %s",
                            task_uuid,
                            e,
                        )
                        status = STATUS_CANCELED
                    except Exception as e:
                        self.logger.error(
                            "Exception processing results for task with UUID %s: %s",
                            task_uuid,
                            e,
                        )
                    finally:
                        await self._file_put_followup(
                            *task_params,
                            status=status,
                            postgres_wrapper=postgres_wrapper,
                        )
                        del self._pending_tasks[task_reference]
                        del self._task_params[task_uuid]
                        self._task_manager.remove_task.remote(task_uuid)
                # Use latest task list as unfinished list, to ensure we get any newly added tasks
                unfinished = [object_ref for object_ref in self._pending_tasks.keys()]

            # Once we've run out of unfinished tasks, sleep and check again
            sleep(1.0)


@ray.remote
def process_file(
    object_id: ObjectId,
    job_id: str,
    config_object_id: ObjectId,
    processor: FileProcessorConfig,
    metadata: dict,
    processing_file: ObjectId,
) -> bool:
    """Remote (non-actor) task for running the configured processing method for a file"""
    message_producer = KafkaMessageProducer()
    object_store = MinioObjectStore()
    logger = get_logger(__name__)

    with tempfile.TemporaryDirectory() as work_dir:
        # Download to local temp working directory
        base_path = PurePosixPath(work_dir)
        local_data_file = base_path / filename(object_id)
        object_store.download_object(processing_file, str(local_data_file))

        with PizzaTracker(message_producer, work_dir, job_id) as pizza_tracker:
            # pizza tracker has to be called in main thread as it needs things like Kafka connector
            run_method = load_python_processor(processor.python)
            method_kwargs = {}
            if processor.python.supports_pizza_tracker:
                method_kwargs["pizza_tracker"] = pizza_tracker.pipe_file_name
                method_kwargs["pizza_job_id"] = job_id
            if processor.python.supports_metadata:
                method_kwargs["file_metadata"] = metadata

            success = True
            try:
                import asyncio

                if asyncio.iscoroutinefunction(run_method):
                    asyncio.run(run_method(*(str(local_data_file),), **method_kwargs))
                else:
                    run_method(*(str(local_data_file),), **method_kwargs)
            except Exception as e:
                logger.error(
                    f"Error response from configured processor {processor.python}: {e}"
                )
                success = False

            # [WS] check once here to avoid spamming logs
            if processor.python.supports_pizza_tracker:
                logger.debug(
                    "processor supports pizza tracker, will start tracker process"
                )
            else:
                logger.warning(
                    "processor %s does not support pizza tracker", config_object_id
                )

    return success
