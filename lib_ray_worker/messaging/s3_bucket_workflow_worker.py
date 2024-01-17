import json
import uuid

import ray
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from lib_ray_worker.config import settings
from lib_ray_worker.general_event_processor import GeneralEventProcessor
from lib_ray_worker.task_manager import TaskManager
from lib_ray_worker.toml_processor import TOMLProcessor
from lib_ray_worker.util import get_logger


# unfortunately making a super class in Ray is not easy/supported https://github.com/ray-project/ray/issues/449
@ray.remote(max_restarts=settings.max_restarts, max_task_retries=settings.max_retries)
class S3BucketWorkFlowWorker:
    def __init__(self, toml_processor: TOMLProcessor, task_manager: TaskManager):
        self.stop_worker = False
        self.is_closed = False
        self.toml_processor = toml_processor
        # Configuration assumes 1 processor per Kafka consumer
        self.general_event_processor = GeneralEventProcessor.remote(
            toml_processor=toml_processor, task_manager=task_manager
        )

        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)

        self.consumer_stop_delay_seconds = 2
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=settings.kafka_bootstrap_server,
                client_id=str(uuid.uuid4()),
                group_id=settings.consumer_grp_etl_source_file,
                key_deserializer=lambda k: k.decode("utf-8") if k is not None else k,
                value_deserializer=lambda v: json.loads(v) if v is not None else v,
                auto_offset_reset="earliest",
                enable_auto_commit=settings.kafka_enable_auto_commit,
                max_poll_records=settings.kafka_max_poll_records,
                max_poll_interval_ms=settings.kafka_max_poll_interval_ms,
                consumer_timeout_ms=30000,
            )
            self.consumer.subscribe([settings.kafka_topic_castiron_etl_source_file])
            self.logger.info(
                "Started consumer worker for topic %s...",
                settings.kafka_topic_castiron_etl_source_file,
            )
        except KafkaError as exc:
            self.logger.error(exc)

    def stop_consumer(self) -> None:
        self.logger.info("Stopping consumer worker...")
        self.stop_worker = True

        # waiting for consumer to stop nicely
        time.sleep(self.consumer_stop_delay_seconds)
        self.logger.info("Stopped consumer worker...")

    def closed(self):
        return self.is_closed

    async def run(self, initial_check_complete: bool) -> None:
        """
        handles processing of either TOML config files or general data files
        """
        # Have the first worker check for stalled files
        if not initial_check_complete:
            self.logger.info("Checking for stalled files...")
            try:
                self.general_event_processor.restart_processing_records.remote()
            except Exception as e:
                self.logger.error(e)
        else:
            self.logger.debug(
                "Another ConsumerWorker is performing the initial check, skipping..."
            )

        while not self.stop_worker:
            records_dict = self.consumer.poll(timeout_ms=1000, max_records=1)

            if records_dict is None or len(records_dict.items()) == 0:
                time.sleep(5)
                continue
            try:
                for topic_partition, consumer_records in records_dict.items():
                    for record in consumer_records:
                        minio_record = record.value
                        if ".toml" in minio_record["Key"]:
                            # Needed to be a separate actor for shared memory access across nodes
                            await self.toml_processor.process.remote(minio_record)
                            # Wait for new toml processing to complete, then restart queued records in case any match it
                            self.general_event_processor.restart_queued_records.remote()
                        else:
                            # Executed in the context of this thread/remote.
                            self.general_event_processor.process.remote(minio_record)

                self.consumer.commit()

                if self.stop_worker:
                    self.consumer.close()
                    self.is_closed = True
                    break
            except Exception as e:
                self.logger.error("Error while running consumer worker!")
                self.logger.error(e, exc_info=True)
