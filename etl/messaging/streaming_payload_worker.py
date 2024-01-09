import json
import uuid

import ray
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from etl.config import settings
from etl.util import get_logger
from etl.file_processor_config import load_member


def process(data: str, **kwargs):
    print("Got Data in streaming_txt_payload")
    print(data)


MSG_KEY_WORKER_RUN_METHOD = "worker_run_method"
MSG_KEY_DATA = "data"


# unfortunately making a super class in Ray is not easy/supported https://github.com/ray-project/ray/issues/449
@ray.remote(max_restarts=settings.max_restarts, max_task_retries=settings.max_retries)
class StreamingPayloadWorker:
    def __init__(self, group_id: str, kafka_topic: str):
        self.stop_worker = False
        self.is_closed = False
        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)

        self.consumer_stop_delay_seconds = 2
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=settings.kafka_bootstrap_server,
                client_id=str(uuid.uuid4()),
                group_id=group_id,
                key_deserializer=lambda k: k.decode("utf-8") if k is not None else k,
                value_deserializer=lambda v: json.loads(v) if v is not None else v,
                auto_offset_reset="earliest",
                enable_auto_commit=settings.kafka_enable_auto_commit,
                max_partition_fetch_bytes=settings.kafka_max_partition_fetch_bytes,
                max_poll_records=settings.kafka_max_poll_records,
                max_poll_interval_ms=settings.kafka_max_poll_interval_ms,
                consumer_timeout_ms=30000,
            )
            self.consumer.subscribe([kafka_topic])
            self.logger.info("Started consumer worker for topic %s...", kafka_topic)
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

    async def run(self) -> None:
        while not self.stop_worker:
            records_dict = self.consumer.poll(timeout_ms=1000, max_records=1)

            if records_dict is None or len(records_dict.items()) == 0:
                time.sleep(5)
                continue

            non_arg_keys = [MSG_KEY_WORKER_RUN_METHOD, MSG_KEY_DATA]
            for topic_partition, consumer_records in records_dict.items():
                for record in consumer_records:
                    try:
                        worker_run_method = record.value[MSG_KEY_WORKER_RUN_METHOD]
                        data: str = record.value[MSG_KEY_DATA]
                        arg_list = {
                            k: v
                            for k, v in record.value.items()
                            if k not in non_arg_keys
                        }
                        self.logger.info(
                            "invoking %s with arg_list %s", worker_run_method, arg_list
                        )
                        worker_run_callable = load_member(import_path=worker_run_method)
                        worker_run_callable(data=data, **arg_list)
                    except Exception as e:
                        self.logger.error("Error processing record %s", record.value)
                        self.logger.error(e, exc_info=True)

            self.consumer.commit()

            if self.stop_worker:
                self.consumer.close()
                self.is_closed = True
                break
