import itertools
import json
import psutil
import time
from typing import List
import uuid

import ray
from ray.actor import ActorHandle
from etl.config import settings
from etl.toml_processor import TOMLProcessor
from etl.general_event_processor import GeneralEventProcessor
from etl.messaging.singleton import singleton
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging

from etl.util import get_logger

LOGGER = get_logger(__name__)

ray._private.utils.get_system_memory = lambda: psutil.virtual_memory().total

if settings.LOCAL_MODE == 'Y':
    ray.init()
else:
    ray.init(address=settings.RAY_HEAD_ADDRESS)

LOGGER.info('''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()), ray.cluster_resources()['CPU']))


@singleton
class ConsumerWorkerManager:
    def __init__(self):
        self.consumer_worker_container: List[ActorHandle] = []
        self.toml_processor = TOMLProcessor.remote()
        processor_list = ray.get(self.toml_processor.get_processors.remote())
        LOGGER.info(f'Available processors length: {len(processor_list)}')


    def stop_all_workers(self):
        for worker_name, worker_actors in self.consumer_worker_container.items():
            for worker_actor in worker_actors:
                # wait on the future to stop the consumers
                ray.get(worker_actor.stop_consumer.remote())

                ray.kill(worker_actor)
            self.consumer_worker_container[worker_name] = []

        LOGGER.info("All consumer workers stopped.")

    def start_all_workers(self):
        started_flag = False
        initial_check_complete = False

        LOGGER.info("Start all workers...")
        if len(self.consumer_worker_container) == 0:
            started_flag = True
            for _ in itertools.repeat(None, settings.num_workers):
                worker_actor: ActorHandle = ConsumerWorker.remote(self.toml_processor)
                worker_actor.run.remote(initial_check_complete)
                initial_check_complete = True
                self.consumer_worker_container.append(worker_actor)

        if not started_flag:
            raise Exception(f'All Consumers already running')
        LOGGER.info("All consumer workers started.")


@ray.remote(max_restarts=settings.max_restarts, max_task_retries=settings.max_retries)
class ConsumerWorker:
    def __init__(self, toml_processor: TOMLProcessor):
        self.consumer_name = settings.consumer_grp_etl_source_file
        self.stop_worker = False
        self.auto_offset_reset = 'earliest'
        self.poll_timeout_ms = 1000
        self.is_closed = False
        self.toml_processor = toml_processor
        # Configuration assumes 1 processor per Kafka consumer
        self.general_event_processor = GeneralEventProcessor.remote(toml_processor=toml_processor)

        # see https://docs.ray.io/en/latest/ray-observability/ray-logging.html
        # let the workers log to default Ray log organization
        # also see https://stackoverflow.com/questions/55272066/how-can-i-use-the-python-logging-in-ray
        self.logger = get_logger(__name__)


        self.consumer_stop_delay_seconds = 2 * self.poll_timeout_ms / 1000
        try:
            self.consumer = KafkaConsumer(bootstrap_servers=settings.kafka_broker,
                                        client_id=str(uuid.uuid4()),
                                        group_id=self.consumer_name,
                                        key_deserializer=lambda k: k.decode('utf-8') if k is not None else k,
                                        value_deserializer=lambda v: json.loads(v) if v is not None else v,
                                        auto_offset_reset=self.auto_offset_reset,
                                        enable_auto_commit=settings.kafka_enable_auto_commit,
                                        max_poll_records=settings.kafka_max_poll_records,
                                        max_poll_interval_ms=settings.kafka_max_poll_interval_ms,
                                        consumer_timeout_ms=30000)
            self.consumer.subscribe([settings.kafka_topic_castiron_etl_source_file])
            self.logger.error(f'Started consumer worker for topic {settings.kafka_topic_castiron_etl_source_file}...')
        except KafkaError as exc:
            self.logger.error(f"Exception {exc}")

    def stop_consumer(self) -> None:
        self.logger.info(f'Stopping consumer worker...')
        self.stop_worker = True

        # waiting for consumer to stop nicely
        time.sleep(self.consumer_stop_delay_seconds)
        self.logger.info(f'Stopped consumer worker...')

    def closed(self):
        return self.is_closed

    def run(self, initial_check_complete) -> None:
        # Have the first worker check for stalled files
        if not initial_check_complete:
            self.logger.error("Checking for stalled files...")
            try:
                self.general_event_processor.checkForProcessingRecords.remote()
            except Exception as e:
                self.logger.error(f"Error from file check: {e}")
            
        while not self.stop_worker:
            records_dict = self.consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=1)

            if records_dict is None or len(records_dict.items()) == 0:
                time.sleep(5)
                continue
            try:
                self.logger.info(f"Received messages: {len(records_dict.items())}")

                for topic_partition, consumer_records in records_dict.items():
                    for record in consumer_records:
                        minio_record = record.value
                        if '.toml' in minio_record['Key']:
                            # Needed to be a separate actor for shared memory access across nodes
                            self.toml_processor.process.remote(minio_record)
                        else:
                            # Executed in the context of this thread/remote.  
                            self.general_event_processor.process.remote(minio_record)

                self.consumer.commit()

                if self.stop_worker:
                    self.consumer.close()
                    self.is_closed = True
                    break
            except BaseException as e:
                self.logger.error('Error while running consumer worker!')
                self.logger.error(e, exc_info=True)


