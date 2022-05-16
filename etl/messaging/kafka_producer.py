"""Contains implementation of MessageProducer backend using Kafka"""
from json import dumps
from typing import Any

from kafka import KafkaProducer

from etl.config import settings
from etl.messaging.interfaces import MessageProducer


class KafkaMessageProducer(MessageProducer):
    """Implementation of MessageProducer backend using Kafka"""

    def __init__(self):
        self._producer = KafkaProducer(bootstrap_servers=[settings.kafka_broker])

    def _publish(self, data: Any):
        self._producer.send(settings.kafka_pizza_tracker_topic, dumps(data, default=str).encode('utf-8'))
        self._producer.flush()

    def job_created(self, job_id: str, filename: str, handler: str, uploader: str) -> None:
        self._publish({
            'type': 'job_created',
            'job_id': job_id,
            'filename': filename,
            'handler': handler,
            'uploader': uploader
        })

    def job_evt_task(self, job_id: str, task: str) -> None:
        self._publish({
            'type': 'job_update',
            'job_id': job_id,
            'task': task
        })

    def job_evt_status(self, job_id: str, status: str) -> None:
        self._publish({
            'type': 'job_update',
            'job_id': job_id,
            'status': status
        })

    def job_evt_progress(self, job_id: str, progress: float) -> None:
        print(f'logging job_evt_progress from kafka producer. job_id: {job_id}, progress: {progress}')
        self._publish({
            'type': 'job_update',
            'job_id': job_id,
            'progress': progress
        })

    def job_evt_committed(self, job_id: str, committed: int) -> None:
        self._publish({
            'type': 'job_update',
            'job_id': job_id,
            'committed': committed
        })
