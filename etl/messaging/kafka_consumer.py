from typing import List

import itertools
import psutil
import ray
from ray.actor import ActorHandle

from etl.config import settings
from etl.messaging.singleton import singleton
from etl.task_manager import TaskManager
from etl.toml_processor import TOMLProcessor
from etl.util import get_logger
from etl.messaging.s3_bucket_workflow_worker import S3BucketWorkFlowWorker
from etl.messaging.streaming_txt_payload_worker import StreamingTextPayloadWorker

LOGGER = get_logger(__name__)

ray._private.utils.get_system_memory = lambda: psutil.virtual_memory().total

if settings.LOCAL_MODE == 'Y':
    ray.init()
else:
    ray.init(address=settings.RAY_HEAD_ADDRESS)

LOGGER.info(
    '''This cluster consists of
    {} nodes in total
    {} CPU resources in total
'''.format(len(ray.nodes()),
           ray.cluster_resources()['CPU'])
)


@singleton
class ConsumerWorkerManager:
    """
    initialized via FAST API life cycle methods to spin up and down Ray workers, see etl/app_manager for more details
    """

    def __init__(self):
        self.consumer_worker_container: List[ActorHandle] = []
        self.toml_processor = TOMLProcessor.remote()
        self.task_manager = TaskManager.remote()
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
            LOGGER.info("Start S3 Bucket Workflow Workers...")
            for _ in itertools.repeat(None, settings.num_s3_workflow_workers):
                worker_actor: ActorHandle = S3BucketWorkFlowWorker.remote(self.toml_processor, self.task_manager)
                # [WS] NOTE: the initial check should be only run by one single ConsumerWorker, so we pass this boolean
                # value through here as False once (prompting a check) and then True for the rest (no check).
                worker_actor.run.remote(initial_check_complete)
                initial_check_complete = True
                self.consumer_worker_container.append(worker_actor)

            LOGGER.info("Start Text Payload Streaming Workers...")
            for _ in itertools.repeat(None, settings.num_text_streaming_workers):
                worker_actor: ActorHandle = StreamingTextPayloadWorker.remote()
                worker_actor.run.remote()
                self.consumer_worker_container.append(worker_actor)

        if not started_flag:
            raise Exception(f'All Consumers already running')
        LOGGER.info("All consumer workers started.")

    async def cancel_processing_task(self, task_uuid: str) -> bool:
        LOGGER.info(f'Canceling task for UUID: {task_uuid}')
        await self.task_manager.cancel_task.remote(task_uuid)
        return True

