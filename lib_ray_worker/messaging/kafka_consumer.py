import itertools
from threading import Thread
from typing import List

import ray
from ray.actor import ActorHandle

from lib_ray_worker.config import adapters
from lib_ray_worker.messaging.s3_bucket_workflow_worker import S3BucketWorkFlowWorker
from lib_ray_worker.task_manager import TaskManager
from lib_ray_worker.toml_processor import TOMLProcessor
from lib_ray_worker.utils import get_logger

LOGGER = get_logger(__name__)


class ConsumerWorkerManager(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.consumer_worker_container: List[ActorHandle] = []
        self.toml_processor = TOMLProcessor.remote()
        self.task_manager = TaskManager.remote()
        self.running = True
        self.started_flag = False
        processor_list = ray.get(self.toml_processor.get_processors.remote())
        LOGGER.info(f"Available processors length: {len(processor_list)}")

    def run(self):
        while self.running:
            if not self.started_flag:
                self.start_all_workers()
        self.stop_all_workers()

    def stop_all_workers(self):
        for worker_name, worker_actors in self.consumer_worker_container.items():
            for worker_actor in worker_actors:
                # wait on the future to stop the consumers
                ray.get(worker_actor.stop_consumer.remote())

                ray.kill(worker_actor)
            self.consumer_worker_container[worker_name] = []

        LOGGER.info("All consumer workers stopped.")

    def start_all_workers(self):
        self.started_flag = False
        initial_check_complete = False

        LOGGER.info("Start all workers...")
        if len(self.consumer_worker_container) == 0:
            self.started_flag = True
            LOGGER.info("Start S3 Bucket Workflow Workers...")
            for _ in itertools.repeat(None, adapters.NUM_S3_WORKFLOW_WORKERS):
                worker_actor: ActorHandle = S3BucketWorkFlowWorker.remote(
                    self.toml_processor, self.task_manager
                )
                # [WS] NOTE: the initial check should be only run by one single ConsumerWorker, so we pass this boolean
                # value through here as False once (prompting a check) and then True for the rest (no check).
                worker_actor.run.remote(initial_check_complete)
                initial_check_complete = True
                self.consumer_worker_container.append(worker_actor)

        LOGGER.info("All consumer workers started.")

    async def cancel_processing_task(self, task_uuid: str) -> bool:
        LOGGER.info("Canceling task for UUID: %s", task_uuid)
        await self.task_manager.cancel_task.remote(task_uuid)
        return True


consumer_worker_manager = ConsumerWorkerManager()
