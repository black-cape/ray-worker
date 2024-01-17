from typing import Dict, List

import ray

from lib_ray_worker.util import get_logger


@ray.remote
class TaskManager:
    """Class set up as a Ray actor to manage tasks by UUID in a way accessible to other actors"""

    def __init__(self):
        # [WS] IMPORTANT: Ray ObjectRefs must be passed within a list, or else their return values will be awaited
        # (we don't want to await in this case since we want the ObjectRef handle in the event of canceling the task)
        self.task_lookup: Dict[str, List[ray.ObjectRef]] = {}
        self.logger = get_logger(__name__)

    def add_task(self, uuid: str, task_handle: List[ray.ObjectRef]) -> None:
        self.task_lookup[uuid] = task_handle

    def remove_task(self, uuid: str) -> None:
        if uuid in self.task_lookup:
            self.task_lookup.pop(uuid)

    async def cancel_task(self, uuid: str) -> None:
        self.logger.debug(f"in function cancel_task() for uuid: {uuid}")
        if uuid in self.task_lookup:
            self.logger.debug(f"found uuid {uuid} in task_lookup, canceling")
            ray.cancel(self.task_lookup[uuid][0])
            self.remove_task(uuid)
            self.logger.info(f"Task canceled for UUID: {uuid}")
