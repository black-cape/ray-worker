import psutil
import ray

from lib_ray_worker.messaging.kafka_consumer import consumer_worker_manager
from lib_ray_worker.utils import get_logger
from lib_ray_worker.config import adapters

LOGGER = get_logger(__name__)


def main():
    ray._private.utils.get_system_memory = lambda: psutil.virtual_memory().total

    if not ray.is_initialized:
        if adapters.LOCAL_MODE == "Y":
            ray.init()
        else:
            ray.init(address=adapters.RAY_HEAD_ADDRESS)

    LOGGER.info(
        """This cluster consists of
        {} nodes in total
        {} CPU resources in total
    """.format(
            len(ray.nodes()), ray.cluster_resources()["CPU"]
        )
    )

    LOGGER.info("starting all workers")
    consumer_worker_manager.start()


if __name__ == "__main__":
    main()
