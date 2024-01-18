import psutil
import ray

from lib_ray_worker.messaging.kafka_consumer import consumer_worker_manager
from lib_ray_worker.util import get_logger
from lib_ray_worker.config import settings

LOGGER = get_logger(__name__)


def main():
    ray._private.utils.get_system_memory = lambda: psutil.virtual_memory().total

    if not ray.is_initialized:
        if settings.LOCAL_MODE == "Y":
            ray.init()
        else:
            ray.init(address=settings.RAY_HEAD_ADDRESS)

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
