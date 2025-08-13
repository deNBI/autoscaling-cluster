import simplevm_api
import utils
from classes.configuration import Configuration
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def get_cluster_workers_from_api(config: Configuration):
    cluster_data = simplevm_api.get_cluster_data()
    return get_cluster_workers(cluster_data=cluster_data, config=config)


def worker_to_cluster_data(key, cluster_workers):
    if cluster_workers:
        for c_worker in cluster_workers:
            if c_worker["hostname"] == key:
                return c_worker
        logger.error("worker %s not in cluster data", key)
    return None


def cluster_worker_to_flavor(flavors_data, cluster_worker):
    """
    Returns a corresponding flavor to a worker
    :param flavors_data: flavor data
    :param cluster_worker: cluster data from worker
    :return: flavor
    """

    if cluster_worker:
        for fv_data in flavors_data:
            if cluster_worker["flavor"]["name"] == fv_data["flavor"]["name"]:
                return fv_data
        logger.error("flavor not found for worker %s", cluster_worker["flavor"])
    return None


def worker_filter(cluster_workers, config: Configuration):
    ignore_workers = config.ignore_workers

    if ignore_workers:
        index = 0
        for key in cluster_workers:
            if key["hostname"] in ignore_workers:
                cluster_workers.pop(index)
                logger.debug("remove %s", key)
            index += 1
        logger.warning("ignore worker: %s", ignore_workers)


def get_cluster_workers(cluster_data, config: Configuration):
    """
    Modify cluster worker data.
    :param cluster_data:
    :return:
    """
    cluster_workers = None
    if cluster_data:
        cluster_workers = cluster_data.get("workers", [])
        logger.info(cluster_workers)

        for w_data in cluster_workers:
            logger.info(f"{w_data} ")
            flavor_data = w_data["flavor"]
            w_data.update(
                {
                    "memory_usable": utils.reduce_flavor_memory(
                        utils.convert_mib_to_gb(flavor_data["ram"])
                    )
                }
            )

            w_data.update({"temporary_disk": flavor_data["ephemeral"]})
        worker_filter(cluster_workers=cluster_workers, config=config)
    return cluster_workers
