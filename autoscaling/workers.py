from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


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
