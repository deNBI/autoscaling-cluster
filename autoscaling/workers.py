import sys
import time

import simplevm_api
import utils
from classes.configuration import Configuration
from classes.flavor import Flavor
from classes.worker import Worker
from constants import WAIT_CLUSTER_SCALING
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


def worker_filter(cluster_workers:list[Worker], config: Configuration):
    ignore_workers = config.ignore_workers

    if ignore_workers:
        index = 0
        for key in cluster_workers:
            if key.hostname in ignore_workers:
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
        cluster_workers =[Worker(hostname=worker["hostname"],status=worker["status"],task_state=worker["task_state"],flavor=Flavor(name=worker["flavor"]["name"],ram=worker["flavor"]["ram"],vcpus=worker["flavor"]["vcpus"],ephemeral_disk=worker["flavor"]["ephemeral_disk"],root_dsik=worker["flavor"]["disk"])) for worker in  cluster_data.get("workers", []))
        logger.info(cluster_workers)

        worker_filter(cluster_workers=cluster_workers, config=config)
    return cluster_workers


def __worker_states(config: Configuration):
    """
    Recall cluster data from server and return worker statistics.
    :return:
        - worker_active: list of workers with state active
        - worker_error: list of workers with state error
        - worker_down: list of workers with state down
        - worker_unknown: list of workers with state unknown
        - worker_error: list of error workers
        - worker_drain_list: list of idle with drain workers
        - cluster_workers: cluster data from portal
    """
    worker_active = []
    worker_down = []
    worker_unknown = []
    worker_error = []
    cluster_data = simplevm_api.get_cluster_data()
    logger.info(cluster_data)
    cluster_workers:list[Worker] = get_cluster_workers(cluster_data=cluster_data, config=config)

    if cluster_workers is not None:
        for c_worker in cluster_workers:
            logger.debug(
                "hostname: '%s' status: '%s'", c_worker.hostname, c_worker.status
            )
            if c_worker.is_down():
                worker_down.append(c_worker.hostname)
            elif c_worker.is_in_error():
                logger.error("ERROR %s", c_worker.hostname)
                worker_error.append(c_worker.hostname)
            elif c_worker.is_creation_failed():
                logger.error("CREATION FAILED %s", c_worker.hostname)
                worker_error.append(c_worker.hostname)
            elif c_worker.is_failed():
                logger.error("FAILED workers, not recoverable %s", c_worker.hostname)
                sys.exit(1)
            elif c_worker.is_active():
                worker_active.append(c_worker.hostname)
            else:
                worker_unknown.append(c_worker.hostname)
    return (
        worker_active,
        worker_unknown,
        worker_error,
        worker_down,
        cluster_workers,
        cluster_data,
    )


def check_workers(rescale:bool, worker_count:int,config: Configuration):
    """
    Fetch cluster data from server and check
        - all workers are active
        - remove broken workers
    :param rescale: rescale decision
    :param worker_count: expected number of workers
    :return: no_error, cluster_data
    """
    worker_ready = False
    start_time = time.time()
    max_time = 1200
    no_error = True
    max_wait_cnt = 0
    service_frequency = int(config.active_mode.service_frequency)
    while not worker_ready:
        (
            worker_active,
            worker_unknown,
            worker_error,
            worker_down,
            cluster_workers,
            cluster_data,
        ) = __worker_states()

        if cluster_data is None:
            logger.error(
                "unable to receive cluster data ... try again in %s seconds",
                service_frequency * WAIT_CLUSTER_SCALING,
            )
            time.sleep(service_frequency * WAIT_CLUSTER_SCALING)
            continue
        cluster_count_live = len(cluster_workers)
        logger.info(
            "workers: active %s, error %s, down %s, not ready %s, error list: %s.",
            len(worker_active),
            len(worker_error),
            len(worker_down),
            len(worker_unknown),
            worker_error,
        )
        logger.debug("workers expected %s", worker_count)
        elapsed_time = time.time() - start_time
        if cluster_count_live < worker_count and WAIT_CLUSTER_SCALING > max_wait_cnt:
            logger.debug(
                "increase wait time for cloud api %s, wait until %s workers are listed, current %s",
                max_wait_cnt,
                worker_count,
                cluster_count_live,
            )
            time.sleep(WAIT_CLUSTER_SCALING / 2)
            max_wait_cnt += 1
        elif elapsed_time > max_time and worker_unknown:
            logger.error("workers are stuck: %s", worker_unknown)
            cluster_scale_down_specific_self_check(
                worker_unknown + worker_error, rescale
            )
            worker_count -= len(worker_unknown + worker_error)
            no_error = False
            __csv_log_entry("E", len(worker_unknown + worker_error), "11")
        elif worker_error and not worker_unknown:
            logger.error("scale down error workers: %s", worker_error)
            cluster_scale_down_specific_self_check(worker_error, rescale)
            worker_count -= len(worker_error)
            no_error = False
            __csv_log_entry("E", len(worker_error), "12")
        elif elapsed_time > max_time and worker_down:
            logger.error("scale down workers are down: %s", worker_down)
            cluster_scale_down_specific_self_check(worker_down, rescale)
            worker_count -= len(worker_down)
            no_error = False
            __csv_log_entry("E", len(worker_down), "17")
        elif not worker_unknown and not worker_error:
            logger.info("ALL WORKERS ACTIVE!")
            if cluster_count_live < worker_count:
                logger.warning("higher number of workers expected!")
            return no_error, cluster_data
        else:
            logger.info(
                "at least one worker is not 'ACTIVE', wait ... %d seconds",
                WAIT_CLUSTER_SCALING,
            )
            time.sleep(WAIT_CLUSTER_SCALING)
