import json
import sys
from pprint import pformat

from classes.configuration import Configuration
from classes.slurm_interface import SlurmInterface
from constants import (
    NODE_ALLOCATED,
    NODE_DOWN,
    NODE_DRAIN,
    NODE_DUMMY,
    NODE_DUMMY_REQ,
    NODE_IDLE,
    NODE_MIX,
)
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def receive_node_data_live(config: Configuration, scheduler_interface: SlurmInterface):
    """
    Query the node data from scheduler (no database call) and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    node_dict = receive_node_data_live_uncut(scheduler_interface=scheduler_interface)
    return __receive_node_stats(node_dict=node_dict, quiet=False, config=config)


def receive_node_data_live_uncut(scheduler_interface: SlurmInterface):
    """
    Query the node data from scheduler (no database call).
    :return:  node dictionary
    """
    node_dict = scheduler_interface.node_data_live()
    if node_dict is None:
        node_dict = {}
    return node_dict


def receive_node_data_db(
    quiet: bool, config: Configuration, scheduler_interface: SlurmInterface
):
    """
    Query the node data from scheduler and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    node_dict = scheduler_interface.fetch_scheduler_node_data()
    return __receive_node_stats(node_dict=node_dict, quiet=quiet, config=config)


def __receive_node_stats(node_dict, quiet: bool, config: Configuration):
    """
    Return stats from job data from scheduler and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    worker_in_use = []
    worker_count = 0
    worker_drain = []
    worker_drain_idle = []

    logger.debug("node dict: %s ", pformat(node_dict))
    node_filter(node_dict=node_dict, config=config)
    if node_dict:
        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            logger.error("%s found, but dummy mode is not active", NODE_DUMMY)
            sys.exit(1)
        else:
            logger.error("%s not found, but dummy mode is active", NODE_DUMMY)
        for key, value in list(node_dict.items()):
            if "temporary_disk" in value:
                tmp_disk = value["temporary_disk"]
            else:
                tmp_disk = 0
            if not quiet:
                logger.info(
                    "key: %s - state: %s cpus %s real_memory %s tmp_disk %s",
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                    tmp_disk,
                )
                # logger.debug(pformat(key))
                # logger.debug(pformat(value))
            if key is not None and "worker" in key:
                worker_count += 1
                if NODE_DRAIN in value["state"]:
                    worker_drain.append(key)
                if (NODE_ALLOCATED in value["state"]) or (NODE_MIX in value["state"]):
                    worker_in_use.append(key)
                elif NODE_DRAIN in value["state"] and NODE_IDLE in value["state"]:
                    worker_drain_idle.append(key)
                elif (NODE_DOWN in value["state"]) and NODE_IDLE not in value["state"]:
                    logger.error("workers are in DOWN state")
            else:
                del node_dict[key]
        if not quiet:
            logger.info(
                "nodes: I found %d worker - %d allocated, %d drain, drain+idle %s",
                worker_count,
                len(worker_in_use),
                len(worker_drain),
                worker_drain_idle,
            )
    else:
        if not quiet:
            logger.info("no nodes found, possible broken configuration!")
    return node_dict, worker_count, worker_in_use, worker_drain, worker_drain_idle


def node_filter(node_dict, config: Configuration):
    ignore_workers = config.ignore_workers
    if ignore_workers:
        for key in node_dict.copy():
            if key in ignore_workers:
                del node_dict[key]
        logger.debug("ignore_nodes: %s, node_dict %s", ignore_workers, node_dict.keys())
