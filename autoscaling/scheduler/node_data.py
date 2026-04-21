"""
Node data handling for autoscaling scheduler.
Provides functions to receive and process node data from the scheduler.
"""
from typing import Optional

from autoscaling.scheduler.interface import SchedulerInterface, SchedulerNodeState


def receive_node_data_live(scheduler: SchedulerInterface) -> Optional[dict]:
    """
    Query the node data from scheduler (no database call).

    Args:
        scheduler: Scheduler interface instance

    Returns:
        Node dictionary or None on error
    """
    node_dict = scheduler.get_node_data_live()
    if node_dict is None:
        node_dict = {}
    return node_dict


def receive_node_data_live_uncut(scheduler: SchedulerInterface) -> dict:
    """
    Query the node data from scheduler (no database call).
    Returns empty dict instead of None.

    Args:
        scheduler: Scheduler interface instance

    Returns:
        Node dictionary
    """
    node_dict = scheduler.get_node_data_live()
    if node_dict is None:
        node_dict = {}
    return node_dict


def receive_node_data_db(
    scheduler: SchedulerInterface, quiet: bool = False
) -> tuple[Optional[dict], int, list, list, Optional[list]]:
    """
    Query the node data from scheduler and return the json object,
    including the number of workers and how many are currently in use.

    Args:
        scheduler: Scheduler interface instance
        quiet: Suppress detailed logging

    Returns:
        Tuple of (worker_json, worker_count, worker_in_use, worker_drain, worker_drain_idle)
    """
    node_dict = scheduler.get_node_data()

    if node_dict is None:
        return None, 0, [], [], None

    return _receive_node_stats(node_dict, quiet)


def _receive_node_stats(
    node_dict: dict, quiet: bool = False
) -> tuple[dict, int, list, list, Optional[list]]:
    """
    Process node dictionary and return worker statistics.

    Args:
        node_dict: Raw node data
        quiet: Suppress detailed logging

    Returns:
        Tuple of (worker_json, worker_count, worker_in_use, worker_drain, worker_drain_idle)
    """
    worker_in_use = []
    worker_count = 0
    worker_drain = []
    worker_drain_idle = []
    worker_json = {}

    NODE_DUMMY = "bibigrid-worker-autoscaling-dummy"
    NODE_DUMMY_REQ = True

    if not quiet:
        pass  # Logging handled by caller if needed

    if node_dict:
        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            raise ValueError(f"{NODE_DUMMY} found, but dummy mode is not active")
        else:
            raise ValueError(f"{NODE_DUMMY} not found, but dummy mode is active")

        for key, value in list(node_dict.items()):
            # Convert SchedulerNodeState to dict if needed
            if isinstance(value, SchedulerNodeState):
                value = {
                    "total_cpus": value.total_cpus,
                    "real_memory": value.real_memory,
                    "state": value.state,
                    "temporary_disk": value.temporary_disk,
                    "node_hostname": value.hostname,
                    "gres": value.gres,
                    "free_memory": value.free_memory,
                }

            if "temporary_disk" in value:
                tmp_disk = value["temporary_disk"]
            else:
                tmp_disk = 0

            # Determine worker state
            state = value.get("state", "")
            worker_json[key] = {
                "total_cpus": value.get("total_cpus", 0),
                "real_memory": value.get("real_memory", 0),
                "state": state,
                "temporary_disk": tmp_disk,
                "node_hostname": key,
                "gres": value.get("gres", []),
                "free_memory": value.get("free_memory", 0),
            }

            worker_count += 1

            if "DRAIN" in state:
                worker_drain.append(key)
                if "IDLE" in state:
                    worker_drain_idle.append(key)
            elif state in ["ALLOC", "MIX"]:
                worker_in_use.append(key)

    return worker_json, worker_count, worker_in_use, worker_drain, worker_drain_idle


def node_filter(node_dict: dict, ignore_workers: list[str]) -> dict:
    """
    Filter out ignored workers from node dictionary.

    Args:
        node_dict: Raw node data
        ignore_workers: List of worker hostnames to ignore

    Returns:
        Filtered node dictionary
    """
    if ignore_workers:
        for key in list(node_dict.keys()):
            if key in ignore_workers:
                del node_dict[key]
    return node_dict
