"""
Scale actions for autoscaling.
Provides functions to execute scale up and scale down operations.
"""
import time
from typing import Optional

from autoscaling.cluster.api import ClusterAPI
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.scheduler.node_data import receive_node_data_db
from autoscaling.scheduler.job_data import sort_jobs
from autoscaling.core.state import ScaleState, Rescale

# Constants
WAIT_CLUSTER_SCALING = 10
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_DRAIN = "DRAIN"


def cluster_scale_up(
    scheduler: SchedulerInterface,
    cluster_api: ClusterAPI,
    jobs_pending_dict: dict,
    jobs_running_dict: dict,
    worker_count: int,
    worker_json: dict,
    worker_drain: list[str],
    state: ScaleState,
    flavor_data: list[dict],
    cluster_worker: list[dict],
    config_mode: dict,
) -> bool:
    """
    Scale up workers and rescale cluster.

    Args:
        scheduler: Scheduler interface
        cluster_api: Cluster API client
        jobs_pending_dict: Pending jobs dictionary
        jobs_running_dict: Running jobs dictionary
        worker_count: Total worker count
        worker_json: Worker data
        worker_drain: Draining workers list
        state: Current scaling state
        flavor_data: Available flavors
        cluster_worker: Cluster worker data
        config_mode: Configuration mode settings

    Returns:
        True if scaling was successful
    """
    from autoscaling.core.scaling_engine import ScalingEngine
    from autoscaling.utils.helpers import get_time

    job_priority = sort_jobs(jobs_pending_dict, config_mode)

    flavor_depth_list = classify_jobs_to_flavors(job_priority, flavor_data)
    if not flavor_depth_list:
        return False

    flavor_depth = int(config_mode.get("flavor_depth", -1))
    DEPTH_MULTI_SINGLE = -3
    WAIT_CLUSTER_SCALING = 10

    upscale_cnt = 0
    flavor_index_cnt = 0
    level_sup = False
    flavors_started_cnt = 0
    worker_memory_usage = _get_worker_memory_usage(worker_json)
    worker_claimed = {}
    skip_starts = False

    while len(flavor_depth_list) > flavor_index_cnt:
        if flavor_index_cnt >= len(flavor_depth_list):
            break

        jobs_pending_flavor, flavor_next, flavor_job_list = flavor_depth_list[
            flavor_index_cnt
        ]

        if not flavor_next:
            flavor_index_cnt += 1
            continue

        if flavors_started_cnt > 0 and flavor_depth == DEPTH_MULTI_SINGLE:
            skip_starts = True

        if level_sup and not skip_starts:
            time.sleep(WAIT_CLUSTER_SCALING)
            flavor_data = get_usable_flavors(True, True)
            level_sup = False

        # Calculate scale up data
        data_tmp = __calculate_scale_up_data(
            flavor_job_list,
            jobs_pending_flavor,
            worker_count,
            worker_json,
            worker_drain,
            state,
            flavor_data,
            flavor_next,
            flavor_index_cnt,
            worker_memory_usage,
            jobs_running_dict,
            cluster_worker,
            flavors_started_cnt,
            len(flavor_depth_list) - flavor_index_cnt - 1,
            worker_claimed,
            config_mode,
        )

        if data_tmp:
            if not skip_starts:
                result = cluster_api.scale_up(
                    data_tmp["worker_flavor_name"],
                    data_tmp["upscale_count"],
                )
                if result:
                    worker_memory_usage = data_tmp.get("worker_memory_usage", 0)
                    upscale_cnt += data_tmp["upscale_count"]
                    worker_count += data_tmp["upscale_count"]
                    flavors_started_cnt += 1
                    level_sup = True
                else:
                    return False

        flavor_index_cnt += 1

    if upscale_cnt > 0:
        rescale_cluster(scheduler, cluster_api, worker_count)
        return True

    return False


def cluster_scale_down_specific_hostnames(
    hostnames: str, rescale: Rescale
) -> bool:
    """
    Scale down with specific hostnames.

    Args:
        hostnames: Comma-separated list of hostnames
        rescale: Rescale type

    Returns:
        True if successful
    """
    worker_hostnames = "[" + hostnames + "]"
    return cluster_scale_down_specific_hostnames_list(worker_hostnames, rescale)


def cluster_scale_down_specific_self_check(
    worker_hostnames: list[str], rescale: Rescale
) -> bool:
    """
    Scale down with specific hostnames.
    Includes a final worker check with most up-to-date worker data.

    Args:
        worker_hostnames: List of hostnames
        rescale: Rescale type

    Returns:
        True if successful
    """
    from autoscaling.cluster.api import get_cluster_workers

    if not worker_hostnames:
        return False

    time.sleep(WAIT_CLUSTER_SCALING)
    worker_json, _, _, _, worker_drain_idle = receive_node_data_db(False)

    scale_down_list = []

    for wb in worker_hostnames:
        if wb in worker_json:
            if wb not in worker_drain_idle:
                if (
                    NODE_ALLOCATED in worker_json[wb]["state"]
                    or NODE_MIX in worker_json[wb]["state"]
                ):
                    logger.error(f"Worker {wb} still allocated, skip scale down")
                    return False
            scale_down_list.append(wb)

    if scale_down_list:
        return cluster_scale_down_specific_hostnames_list(
            scale_down_list, rescale
        )

    return False


def cluster_scale_down_specific_hostnames_list(
    worker_hostnames: list[str], rescale: Rescale
) -> bool:
    """
    Scale down specific workers by hostname list.

    Args:
        worker_hostnames: List of worker hostnames
        rescale: Rescale type

    Returns:
        True if successful
    """
    from autoscaling.cluster.api import ClusterAPI

    # Import cluster API
    from autoscaling.cluster.api import ClusterAPI

    # Create cluster API client
    cluster_api = ClusterAPI()

    result = cluster_api.scale_down(worker_hostnames)

    if result and rescale == Rescale.CHECK:
        time.sleep(WAIT_CLUSTER_SCALING)
        rescale_cluster()

    return result is not None


def cluster_scale_down_specific(
    worker_json: dict,
    worker_num: int,
    rescale: Rescale,
    jobs_dict: Optional[dict],
) -> bool:
    """
    Scale down a specific number of workers.

    Args:
        worker_json: Worker data dictionary
        worker_num: Number of workers to delete
        rescale: Rescale type
        jobs_dict: Jobs dictionary (optional)

    Returns:
        True if successful
    """
    worker_list = __generate_downscale_list(worker_json, worker_num, jobs_dict)
    return cluster_scale_down_specific_self_check(worker_list, rescale)


def __cluster_scale_down_complete() -> bool:
    """
    Scale down all scheduler workers.

    Returns:
        True if successful
    """
    worker_json, worker_count, _, _, _ = receive_node_data_db(False)
    return cluster_scale_down_specific(worker_json, worker_count, Rescale.CHECK, None)


def __cluster_shut_down() -> bool:
    """
    Scale down all workers by cluster data.

    Returns:
        True if successful
    """
    from autoscaling.cluster.api import get_cluster_workers_from_api

    cluster_workers = get_cluster_workers_from_api()
    worker_list = []

    if cluster_workers is not None:
        for cl in cluster_workers:
            if "worker" in cl["hostname"]:
                worker_list.append(cl["hostname"])

    if worker_list:
        time.sleep(WAIT_CLUSTER_SCALING)
        return cluster_scale_down_specific_hostnames_list(
            worker_list, Rescale.CHECK
        )

    return False


def rescale_cluster(scheduler, cluster_api, worker_count: int = 0) -> bool:
    """
    Apply new worker configuration.
    Executes scaling, modifies and runs ansible playbook.

    Args:
        scheduler: Scheduler interface
        cluster_api: Cluster API client
        worker_count: Expected worker count (0 = default delay)

    Returns:
        True if successful
    """
    from autoscaling.cloud.ansible import AnsibleRunner

    if worker_count == 0:
        time.sleep(WAIT_CLUSTER_SCALING)

    ansible_runner = AnsibleRunner()
    rescale_success = ansible_runner.run()

    return rescale_success


def _get_worker_memory_usage(worker_json: dict) -> int:
    """
    Get total memory usage from workers.

    Args:
        worker_json: Worker data dictionary

    Returns:
        Total memory usage in MB
    """
    total_memory = 0
    for worker in worker_json.values():
        total_memory += worker.get("real_memory", 0)
    return total_memory


def _generate_downscale_list(
    worker_json: dict, worker_num: int, jobs_dict: Optional[dict]
) -> list[str]:
    """
    Generate list of workers to scale down.

    Args:
        worker_json: Worker data dictionary
        worker_num: Number of workers to delete
        jobs_dict: Jobs dictionary

    Returns:
        List of worker hostnames to delete
    """
    worker_list = []

    # Get idle workers
    idle_workers = [
        key for key, value in worker_json.items()
        if value.get("state") == "IDLE" and "worker" in key
    ]

    # Get workers in use with drain state
    drain_workers = [
        key for key, value in worker_json.items()
        if "DRAIN" in value.get("state", "") and "worker" in key
    ]

    # Combine and limit
    all_candidates = idle_workers + drain_workers
    worker_list = all_candidates[:worker_num]

    return worker_list


def classify_jobs_to_flavors(
    job_priority: list, flavor_data: list[dict]
) -> list:
    """
    Classify pending jobs into flavor groups.

    Args:
        job_priority: Priority-sorted job list
        flavor_data: Available flavors

    Returns:
        List of (count, flavor, jobs) tuples
    """
    from autoscaling.cluster.flavors import translate_metrics_to_flavor

    flavor_depth_list = []

    for job_key, job_value in job_priority:
        if isinstance(job_value, dict):
            cpu = job_value.get("req_cpus", 1)
            mem = job_value.get("req_mem", 0)
            tmp_disk = job_value.get("temporary_disk", 0)
        else:
            cpu = job_value.req_cpus
            mem = job_value.req_mem
            tmp_disk = job_value.temporary_disk

        flavor_next = translate_metrics_to_flavor(
            cpu, mem, tmp_disk, flavor_data, available_check=True
        )

        if flavor_next:
            flavor_name = flavor_next.get("flavor", {}).get("name")
            if not flavor_name:
                continue

            # Find or create flavor group
            found = False
            for i, (count, flavor, jobs) in enumerate(flavor_depth_list):
                if flavor.get("flavor", {}).get("name") == flavor_name:
                    flavor_depth_list[i] = (count + 1, flavor, jobs + [job_key])
                    found = True
                    break

            if not found:
                flavor_depth_list.append((1, flavor_next, [job_key]))

    return flavor_depth_list


def __calculate_scale_up_data(
    flavor_job_list: list,
    jobs_pending_flavor: int,
    worker_count: int,
    worker_json: dict,
    worker_drain: list[str],
    state: ScaleState,
    flavor_data: list[dict],
    flavor_next: dict,
    level: int,
    worker_memory_usage: int,
    jobs_running_dict: dict,
    cluster_worker: list[dict],
    flavors_started_cnt: int,
    level_pending: int,
    worker_claimed: dict,
    config_mode: dict,
) -> Optional[dict]:
    """
    Calculate scale up data for a specific flavor.

    Args:
        flavor_job_list: Job list for this flavor
        jobs_pending_flavor: Number of pending jobs for this flavor
        worker_count: Total worker count
        worker_json: Worker data
        worker_drain: Draining workers
        state: Current scaling state
        flavor_data: Available flavors
        flavor_next: Selected flavor
        level: Current flavor level
        worker_memory_usage: Current memory usage
        jobs_running_dict: Running jobs
        cluster_worker: Cluster worker data
        flavors_started_cnt: Flavors started in previous levels
        level_pending: Number of pending levels
        worker_claimed: Claimed workers
        config_mode: Configuration mode

    Returns:
        Scale up data dictionary or None
    """
    # Placeholder for complex scaling calculation
    # This would be the full implementation from the legacy code

    current_force = float(config_mode.get("scale_force", 0.6))
    upscale_limit = max(int(jobs_pending_flavor * current_force), 1)

    # Apply limits
    limit_worker_starts = int(config_mode.get("limit_worker_starts", 0))
    if limit_worker_starts > 0:
        upscale_limit = min(upscale_limit, limit_worker_starts)

    limit_workers = int(config_mode.get("limit_workers", 0))
    if limit_workers > 0:
        upscale_limit = min(upscale_limit, limit_workers - worker_count)

    if upscale_limit <= 0:
        return None

    return {
        "password": None,  # Will be set by ClusterAPI
        "worker_flavor_name": flavor_next.get("flavor", {}).get("name"),
        "upscale_count": upscale_limit,
        "worker_memory_usage": worker_memory_usage,
    }
