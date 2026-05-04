"""
Filter utilities for autoscaling.
"""


def filter_by_flavor(workers: dict, flavor_name: str, flavor_key: str = "flavor") -> dict:
    """
    Filter workers by flavor.

    Args:
        workers: Dictionary of worker data
        flavor_name: Flavor name to filter by
        flavor_key: Key to access flavor in worker data

    Returns:
        Filtered dictionary of workers
    """
    result = {}
    for hostname, worker in workers.items():
        worker_flavor = worker.get(flavor_key, {})
        if worker_flavor.get("name") == flavor_name:
            result[hostname] = worker
    return result


def filter_by_state(workers: dict, state: str, include_drain: bool = True) -> dict:
    """
    Filter workers by state.

    Args:
        workers: Dictionary of worker data
        state: State to filter by (ALLOC, IDLE, MIX, DRAIN)
        include_drain: Whether to include draining workers

    Returns:
        Filtered dictionary of workers
    """
    result = {}
    for hostname, worker in workers.items():
        worker_state = worker.get("state", "")
        if include_drain:
            if state in worker_state:
                result[hostname] = worker
        else:
            if worker_state == state:
                result[hostname] = worker
    return result


def filter_idle_workers(workers: dict) -> dict:
    """
    Filter only idle workers.

    Args:
        workers: Dictionary of worker data

    Returns:
        Dictionary of idle workers
    """
    return filter_by_state(workers, "IDLE", include_drain=False)


def filter_active_workers(workers: dict) -> dict:
    """
    Filter active workers (ALLOC or MIX).

    Args:
        workers: Dictionary of worker data

    Returns:
        Dictionary of active workers
    """
    result = {}
    for hostname, worker in workers.items():
        state = worker.get("state", "")
        if state in ["ALLOC", "MIX"]:
            result[hostname] = worker
    return result


def filter_draining_workers(workers: dict) -> dict:
    """
    Filter draining workers.

    Args:
        workers: Dictionary of worker data

    Returns:
        Dictionary of draining workers
    """
    result = {}
    for hostname, worker in workers.items():
        state = worker.get("state", "")
        if "DRAIN" in state:
            result[hostname] = worker
    return result


def filter_by_name_pattern(workers: dict, pattern: str) -> dict:
    """
    Filter workers by hostname pattern.

    Args:
        workers: Dictionary of worker data
        pattern: Pattern to match in hostname

    Returns:
        Filtered dictionary of workers
    """
    result = {}
    for hostname, worker in workers.items():
        if pattern in hostname:
            result[hostname] = worker
    return result


def exclude_workers(workers: dict, excluded: list[str]) -> dict:
    """
    Exclude workers by hostname.

    Args:
        workers: Dictionary of worker data
        excluded: List of hostnames to exclude

    Returns:
        Filtered dictionary of workers
    """
    excluded_set = set(excluded)
    return {hostname: worker for hostname, worker in workers.items() if hostname not in excluded_set}


def get_worker_count_by_state(workers: dict) -> dict[str, int]:
    """
    Get worker count grouped by state.

    Args:
        workers: Dictionary of worker data

    Returns:
        Dictionary with state counts
    """
    counts = {
        "ALLOC": 0,
        "MIX": 0,
        "IDLE": 0,
        "DRAIN": 0,
        "DOWN": 0,
        "OTHER": 0,
    }

    for worker in workers.values():
        state = worker.get("state", "OTHER")
        if state in counts:
            counts[state] += 1
        else:
            counts["OTHER"] += 1

    return counts
