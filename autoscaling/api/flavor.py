"""
Flavor management and API utilities.
"""

import logging
from typing import Any, Optional

from autoscaling.utils.converter import convert_mib_to_gb, reduce_flavor_memory
from autoscaling.utils.helpers import FLAVOR_GPU_ONLY, FLAVOR_GPU_REMOVE

logger = logging.getLogger(__name__)


def get_usable_flavors(
    flavor_data: list[dict[str, Any]], quiet: bool = True, cut: bool = True
) -> Optional[list[dict[str, Any]]]:
    """
    Get usable flavors based on configuration.

    Args:
        flavor_data: Raw flavor data from API
        quiet: Suppress logging
        cut: Apply flavor restriction

    Returns:
        List of usable flavors, or None on error
    """
    from autoscaling.config.loader import load_config
    from autoscaling.utils.helpers import get_autoscaling_folder

    # Load config for settings
    config_path = get_autoscaling_folder() + "autoscaling_config.yaml"
    try:
        load_config(config_path)
    except Exception:
        # Use defaults if config fails
        pass

    # Apply GPU filter
    flavor_data = flavor_mod_gpu(flavor_data)

    # Filter by allowed names if configured
    flavor_data = filter_flavor_by_allowed_names(flavor_data)

    # Apply usable flavor calculation
    if cut:
        flavor_data = usable_flavor_data(flavor_data, quiet)

    return flavor_data


def usable_flavor_data(
    flavor_data: list[dict[str, Any]], quiet: bool = True
) -> list[dict[str, Any]]:
    """
    Calculate the number of usable flavors based on configuration.

    Args:
        flavor_data: List of flavor data
        quiet: Suppress logging

    Returns:
        List of usable flavors
    """
    from autoscaling.config.loader import load_config
    from autoscaling.utils.helpers import get_autoscaling_folder

    config_path = get_autoscaling_folder() + "autoscaling_config.yaml"
    try:
        config = load_config(config_path)
    except Exception:
        config = {}

    fv_cut = int(config.get("flavor_restriction", 0))

    flavors_available = len(flavor_data)

    if not quiet:
        logger.debug("flavors_available %d, fv_cut %d", flavors_available, fv_cut)

    if 0 < fv_cut < 1:
        fv_cut = int(flavors_available * fv_cut + 0.5)

    if 1 <= fv_cut < flavors_available:
        flavors_usable = fv_cut
    else:
        flavors_usable = flavors_available
        if fv_cut != 0:
            logger.error("Wrong flavor cut value")

    if not quiet:
        logger.debug("Found %d flavors, %d usable", flavors_available, flavors_usable)

    if flavors_usable == 0 and flavors_available > 0:
        flavors_usable = 1

    # Return top N flavors
    return flavor_data[:flavors_usable] if flavor_data else []


def flavor_mod_gpu(flavors_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Modify flavor data according to GPU option.

    Args:
        flavors_data: List of flavor data

    Returns:
        Modified flavor data
    """
    from autoscaling.config.loader import load_config
    from autoscaling.utils.helpers import get_autoscaling_folder

    config_path = get_autoscaling_folder() + "autoscaling_config.yaml"
    try:
        config = load_config(config_path)
    except Exception:
        config = {}

    flavor_gpu = int(config.get("flavor_gpu", 1))

    flavors_data_mod = []
    removed_flavors = []

    for fv_data in flavors_data:
        gpu_count = fv_data.get("flavor", {}).get("gpu", 0)

        # Use only GPU flavors
        if flavor_gpu == FLAVOR_GPU_ONLY and gpu_count == 0:
            removed_flavors.append(fv_data["flavor"]["name"])
        # Remove all GPU flavors
        elif flavor_gpu == FLAVOR_GPU_REMOVE and gpu_count != 0:
            removed_flavors.append(fv_data["flavor"]["name"])
        else:
            flavors_data_mod.append(fv_data)

    if removed_flavors:
        logger.debug("Unlisted flavors by GPU config: %s", removed_flavors)

    return flavors_data_mod


def filter_flavor_by_allowed_names(
    flavors_data: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    Filter flavors by allowed names from configuration.

    Args:
        flavors_data: List of flavor data

    Returns:
        Filtered flavor data
    """
    from autoscaling.config.loader import load_config
    from autoscaling.utils.helpers import get_autoscaling_folder

    config_path = get_autoscaling_folder() + "autoscaling_config.yaml"
    try:
        config = load_config(config_path)
    except Exception:
        config = {}

    allowed_names = config.get("allowed_flavor_names", [])

    if not allowed_names:
        return flavors_data

    filtered = [
        fv for fv in flavors_data if fv.get("flavor", {}).get("name") in allowed_names
    ]

    if len(filtered) < len(flavors_data):
        logger.debug("Filtered flavors to: %s", [f["flavor"]["name"] for f in filtered])

    return filtered


def get_flavor_by_name(
    flavor_data: list[dict[str, Any]], flavor_name: str
) -> Optional[dict[str, Any]]:
    """
    Get flavor by name.

    Args:
        flavor_data: List of flavor data
        flavor_name: Name of flavor to find

    Returns:
        Flavor data, or None if not found
    """
    for flavor in flavor_data:
        if flavor.get("flavor", {}).get("name") == flavor_name:
            return flavor
    return None


def get_flavor_available_count(
    flavor_data: list[dict[str, Any]], flavor_name: str
) -> int:
    """
    Get available count for a flavor.

    Args:
        flavor_data: List of flavor data
        flavor_name: Name of flavor

    Returns:
        Number of available workers with this flavor
    """
    flavor = get_flavor_by_name(flavor_data, flavor_name)
    if flavor and flavor.get("usable_count"):
        return int(flavor["usable_count"])
    return 0


def cluster_workers_to_flavor(
    flavors_data: list[dict[str, Any]], cluster_worker: dict[str, Any]
) -> Optional[dict[str, Any]]:
    """
    Match cluster worker to flavor data.

    Args:
        flavors_data: List of flavor data
        cluster_worker: Worker data from cluster API

    Returns:
        Matching flavor data, or None
    """
    worker_flavor = cluster_worker.get("flavor", {})
    worker_flavor_name = worker_flavor.get("name", "")

    for flavor in flavors_data:
        if flavor.get("flavor", {}).get("name") == worker_flavor_name:
            return flavor
    return None


def cluster_worker_to_credit(
    flavors_data: list[dict[str, Any]], cluster_worker: dict[str, Any]
) -> float:
    """
    Get credit cost per hour for a worker.

    Args:
        flavors_data: List of flavor data
        cluster_worker: Worker data from cluster API

    Returns:
        Credit cost per hour
    """
    fv_data = cluster_workers_to_flavor(flavors_data, cluster_worker)
    if fv_data:
        try:
            return float(fv_data["flavor"].get("credits_costs_per_hour", 0))
        except (KeyError, ValueError):
            logger.error("KeyError or ValueError in flavor credit data")
    return 0.0


def update_cluster_worker_data(
    cluster_workers: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    Update cluster worker data with derived fields.

    Args:
        cluster_workers: List of worker data

    Returns:
        Updated worker data
    """
    if not cluster_workers:
        return []

    for w_data in cluster_workers:
        flavor = w_data.get("flavor", {})
        w_data.update(
            {
                "memory_usable": reduce_flavor_memory(
                    convert_mib_to_gb(flavor.get("ram", 0))
                ),
                "temporary_disk": flavor.get("ephemeral", 0),
            }
        )

    return cluster_workers
