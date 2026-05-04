"""
Flavor handling for autoscaling.
Provides flavor filtering, sorting, and matching functionality.
"""
from typing import Optional

# Constants
FLAVOR_GPU_ONLY = -1
FLAVOR_GPU_REMOVE = 1


def filter_flavor_by_allowed_names(flavors_data: list[dict], allowed_names: list[str]) -> list[dict]:
    """
    Filter flavors by allowed names.

    Args:
        flavors_data: List of flavor data
        allowed_names: List of allowed flavor names (empty = allow all)

    Returns:
        Filtered list of flavors
    """
    if not allowed_names:
        return flavors_data

    allowed_set = set(allowed_names)
    return [f for f in flavors_data if f.get("flavor", {}).get("name") in allowed_set]


def flavor_mod_gpu(flavors_data: list[dict], gpu_mode: int) -> list[dict]:
    """
    Modify flavor data according to GPU option.

    Args:
        flavors_data: List of flavor data
        gpu_mode: GPU filter mode
            - FLAVOR_GPU_ONLY (-1): use only GPU flavors
            - FLAVOR_GPU_REMOVE (1): remove all GPU flavors

    Returns:
        Modified list of flavors
    """
    flavors_data_mod = []
    removed_flavors = []

    for fv_data in flavors_data:
        gpu_count = fv_data.get("flavor", {}).get("gpu", 0)

        if gpu_mode == FLAVOR_GPU_ONLY and gpu_count == 0:
            removed_flavors.append(fv_data.get("flavor", {}).get("name"))
        elif gpu_mode == FLAVOR_GPU_REMOVE and gpu_count != 0:
            removed_flavors.append(fv_data.get("flavor", {}).get("name"))
        else:
            flavors_data_mod.append(fv_data)

    return flavors_data_mod


def get_flavor_by_name(flavors_data: list[dict], flavor_name: str) -> Optional[dict]:
    """
    Get flavor object by flavor name.

    Args:
        flavors_data: List of flavor data
        flavor_name: Name of the flavor to find

    Returns:
        Flavor data or None if not found
    """
    for flavor in flavors_data:
        if flavor.get("flavor", {}).get("name") == flavor_name:
            return flavor
    return None


def get_flavor_available_count(flavors_data: list[dict], flavor_name: str) -> int:
    """
    Return the number of available workers with this flavor.

    Args:
        flavors_data: List of flavor data
        flavor_name: Name of the flavor

    Returns:
        Number of available workers
    """
    flavor = get_flavor_by_name(flavors_data, flavor_name)
    if flavor:
        return int(flavor.get("usable_count", 0))
    return 0


def usable_flavor_data(flavors_data: list[dict], flavor_restriction: float) -> int:
    """
    Calculate the number of usable flavors based on configuration.

    Args:
        flavors_data: List of flavor data
        flavor_restriction: Flavor restriction value
            - 0 = all flavors usable
            - 0 < x < 1 = percentage of flavors
            - x >= 1 = absolute number of flavors

    Returns:
        Number of usable flavors
    """
    flavors_available = len(flavors_data)

    if 0 < flavor_restriction < 1:
        fv_cut = int(flavors_available * flavor_restriction)
    elif flavor_restriction >= 1:
        fv_cut = int(flavor_restriction)
    else:
        fv_cut = flavors_available

    if fv_cut < 1 and flavors_available > 0:
        fv_cut = 1

    return min(fv_cut, flavors_available)


def translate_metrics_to_flavor(
    cpu: int,
    mem: int,
    tmp_disk: int,
    flavors_data: list[dict],
    available_check: bool = False,
) -> Optional[dict]:
    """
    Select the flavor by CPU and memory requirements.

    Args:
        cpu: Required CPU value for job
        mem: Required memory value for job (MB)
        tmp_disk: Required ephemeral value (MB)
        flavors_data: Flavor data from cluster API
        available_check: Test if flavor is available

    Returns:
        Matched and available flavor or None
    """
    if not flavors_data:
        return None

    cpu = int(cpu)
    mem = int(mem)

    for fd in flavors_data:
        fv = fd.get("flavor", {})

        # Check CPU
        if fv.get("vcpus", 0) < cpu:
            continue

        # Check memory (GB to MB)
        if fv.get("ram_gib", 0) * 1024 < mem:
            continue

        # Check ephemeral disk
        fv_tmp_disk = fv.get("temporary_disk", fv.get("ephemeral_disk", 0))
        if fv_tmp_disk < tmp_disk:
            continue

        # Check availability if required
        if available_check:
            available_count = fv.get("usable_count", 0)
            if available_count <= 0:
                continue

        return fd

    return None


def sort_flavors_by_size(flavors_data: list[dict], reverse: bool = True) -> list[dict]:
    """
    Sort flavors by size (credits, ram, vcpus, ephemeral_disk).

    Args:
        flavors_data: List of flavor data
        reverse: Sort descending if True

    Returns:
        Sorted list of flavors
    """
    return sorted(
        flavors_data,
        key=lambda k: (
            k.get("flavor", {}).get("credits_costs_per_hour", 0),
            k.get("flavor", {}).get("ram_gib", 0),
            k.get("flavor", {}).get("vcpus", 0),
            k.get("flavor", {}).get("ephemeral_disk", 0),
        ),
        reverse=reverse,
    )
