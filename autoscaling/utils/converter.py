"""
Converter utilities for unit conversions.
"""


def convert_gb_to_mb(value: int) -> int:
    """
    Convert GB value to MB.

    Args:
        value: Value in GB

    Returns:
        Value in MB (using 1000 multiplier)
    """
    return int(value) * 1000


def convert_gb_to_mib(value: int) -> int:
    """
    Convert GB value to MiB.

    Args:
        value: Value in GB

    Returns:
        Value in MiB (using 1024 multiplier)
    """
    return int(value) * 1024


def convert_mib_to_gb(value: int) -> int:
    """
    Convert MiB value to GB.

    Args:
        value: Value in MiB

    Returns:
        Value in GB
    """
    return int(int(value) / 1024)


def convert_tb_to_mb(value: float) -> int:
    """
    Convert TB value to MB.

    Args:
        value: Value in TB

    Returns:
        Value in MB
    """
    return int(value * 1000000)


def convert_tb_to_mib(value: float) -> int:
    """
    Convert TB value to MiB.

    Args:
        value: Value in TB

    Returns:
        Value in MiB
    """
    return int(value) * 1000 * 1024


def reduce_flavor_memory(mem_gb: int) -> int:
    """
    Reduce raw memory in GB to usable value.

    Calculation according to BiBiGrid setup:
    - Memory reduces to OS requirements
    - For mem <= 16001 GB: reduce by 1/16, minimum 512 MB
    - For mem > 16001 GB: reduce by 1/16, maximum 4000 MB

    Args:
        mem_gb: Raw memory in GB

    Returns:
        Usable memory in MB
    """
    mem = convert_gb_to_mb(mem_gb)
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    sub = int(mem / 16)

    if mem <= mem_border:
        sub = max(sub, mem_min)
    else:
        sub = min(sub, mem_max)

    return int(mem - sub)


def convert_to_large_flavor_cpu(
    mem: int, cpu: int, disk: int, flavor_cpu: int, flavor_mem: int, flavor_disk: int
) -> bool:
    """
    Check if job requirements fit within flavor capabilities.

    Args:
        mem: Job memory requirement (MB)
        cpu: Job CPU requirement
        disk: Job disk requirement (MB)
        flavor_mem: Flavor memory (MB)
        flavor_cpu: Flavor CPU count
        flavor_disk: Flavor disk (MB)

    Returns:
        True if job fits in flavor
    """
    return (
        cpu <= flavor_cpu
        and mem <= flavor_mem
        and (disk < flavor_disk or (disk == 0 and flavor_disk >= 0))
    )
