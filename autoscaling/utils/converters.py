"""
Unit conversion utilities for autoscaling.
"""
from typing import Union


def convert_gb_to_mb(value: Union[int, float]) -> int:
    """
    Convert gigabytes to megabytes.

    Args:
        value: Value in GB

    Returns:
        Value in MB
    """
    return int(float(value) * 1024)


def convert_gb_to_mib(value: Union[int, float]) -> int:
    """
    Convert gigabytes to mebibytes (binary units).

    Args:
        value: Value in GB

    Returns:
        Value in MiB
    """
    return int(float(value) * 1024)


def convert_mib_to_gb(value: Union[int, float]) -> float:
    """
    Convert mebibytes to gigabytes.

    Args:
        value: Value in MiB

    Returns:
        Value in GB
    """
    return float(value) / 1024


def convert_tb_to_mb(value: Union[int, float]) -> int:
    """
    Convert terabytes to megabytes.

    Args:
        value: Value in TB

    Returns:
        Value in MB
    """
    return int(float(value) * 1024 * 1024)


def convert_tb_to_mib(value: Union[int, float]) -> int:
    """
    Convert terabytes to mebibytes (binary units).

    Args:
        value: Value in TB

    Returns:
        Value in MiB
    """
    return int(float(value) * 1024 * 1024)


def convert_memory(value: Union[int, float], from_unit: str, to_unit: str) -> float:
    """
    Convert memory between units.

    Args:
        value: Value to convert
        from_unit: Source unit (GB, MB, MiB, TB)
        to_unit: Target unit (GB, MB, MiB, TB)

    Returns:
        Converted value
    """
    # Convert to MB first
    unit_to_mb = {
        "GB": 1024,
        "MB": 1,
        "MiB": 1,
        "TB": 1024 * 1024,
    }

    mb_value = float(value) * unit_to_mb.get(from_unit.upper(), 1)

    # Convert from MB to target unit
    unit_from_mb = {
        "GB": 1 / 1024,
        "MB": 1,
        "MiB": 1,
        "TB": 1 / (1024 * 1024),
    }

    return mb_value * unit_from_mb.get(to_unit.upper(), 1)


def convert_time(value: Union[int, float], from_unit: str, to_unit: str) -> float:
    """
    Convert time between units.

    Args:
        value: Value to convert
        from_unit: Source unit (s, m, h, d)
        to_unit: Target unit (s, m, h, d)

    Returns:
        Converted value
    """
    # Convert to seconds first
    unit_to_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
    }

    seconds_value = float(value) * unit_to_seconds.get(from_unit.lower(), 1)

    # Convert from seconds to target unit
    unit_from_seconds = {
        "s": 1,
        "m": 1 / 60,
        "h": 1 / 3600,
        "d": 1 / 86400,
    }

    return seconds_value * unit_from_seconds.get(to_unit.lower(), 1)
