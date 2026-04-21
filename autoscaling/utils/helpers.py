"""
Helper utilities for autoscaling.
"""
import hashlib
import json
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import yaml


def get_time() -> int:
    """
    Get current time in seconds (Unix timestamp).

    Returns:
        Current Unix timestamp
    """
    return int(time.time())


def get_datetime() -> datetime:
    """
    Get current UTC datetime.

    Returns:
        Current UTC datetime
    """
    return datetime.utcnow()


def timedelta(**kwargs) -> timedelta:
    """
    Create a timedelta.

    Args:
        **kwargs: Time delta arguments

    Returns:
        timedelta instance
    """
    return timedelta(**kwargs)


def generate_hash(file_path: str) -> str:
    """
    Generate MD5 hash of a file.

    Args:
        file_path: Path to the file

    Returns:
        MD5 hash as hex string
    """
    hash_md5 = hashlib.md5()

    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except OSError:
        return ""


def remove_suffix(input_string: str, suffix: str) -> str:
    """
    Remove suffix from string if present.

    Args:
        input_string: Input string
        suffix: Suffix to remove

    Returns:
        String without suffix
    """
    if suffix and input_string.endswith(suffix):
        return input_string[: -len(suffix)]
    return input_string


def normalize_flavor_name(flavor_name: str, suffix: str = " ephemeral") -> str:
    """
    Normalize flavor name by removing suffix.

    Args:
        flavor_name: Original flavor name
        suffix: Suffix to remove

    Returns:
        Normalized flavor name
    """
    return remove_suffix(flavor_name, suffix)


def fuzzy_match(s1: str, s2: str, threshold: float = 0.95) -> bool:
    """
    Check if two strings match above threshold.

    Args:
        s1: First string
        s2: Second string
        threshold: Matching threshold (0.0 to 1.0)

    Returns:
        True if strings match above threshold
    """
    if s1 == s2:
        return True

    if not s1 or not s2:
        return False

    longer = max(len(s1), len(s2))
    if longer == 0:
        return True

    matches = sum(1 for c1, c2 in zip(s1, s2) if c1 == c2)
    similarity = matches / longer

    return similarity >= threshold


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safe division that returns default on zero division.

    Args:
        numerator: Numerator
        denominator: Denominator
        default: Value to return on division by zero

    Returns:
        Result of division or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def safe_divide_up(numerator: float, denominator: float, default: int = 0) -> int:
    """
    Safe division with ceiling that returns default on zero division.

    Args:
        numerator: Numerator
        denominator: Denominator
        default: Value to return on division by zero

    Returns:
        Ceiling of division or default
    """
    if denominator == 0:
        return default
    return int((numerator + denominator - 1) // denominator)


def safe_divide_down(numerator: float, denominator: float, default: int = 0) -> int:
    """
    Safe division with floor that returns default on zero division.

    Args:
        numerator: Numerator
        denominator: Denominator
        default: Value to return on division by zero

    Returns:
        Floor of division or default
    """
    if denominator == 0:
        return default
    return int(numerator // denominator)


def format_memory(mb: int) -> str:
    """
    Format memory value in MB to human readable string.

    Args:
        mb: Memory in MB

    Returns:
        Human readable string (e.g., "2.5 GB")
    """
    if mb >= 1024 * 1024:
        return f"{mb / (1024 * 1024):.1f} TB"
    elif mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    else:
        return f"{mb} MB"


def format_time(seconds: int) -> str:
    """
    Format time value in seconds to human readable string.

    Args:
        seconds: Time in seconds

    Returns:
        Human readable string (e.g., "1h 30m 45s")
    """
    if seconds < 0:
        return "0s"

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")

    return " ".join(parts)


def parse_time(time_str: str) -> int:
    """
    Parse human readable time string to seconds.

    Args:
        time_str: Time string (e.g., "1h 30m 45s")

    Returns:
        Time in seconds
    """
    total_seconds = 0

    time_str = time_str.strip().lower()

    # Handle formats like "1-04:27:12" (days-hh:mm:ss)
    if "-" in time_str:
        parts = time_str.split("-")
        if len(parts) == 2:
            total_seconds += int(parts[0]) * 86400
            time_str = parts[1]

    # Parse hh:mm:ss or mm:ss
    parts = time_str.split(":")
    if len(parts) == 3:
        total_seconds += int(parts[0]) * 3600
        total_seconds += int(parts[1]) * 60
        total_seconds += int(parts[2])
    elif len(parts) == 2:
        total_seconds += int(parts[0]) * 60
        total_seconds += int(parts[1])
    elif len(parts) == 1:
        total_seconds += int(parts[0])

    return total_seconds


def read_json_file(file_path: str) -> Optional[dict]:
    """
    Read JSON content from file.

    Args:
        file_path: Path to the JSON file

    Returns:
        JSON content as dictionary or None on error
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError):
        return None


def save_json_file(file_path: str, content: dict) -> bool:
    """
    Save JSON content to file.

    Args:
        file_path: Path to the JSON file
        content: Content to save

    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        return True
    except IOError:
        return False


def read_yaml_file(file_path: str) -> Optional[dict]:
    """
    Read YAML data from file.

    Args:
        file_path: Path to the YAML file

    Returns:
        YAML content as dictionary or None on error
    """
    try:
        with open(file_path, "r", encoding="utf8") as stream:
            return yaml.safe_load(stream)
    except (yaml.YAMLError, OSError):
        return None


def write_yaml_file(file_path: str, data: dict) -> bool:
    """
    Write YAML data to file.

    Args:
        file_path: Path to the YAML file
        data: Data to write

    Returns:
        True if successful, False otherwise
    """
    try:
        with open(file_path, "w", encoding="utf8") as target:
            yaml.dump(data, target)
        return True
    except yaml.YAMLError:
        return False


def get_cluster_password(password_file: str = "cluster_pw.json") -> Optional[str]:
    """
    Read cluster password from file.

    Args:
        password_file: Path to password file

    Returns:
        Password string or None if not found
    """
    data = read_json_file(password_file)
    if data:
        return data.get("password")
    return None


def set_cluster_password(password: str, password_file: str = "cluster_pw.json") -> bool:
    """
    Save cluster password to file.

    Args:
        password: Password to save
        password_file: Path to password file

    Returns:
        True if successful, False otherwise
    """
    return save_json_file(password_file, {"password": password})
