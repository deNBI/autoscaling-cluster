"""
Helper functions and utilities.
"""

import hashlib
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Constants from original code
AUTOSCALING_VERSION = "2.3.0"
SCALING_TYPE = "autoscaling"
REPO_LINK = "https://github.com/deNBI/autoscaling-cluster/"

# File paths
FILE_CONFIG = "autoscaling_config.yaml"
IDENTIFIER = "autoscaling"
DATABASE_FILE = IDENTIFIER + "_database.json"
CLUSTER_PASSWORD_FILE = "cluster_pw.json"
LOG_FILE = IDENTIFIER + ".log"
LOG_CSV = IDENTIFIER + ".csv"

# Node states
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_IDLE = "IDLE"
NODE_DRAIN = "DRAIN"
NODE_DOWN = "DOWN"
NODE_DUMMY = "bibigrid-worker-autoscaling-dummy"
NODE_DUMMY_REQ = True

# Worker states
WORKER_ACTIVE = "ACTIVE"
WORKER_ERROR = "ERROR"
WORKER_CREATION_FAILED = "CREATION_FAILED"
WORKER_FAILED = "FAILED"
WORKER_PORT_CLOSED = "PORT_CLOSED"
WORKER_PLANNED = "PLANNED"
WORKER_SCHEDULING = "SCHEDULING"

# Flavor options
FLAVOR_GPU_ONLY = -1
FLAVOR_GPU_REMOVE = 1

# Flavor depth options
DEPTH_MAX_WORKER = -2
DEPTH_MULTI_SINGLE = -3
DEPTH_MULTI = -1

# Job state IDs
JOB_FINISHED = 3
JOB_PENDING = 0
JOB_RUNNING = 1

# Scaling constants
DOWNSCALE_LIMIT = 0
WAIT_CLUSTER_SCALING = 10
FLAVOR_HIGH_MEM = "hmf"
DATA_LONG_TIME = 7
DATABASE_WORKER_PATTERN = " + ephemeral"

# HTTP codes
HTTP_CODE_OK = 200
HTTP_CODE_UNAUTHORIZED = 401
HTTP_CODE_OUTDATED = 400

# Normalization
NORM_HIGH = None
NORM_LOW = 0.0001
FORCE_LOW = 0.0001

# Request timeout
REQUEST_TIMEOUT = 60

# Directories
HOME = str(Path.home())
PLAYBOOK_DIR = HOME + "/playbook"
PLAYBOOK_VARS_DIR = HOME + "/playbook/vars"
COMMON_CONFIGURATION_YML = PLAYBOOK_VARS_DIR + "/common_configuration.yml"
ANSIBLE_HOSTS_FILE = PLAYBOOK_DIR + "/ansible_hosts"
INSTANCES_YML = PLAYBOOK_VARS_DIR + "/instances.yml"
SCHEDULER_YML = PLAYBOOK_VARS_DIR + "/scheduler_config.yml"


def get_autoscaling_folder() -> str:
    """
    Get the autoscaling folder path.

    Returns:
        Path to autoscaling folder
    """
    return os.path.dirname(os.path.realpath(__file__)) + "/../"


def get_scaling_script_file() -> str:
    """
    Get the scaling script file path.

    Returns:
        Path to scaling.py
    """
    return get_autoscaling_folder() + "scaling.py"


def generate_hash(file_path: str) -> str:
    """
    Generate a sha256 hash from a file.

    Args:
        file_path: Path to file

    Returns:
        Hex digest of SHA256 hash, or None if file doesn't exist
    """
    if os.path.isfile(file_path):
        hash_v = hashlib.sha256()
        with open(file_path, "rb") as file:
            while True:
                chunk = file.read(hash_v.block_size)
                if not chunk:
                    break
                hash_v.update(chunk)

        logger.debug("Generated hash for %s: %s", file_path, hash_v.hexdigest())
        return hash_v.hexdigest()
    return None


def get_cluster_password(password_file: str = None) -> str:
    """
    Read cluster password from JSON file.

    Args:
        password_file: Path to password file. Defaults to CLUSTER_PASSWORD_FILE.

    Returns:
        Password string

    Raises:
        SystemExit: If password file is missing or invalid
    """
    import json

    if password_file is None:
        password_file = get_autoscaling_folder() + CLUSTER_PASSWORD_FILE

    try:
        with open(password_file, "r", encoding="utf8") as f:
            pw_json = json.load(f)
        cluster_pw = pw_json.get("password")
        if not cluster_pw:
            logger.error("No cluster password found in %s", password_file)
            sys.exit(1)
        logger.debug("Loaded password from %s", password_file)
        return cluster_pw
    except (IOError, json.JSONDecodeError) as exc:
        logger.error("Error reading password file %s: %s", password_file, exc)
        sys.exit(1)


def set_cluster_password(password: str, password_file: str = None) -> bool:
    """
    Save cluster password to JSON file.

    Args:
        password: Password to save
        password_file: Path to password file. Defaults to CLUSTER_PASSWORD_FILE.

    Returns:
        True on success
    """
    import json

    if password_file is None:
        password_file = get_autoscaling_folder() + CLUSTER_PASSWORD_FILE

    try:
        with open(password_file, "w", encoding="utf8") as f:
            json.dump({"password": password}, f, ensure_ascii=False, indent=4)
        logger.info("Saved password to %s", password_file)
        return True
    except IOError as exc:
        logger.error("Error writing password file %s: %s", password_file, exc)
        return False


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


def normalize_flavor_name(fv_name: str, pattern: str = DATABASE_WORKER_PATTERN) -> str:
    """
    Normalize flavor name by removing pattern.

    Args:
        fv_name: Original flavor name
        pattern: Pattern to remove

    Returns:
        Normalized flavor name
    """
    return remove_suffix(fv_name, pattern)
