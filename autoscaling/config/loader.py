"""
Configuration loading and management.
"""

import logging
import os
from typing import Any, Optional

import yaml

from autoscaling.utils.helpers import get_autoscaling_folder

logger = logging.getLogger(__name__)

# Configuration file paths
FILE_CONFIG = "autoscaling_config.yaml"

# Default configuration values
DEFAULT_CONFIG = {
    "scaling": {
        "portal_scaling_link": "https://simplevm.denbi.de/portal/api/autoscaling",
        "portal_webapp_link": "https://simplevm.denbi.de/portal/webapp/#/clusters/overview",
        "scaling_script_url": "https://raw.githubusercontent.com/deNBI/user_scripts/refs/heads/master/bibigrid/scaling.py",
        "scheduler": "slurm",
        "active_mode": "basic",
        "automatic_update": True,
        "database_reset": False,
        "pattern_id": "",
        "history_recall": 7,
        "ignore_workers": [],
        "pending_jobs_percent": 1.0,
        "resource_sorting": False,
    }
}


class ConfigError(Exception):
    """Configuration error exception."""


def load_config(config_path: Optional[str] = None) -> dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config file. If None, uses default location.

    Returns:
        Configuration dictionary

    Raises:
        ConfigError: If configuration cannot be loaded or is invalid.
    """
    if config_path is None:
        autoscaling_folder = get_autoscaling_folder()
        config_path = os.path.join(autoscaling_folder, FILE_CONFIG)

    logger.info("Loading configuration from %s", config_path)

    # Try to load existing config
    yaml_config = _read_yaml_file(config_path)

    if yaml_config is None:
        # Config file doesn't exist, create default
        logger.warning("Config file not found, using defaults")
        yaml_config = DEFAULT_CONFIG

    # Validate and return scaling section
    if "scaling" not in yaml_config:
        raise ConfigError("Configuration missing 'scaling' section")

    return yaml_config["scaling"]


def _read_yaml_file(file_path: str) -> Optional[dict[str, Any]]:
    """
    Read YAML data from file.

    Args:
        file_path: Path to YAML file

    Returns:
        Parsed YAML data or None on error
    """
    try:
        with open(file_path, "r", encoding="utf8") as stream:
            yaml_data = yaml.safe_load(stream)
        return yaml_data
    except yaml.YAMLError as exc:
        logger.error("YAML error reading %s: %s", file_path, exc)
        return None
    except OSError as exc:
        logger.error("OS error reading %s: %s", file_path, exc)
        return None


def write_config(config: dict[str, Any], config_path: str) -> bool:
    """
    Write configuration to YAML file.

    Args:
        config: Configuration dictionary
        config_path: Path to write config file

    Returns:
        True on success, False on error
    """
    try:
        with open(config_path, "w+", encoding="utf8") as target:
            yaml.dump(config, target, default_flow_style=False)
        return True
    except yaml.YAMLError as exc:
        logger.error("YAML error writing %s: %s", config_path, exc)
        return False
    except OSError as exc:
        logger.error("OS error writing %s: %s", config_path, exc)
        return False


def read_cluster_id() -> str:
    """
    Read cluster ID from hostname.

    Expected hostname format: bibigrid-master-clusterID

    Returns:
        Cluster ID string

    Raises:
        ConfigError: If hostname format is invalid
    """
    try:
        with open("/etc/hostname", "r", encoding="utf8") as file:
            hostname = file.read().rstrip()

        parts = hostname.split("-")
        if len(parts) < 3:
            raise ConfigError(
                f"Invalid hostname format: {hostname}. "
                "Expected format: bibigrid-master-clusterID"
            )

        cluster_id = parts[2]
        logger.debug("Read cluster ID: %s", cluster_id)
        return cluster_id

    except OSError as exc:
        logger.error("Error reading hostname: %s", exc)
        raise ConfigError(f"Failed to read hostname: {exc}")


def get_scaling_script_url(config: dict[str, Any]) -> str:
    """Get scaling script URL from config."""
    return config.get(
        "scaling_script_url", DEFAULT_CONFIG["scaling"]["scaling_script_url"]
    )


def get_portal_url_webapp(config: dict[str, Any]) -> str:
    """Get portal webapp URL from config."""
    return config.get(
        "portal_webapp_link", DEFAULT_CONFIG["scaling"]["portal_webapp_link"]
    )


def get_portal_url_scaling(config: dict[str, Any]) -> str:
    """Get portal scaling URL from config."""
    scaling_link = config.get(
        "portal_scaling_link", DEFAULT_CONFIG["scaling"]["portal_scaling_link"]
    )
    return scaling_link if scaling_link.endswith("/") else scaling_link + "/"


def get_url_scale_up(config: dict[str, Any], cluster_id: str) -> str:
    """Get scale-up API URL."""
    return get_portal_url_scaling(config) + cluster_id + "/scale-up/"


def get_url_scale_down_specific(config: dict[str, Any], cluster_id: str) -> str:
    """Get scale-down specific API URL."""
    return get_portal_url_scaling(config) + cluster_id + "/scale-down/specific/"


def get_url_info_cluster(config: dict[str, Any], cluster_id: str) -> str:
    """Get cluster info API URL."""
    return get_portal_url_scaling(config) + cluster_id + "/scale-data/"


def get_url_info_flavors(config: dict[str, Any], cluster_id: str) -> str:
    """Get flavors info API URL."""
    return get_portal_url_scaling(config) + cluster_id + "/usable_flavors/"
