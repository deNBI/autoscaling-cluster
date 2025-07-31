import logging
from typing import Any

# Assuming you have the dataclasses and Configuration class defined as before.
from classes import BaseMode, Configuration  # Import your dataclasses
from utils import read_config_yaml  # Import your utils

logger = logging.getLogger(__name__)


def __setting_overwrite(
    mode_setting: str = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Loads configuration from a YAML file, tests for available values,
    relies on BasicMode defaults, and handles a flattened YAML structure.
    """

    yaml_config_data = read_config_yaml()

    if yaml_config_data is None:
        logger.warning("No configuration file found, using BasicMode defaults.")
        yaml_config_data = {}

    # Apply active mode setting
    if mode_setting is None:
        mode_setting = yaml_config_data.get("active_mode", "basic")
        logger.warning(f"Mode missing, using default: {mode_setting}")

    # Create Configuration object
    configuration = Configuration(yaml_config_data)

    # Validate active mode
    if mode_setting not in configuration.mode_configs:
        raise ValueError(f"Unknown active mode: {mode_setting}")

    # Get active mode settings
    sconf = configuration.get_mode_config(mode_setting)

    # Optionally update playbook scheduler settings
    if "scheduler_settings" in sconf:
        __update_playbook_scheduler_config(sconf["scheduler_settings"])
    else:
        logger.debug(f"Mode {mode_setting} without scheduler parameters")

    logger.debug(pformat(sconf))
    return sconf, yaml_config_data
