"""
Configuration loader for autoscaling.
Reads and merges configuration from environment variables, YAML files and defaults.
"""
import os
import re
from pathlib import Path
from typing import Any, Optional

import yaml

from autoscaling.config.defaults import MODE_DEFAULTS, DEFAULTS, GlobalDefaults, ModeDefaults


def _normalize_env_var_name(name: str) -> str:
    """
    Normalize a configuration key to environment variable format.

    Converts "portal_scaling_link" to "AUTOSCALING_PORTAL_SCALING_LINK"
    and "active_mode" to "AUTOSCALING_ACTIVE_MODE"
    Also handles nested keys: "mode.basic.service_frequency" -> "AUTOSCALING_MODE_BASIC_SERVICE_FREQUENCY"
    """
    return "AUTOSCALING_" + name.upper().replace(".", "_")


def _get_env_var_value(key: str, var_type: type = str) -> Any:
    """
    Get a configuration value from environment variable.

    Args:
        key: Configuration key (e.g., "portal_scaling_link")
        var_type: Expected type (str, int, float, bool)

    Returns:
        Value converted to the expected type, or None if not set
    """
    env_name = _normalize_env_var_name(key)
    env_value = os.environ.get(env_name)

    if env_value is None:
        return None

    try:
        if var_type == int:
            return int(env_value)
        elif var_type == float:
            return float(env_value)
        elif var_type == bool:
            return env_value.lower() in ("true", "1", "yes")
        elif var_type == list:
            # Handle comma-separated lists
            return [item.strip() for item in env_value.split(",") if item.strip()]
        else:
            return env_value
    except (ValueError, AttributeError):
        return None


class ConfigError(Exception):
    """Configuration loading error."""
    pass


class ConfigLoader:
    """
    Load and manage autoscaling configuration.

    Configuration sources (in priority order):
    1. Environment variables
    2. YAML configuration file
    3. Default values
    """

    def __init__(self, config_file: str = "autoscaling_config.yaml"):
        """
        Initialize the config loader.

        Args:
            config_file: Path to the YAML configuration file
        """
        self.config_file = config_file
        self._raw_config: Optional[dict] = None
        self._merged_config: Optional[dict] = None

    def load(self) -> dict[str, Any]:
        """
        Load and merge all configuration sources.

        Returns:
            Merged configuration dictionary

        Raises:
            ConfigError: If configuration cannot be loaded
        """
        # Load from YAML file
        yaml_config = self._load_yaml()

        # Merge with defaults
        self._merged_config = self._merge_with_defaults(yaml_config)

        return self._merged_config

    def _load_yaml(self) -> dict[str, Any]:
        """
        Load configuration from YAML file.

        Returns:
            Raw YAML configuration, or empty dict if file not found
        """
        if not os.path.exists(self.config_file):
            return {}

        try:
            with open(self.config_file, "r", encoding="utf8") as stream:
                yaml_data = yaml.safe_load(stream)
            return yaml_data if yaml_data else {}
        except yaml.YAMLError as exc:
            raise ConfigError(f"Error parsing YAML config: {exc}")
        except OSError as exc:
            raise ConfigError(f"Error reading config file: {exc}")

    def _merge_with_defaults(self, user_config: dict[str, Any]) -> dict[str, Any]:
        """
        Merge user configuration with defaults.

        Args:
            user_config: User-provided configuration

        Returns:
            Merged configuration with defaults applied
        """
        result = {}

        # Merge global settings
        global_config = user_config.get("scaling", {})
        result["scaling"] = self._merge_global_settings(global_config)

        # Merge mode settings
        result["scaling"]["mode"] = self._merge_mode_settings(
            global_config.get("mode", {})
        )

        return result

    def _merge_global_settings(self, user_settings: dict[str, Any]) -> dict[str, Any]:
        """
        Merge global settings with defaults.

        Args:
            user_settings: User-provided global settings

        Returns:
            Merged global settings
        """
        result = {}

        # Get all attributes from GlobalDefaults
        for attr in dir(DEFAULTS):
            if attr.startswith("_"):
                continue

            default_value = getattr(DEFAULTS, attr)
            user_value = user_settings.get(attr)

            # Priority: environment variable > user config > default
            env_value = _get_env_var_value(attr, type(default_value))

            if env_value is not None:
                result[attr] = env_value
            elif user_value is not None:
                result[attr] = user_value
            else:
                result[attr] = default_value

        return result

    def _merge_mode_settings(self, user_modes: dict[str, Any]) -> dict[str, Any]:
        """
        Merge mode-specific settings with defaults.

        Args:
            user_modes: User-provided mode settings

        Returns:
            Merged mode settings
        """
        result = {}

        for mode_name, mode_defaults in MODE_DEFAULTS.items():
            user_mode = user_modes.get(mode_name, {})
            merged_mode = self._merge_mode(mode_defaults, user_mode, mode_name)
            merged_mode["info"] = user_mode.get("info", mode_defaults.info)
            result[mode_name] = merged_mode

        return result

    def _merge_mode(self, defaults: ModeDefaults, user_mode: dict[str, Any], mode_name: str = "") -> dict[str, Any]:
        """
        Merge a single mode's settings.

        Args:
            defaults: Default values for the mode
            user_mode: User-provided values
            mode_name: Name of the mode (used for env var key construction)

        Returns:
            Merged mode settings
        """
        result = {}

        # Get all attributes from ModeDefaults
        for attr in dir(defaults):
            if attr.startswith("_"):
                continue

            default_value = getattr(defaults, attr)
            user_value = user_mode.get(attr)
            # Construct the full key path for mode-specific env vars
            # e.g., "mode.basic.service_frequency" -> "AUTOSCALING_MODE_BASIC_SERVICE_FREQUENCY"
            env_key = f"mode.{mode_name}.{attr}" if mode_name else attr
            env_value = _get_env_var_value(env_key, type(default_value))

            # Priority: environment variable > user config > default
            if env_value is not None:
                result[attr] = env_value
            elif user_value is not None:
                result[attr] = user_value
            else:
                result[attr] = default_value

        return result

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            key: Configuration key (dot-separated for nested keys)
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if self._merged_config is None:
            return default

        keys = key.split(".")
        value = self._merged_config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_mode(self, mode_name: str) -> dict[str, Any]:
        """
        Get settings for a specific mode.

        Args:
            mode_name: Name of the mode

        Returns:
            Mode settings dictionary
        """
        modes = self._merged_config.get("scaling", {}).get("mode", {})
        return modes.get(mode_name, {})

    @property
    def raw_config(self) -> Optional[dict]:
        """Get the raw loaded YAML config."""
        return self._raw_config

    @property
    def merged_config(self) -> Optional[dict]:
        """Get the merged configuration."""
        return self._merged_config


def load_config(config_file: str = "autoscaling_config.yaml") -> dict[str, Any]:
    """
    Convenience function to load configuration.

    Args:
        config_file: Path to the YAML configuration file

    Returns:
        Merged configuration dictionary

    Raises:
        ConfigError: If configuration cannot be loaded
    """
    loader = ConfigLoader(config_file)
    return loader.load()
