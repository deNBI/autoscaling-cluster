"""
Configuration loader for autoscaling.
Reads and merges configuration from YAML files and defaults.
"""
import os
from pathlib import Path
from typing import Any, Optional

import yaml

from autoscaling.config.defaults import MODE_DEFAULTS, DEFAULTS, GlobalDefaults, ModeDefaults


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

            # Use user value if provided, otherwise use default
            if user_value is not None:
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
            merged_mode = self._merge_mode(mode_defaults, user_mode)
            merged_mode["info"] = user_mode.get("info", mode_defaults.info)
            result[mode_name] = merged_mode

        return result

    def _merge_mode(self, defaults: ModeDefaults, user_mode: dict[str, Any]) -> dict[str, Any]:
        """
        Merge a single mode's settings.

        Args:
            defaults: Default values for the mode
            user_mode: User-provided values

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

            # Handle list merging
            if isinstance(default_value, list) and isinstance(user_value, list):
                result[attr] = user_value if user_value else default_value
            # Handle dict merging
            elif isinstance(default_value, dict) and isinstance(user_value, dict):
                result[attr] = {**default_value, **user_value}
            # Simple value
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
