"""
Configuration validator for autoscaling.
Validates configuration values for plausibility.
"""

from typing import Any

from autoscaling.config.loader import ConfigError


class ConfigValidator:
    """
    Validate autoscaling configuration values.
    """

    def __init__(self):
        """Initialize the validator."""
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def validate(self, config: dict[str, Any]) -> bool:
        """
        Validate the entire configuration.

        Args:
            config: Configuration dictionary to validate

        Returns:
            True if configuration is valid, False otherwise
        """
        self.errors = []
        self.warnings = []

        # Validate scaling section
        scaling_config = config.get("scaling", {})
        self._validate_scaling(scaling_config)

        return len(self.errors) == 0

    def _validate_scaling(self, config: dict[str, Any]) -> None:
        """
        Validate scaling configuration.

        Args:
            config: Scaling configuration section
        """
        # Validate scheduler
        scheduler = config.get("scheduler", "")
        valid_schedulers = ["slurm", "pbs", "lsf"]
        if scheduler and scheduler not in valid_schedulers:
            self.errors.append(f"Invalid scheduler '{scheduler}'. Must be one of: {valid_schedulers}")

        # Validate active mode
        active_mode = config.get("active_mode", "")
        valid_modes = ["basic", "approach", "adaptive", "sequence", "multi", "max", "default", "flavor", "min", "reactive"]
        if active_mode and active_mode not in valid_modes:
            self.errors.append(f"Invalid active_mode '{active_mode}'. Must be one of: {valid_modes}")

        # Validate mode-specific settings
        modes = config.get("mode", {})
        for mode_name, mode_config in modes.items():
            self._validate_mode(mode_name, mode_config)

        # Validate time values
        self._validate_time_values(config)

        # Validate percentage values
        self._validate_percentages(config)

    def _validate_mode(self, mode_name: str, config: dict[str, Any]) -> None:
        """
        Validate a single mode's configuration.

        Args:
            mode_name: Name of the mode
            config: Mode configuration
        """
        # Validate service_frequency
        service_freq = config.get("service_frequency", 60)
        if not isinstance(service_freq, int) or service_freq < 10:
            self.warnings.append(f"Mode '{mode_name}': service_frequency should be at least 10 seconds")

        # Validate scale_force
        scale_force = config.get("scale_force", 0.6)
        if not isinstance(scale_force, (int, float)) or not 0.0 <= scale_force <= 1.0:
            self.errors.append(f"Mode '{mode_name}': scale_force must be between 0.0 and 1.0")

        # Validate worker_cool_down
        cool_down = config.get("worker_cool_down", 60)
        if not isinstance(cool_down, int) or cool_down < 0:
            self.errors.append(f"Mode '{mode_name}': worker_cool_down must be a non-negative integer")

        # Validate limit values
        for limit_key in ["limit_memory", "limit_worker_starts", "limit_workers"]:
            limit_value = config.get(limit_key, 0)
            if not isinstance(limit_value, int) or limit_value < 0:
                self.errors.append(f"Mode '{mode_name}': {limit_key} must be a non-negative integer")

        # Validate smoothing_coefficient
        smoothing = config.get("smoothing_coefficient", 0.0)
        if not isinstance(smoothing, (int, float)) or not 0.0 <= smoothing <= 1.0:
            self.errors.append(f"Mode '{mode_name}': smoothing_coefficient must be between 0.0 and 1.0")

        # Validate flavor_depth
        flavor_depth = config.get("flavor_depth", -1)
        valid_depths = [-2, -3, -1, 0, 1, 2, 3]
        if flavor_depth not in valid_depths:
            self.errors.append(f"Mode '{mode_name}': flavor_depth must be one of {valid_depths}")

    def _validate_time_values(self, config: dict[str, Any]) -> None:
        """
        Validate time-related configuration values.

        Args:
            config: Configuration dictionary
        """
        # Validate time_range values
        time_range_max = config.get("time_range_max", 3600)
        time_range_min = config.get("time_range_min", 60)

        if time_range_max <= time_range_min:
            self.errors.append("time_range_max must be greater than time_range_min")

        if time_range_max < 60:
            self.warnings.append("time_range_max is very low (less than 60 seconds)")

    def _validate_percentages(self, config: dict[str, Any]) -> None:
        """
        Validate percentage-related configuration values.

        Args:
            config: Configuration dictionary
        """
        pending_pct = config.get("pending_jobs_percent", 1.0)
        if not isinstance(pending_pct, (int, float)) or not 0.0 <= pending_pct <= 2.0:
            self.errors.append("pending_jobs_percent must be between 0.0 and 2.0")

        job_match = config.get("job_match_value", 0.95)
        if not isinstance(job_match, (int, float)) or not 0.0 <= job_match <= 1.0:
            self.errors.append("job_match_value must be between 0.0 and 1.0")

    def is_valid(self) -> bool:
        """
        Check if the last validation was successful.

        Returns:
            True if valid, False otherwise
        """
        return len(self.errors) == 0

    def get_errors(self) -> list[str]:
        """
        Get validation errors.

        Returns:
            List of error messages
        """
        return self.errors

    def get_warnings(self) -> list[str]:
        """
        Get validation warnings.

        Returns:
            List of warning messages
        """
        return self.warnings

    def raise_if_invalid(self) -> None:
        """
        Raise ConfigError if validation failed.

        Raises:
            ConfigError: If validation failed
        """
        if self.errors:
            error_msg = "Configuration validation failed:\n"
            error_msg += "\n".join(f"  - {e}" for e in self.errors)
            raise ConfigError(error_msg)


def validate_config(config: dict[str, Any]) -> tuple[bool, list[str], list[str]]:
    """
    Convenience function to validate configuration.

    Args:
        config: Configuration dictionary to validate

    Returns:
        Tuple of (is_valid, errors, warnings)
    """
    validator = ConfigValidator()
    is_valid = validator.validate(config)
    return is_valid, validator.get_errors(), validator.get_warnings()
