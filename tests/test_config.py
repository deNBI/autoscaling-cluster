"""
Unit tests for the configuration module.
"""
import os
import tempfile
import unittest

import yaml

from autoscaling.config.loader import ConfigLoader, ConfigError, load_config
from autoscaling.config.validator import ConfigValidator, validate_config
from autoscaling.config.defaults import DEFAULTS, MODE_DEFAULTS, GlobalDefaults, ModeDefaults


class TestConfigDefaults(unittest.TestCase):
    """Tests for default configuration values."""

    def test_global_defaults_exists(self):
        """Test that DEFAULTS instance exists."""
        self.assertIsInstance(DEFAULTS, GlobalDefaults)

    def test_mode_defaults_exists(self):
        """Test that MODE_DEFAULTS mapping exists."""
        self.assertIsInstance(MODE_DEFAULTS, dict)
        self.assertIn("basic", MODE_DEFAULTS)
        self.assertIn("adaptive", MODE_DEFAULTS)

    def test_mode_defaults_is_instance(self):
        """Test that mode defaults are ModeDefaults instances."""
        for mode_name, mode_defaults in MODE_DEFAULTS.items():
            self.assertIsInstance(
                mode_defaults, ModeDefaults,
                f"Mode '{mode_name}' should be a ModeDefaults instance"
            )

    def test_mode_defaults_has_required_attributes(self):
        """Test that mode defaults have all required attributes."""
        required_attrs = [
            "info", "service_frequency", "limit_memory", "limit_worker_starts",
            "limit_workers", "scale_force", "scale_delay", "worker_cool_down"
        ]
        for mode_name, mode_defaults in MODE_DEFAULTS.items():
            for attr in required_attrs:
                self.assertTrue(
                    hasattr(mode_defaults, attr),
                    f"Mode '{mode_name}' should have attribute '{attr}'"
                )


class TestConfigLoader(unittest.TestCase):
    """Tests for the ConfigLoader class."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.yaml")

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_config_file(self, config_dict):
        """Helper to create a config file."""
        with open(self.config_file, "w") as f:
            yaml.dump(config_dict, f)

    def test_load_with_empty_file(self):
        """Test loading with an empty config file."""
        self._create_config_file({})
        loader = ConfigLoader(self.config_file)
        config = loader.load()
        # Should have defaults applied
        self.assertIn("scaling", config)

    def test_load_with_missing_file(self):
        """Test loading with a non-existent file."""
        loader = ConfigLoader("/nonexistent/path/config.yaml")
        config = loader.load()
        self.assertIn("scaling", config)

    def test_load_with_valid_config(self):
        """Test loading with valid configuration."""
        config_dict = {
            "scaling": {
                "active_mode": "adaptive",
                "service_frequency": 120
            }
        }
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        config = loader.load()
        self.assertEqual(config["scaling"]["active_mode"], "adaptive")
        # service_frequency from defaults, not overwritten
        self.assertEqual(config["scaling"]["mode"]["basic"]["service_frequency"], 60)

    def test_get_method(self):
        """Test the get() method for nested keys."""
        config_dict = {
            "scaling": {
                "active_mode": "adaptive",
                "mode": {
                    "basic": {
                        "scale_force": 0.8
                    }
                }
            }
        }
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        self.assertEqual(loader.get("scaling.active_mode"), "adaptive")
        self.assertEqual(loader.get("scaling.mode.basic.scale_force"), 0.8)
        self.assertIsNone(loader.get("nonexistent.key"))

    def test_get_method_with_default(self):
        """Test the get() method with default value."""
        config_dict = {"scaling": {}}
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        self.assertEqual(loader.get("nonexistent.key", "default"), "default")

    def test_get_mode_method(self):
        """Test the get_mode() method."""
        config_dict = {
            "scaling": {
                "mode": {
                    "basic": {
                        "scale_force": 0.7
                    }
                }
            }
        }
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        mode_config = loader.get_mode("basic")
        self.assertEqual(mode_config["scale_force"], 0.7)

    def test_get_mode_method_missing(self):
        """Test the get_mode() method for non-existent mode."""
        config_dict = {"scaling": {}}
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        mode_config = loader.get_mode("nonexistent")
        self.assertEqual(mode_config, {})


class TestConfigValidator(unittest.TestCase):
    """Tests for the ConfigValidator class."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = ConfigValidator()

    def test_validate_valid_config(self):
        """Test validation of a valid configuration."""
        config = {
            "scheduler": "slurm",
            "active_mode": "basic",
            "mode": {
                "basic": {
                    "scale_force": 0.6,
                    "service_frequency": 60
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertTrue(is_valid)
        self.assertEqual(len(self.validator.get_errors()), 0)

    def test_validate_invalid_scheduler(self):
        """Test validation with invalid scheduler."""
        config = {
            "scaling": {
                "scheduler": "invalid",
                "active_mode": "basic"
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("Invalid scheduler", self.validator.get_errors()[0])

    def test_validate_invalid_mode(self):
        """Test validation with invalid mode."""
        config = {
            "scaling": {
                "scheduler": "slurm",
                "active_mode": "invalid_mode"
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("Invalid active_mode", self.validator.get_errors()[0])

    def test_validate_scale_force_out_of_range(self):
        """Test validation with scale_force out of range."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "scale_force": 1.5
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("scale_force must be between", self.validator.get_errors()[0])

    def test_validate_negative_worker_cool_down(self):
        """Test validation with negative worker_cool_down."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "worker_cool_down": -1
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("worker_cool_down must be a non-negative integer",
                      self.validator.get_errors()[0])

    def test_validate_time_range_invalid(self):
        """Test validation with invalid time range."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "time_range_max": 50,
                "time_range_min": 100
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("time_range_max must be greater than time_range_min",
                      self.validator.get_errors()[0])

    def test_validate_pending_jobs_percent(self):
        """Test validation with pending_jobs_percent out of range."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "pending_jobs_percent": 3.0
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)

    def test_is_valid_method(self):
        """Test the is_valid() method."""
        config = {"active_mode": "basic"}
        self.validator.validate(config)
        self.assertTrue(self.validator.is_valid())

    def test_raise_if_invalid_raises(self):
        """Test that raise_if_invalid() raises on invalid config."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "scale_force": -1.0
                    }
                }
            }
        }
        self.validator.validate(config)
        with self.assertRaises(ConfigError):
            self.validator.raise_if_invalid()


class TestValidateConfigFunction(unittest.TestCase):
    """Tests for the validate_config convenience function."""

    def test_returns_tuple(self):
        """Test that validate_config returns a tuple."""
        config = {"active_mode": "basic"}
        result = validate_config(config)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)

    def test_return_values(self):
        """Test that validate_config returns (is_valid, errors, warnings)."""
        config = {"active_mode": "basic"}
        is_valid, errors, warnings = validate_config(config)
        self.assertTrue(is_valid)
        self.assertEqual(errors, [])
        self.assertIsInstance(warnings, list)


if __name__ == "__main__":
    unittest.main()
