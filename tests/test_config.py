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


class TestConfigValidatorAdditional(unittest.TestCase):
    """Additional tests for ConfigValidator edge cases."""

    def setUp(self):
        """Set up test fixtures."""
        self.validator = ConfigValidator()

    def test_validate_service_frequency_warning(self):
        """Test that low service_frequency generates a warning."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "service_frequency": 5  # Below minimum of 10
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        # Should be valid but with warning
        self.assertTrue(is_valid)
        warnings = self.validator.get_warnings()
        self.assertIn("service_frequency", warnings[0])

    def test_validate_smoothing_coefficient_out_of_range(self):
        """Test validation with smoothing_coefficient out of range."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "smoothing_coefficient": 1.5
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("smoothing_coefficient", self.validator.get_errors()[0])

    def test_validate_invalid_flavor_depth(self):
        """Test validation with invalid flavor_depth."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "flavor_depth": 5  # Not in valid depths
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("flavor_depth", self.validator.get_errors()[0])

    def test_validate_negative_limit_values(self):
        """Test validation with negative limit values."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "limit_memory": -100,
                        "limit_worker_starts": 10,
                        "limit_workers": 0
                    }
                }
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        errors = self.validator.get_errors()
        self.assertTrue(any("limit_memory" in e for e in errors))

    def test_validate_time_range_max_warning(self):
        """Test that very low time_range_max generates warning."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "time_range_max": 30,  # Below 60 second threshold
                "time_range_min": 10
            }
        }
        is_valid = self.validator.validate(config)
        self.assertTrue(is_valid)
        warnings = self.validator.get_warnings()
        self.assertIn("time_range_max", warnings[0])

    def test_validate_job_match_value(self):
        """Test validation with job_match_value out of range."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "job_match_value": -0.5
            }
        }
        is_valid = self.validator.validate(config)
        self.assertFalse(is_valid)
        self.assertIn("job_match_value", self.validator.get_errors()[0])

    def test_validate_with_all_valid_modes(self):
        """Test validation with all valid mode names."""
        for mode_name in ["basic", "approach", "adaptive", "sequence", "multi",
                          "max", "default", "flavor", "min", "reactive"]:
            config = {
                "scaling": {
                    "active_mode": mode_name,
                    "mode": {
                        mode_name: {
                            "scale_force": 0.6,
                            "service_frequency": 60
                        }
                    }
                }
            }
            is_valid = self.validator.validate(config)
            self.assertTrue(is_valid, f"Mode '{mode_name}' should be valid")

    def test_get_warnings_method(self):
        """Test the get_warnings() method."""
        config = {
            "scaling": {
                "active_mode": "basic",
                "mode": {
                    "basic": {
                        "service_frequency": 5
                    }
                }
            }
        }
        self.validator.validate(config)
        warnings = self.validator.get_warnings()
        self.assertIsInstance(warnings, list)
        self.assertGreater(len(warnings), 0)

    def test_validate_empty_config(self):
        """Test validation with empty configuration."""
        config = {}
        is_valid = self.validator.validate(config)
        # Should be valid since defaults will be used
        self.assertTrue(is_valid)


class TestEnvVarOverride(unittest.TestCase):
    """Tests for environment variable override functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.yaml")
        self._original_env = os.environ.copy()

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        os.environ.clear()
        os.environ.update(self._original_env)

    def _create_config_file(self, config_dict):
        """Helper to create a config file."""
        with open(self.config_file, "w") as f:
            yaml.dump(config_dict, f)

    def test_env_var_overrides_yaml(self):
        """Test that environment variables override YAML config."""
        config_dict = {"scaling": {"active_mode": "basic"}}
        self._create_config_file(config_dict)

        # Set environment variable
        os.environ["AUTOSCALING_ACTIVE_MODE"] = "adaptive"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["active_mode"], "adaptive")

    def test_env_var_overrides_mode_settings(self):
        """Test that environment variables override mode settings."""
        config_dict = {"scaling": {"mode": {"basic": {"scale_force": 0.6}}}}
        self._create_config_file(config_dict)

        # Set mode-specific environment variable
        os.environ["AUTOSCALING_MODE_BASIC_SCALE_FORCE"] = "0.8"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["mode"]["basic"]["scale_force"], 0.8)

    def test_env_var_int_conversion(self):
        """Test that environment variables are converted to int."""
        # service_frequency is in mode settings, not global settings
        config_dict = {"scaling": {"mode": {"basic": {"service_frequency": 60}}}}
        self._create_config_file(config_dict)

        # service_frequency needs mode prefix in env var
        os.environ["AUTOSCALING_MODE_BASIC_SERVICE_FREQUENCY"] = "120"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["mode"]["basic"]["service_frequency"], 120)

    def test_env_var_float_conversion(self):
        """Test that environment variables are converted to float."""
        config_dict = {"scaling": {}}
        self._create_config_file(config_dict)

        os.environ["AUTOSCALING_MODE_BASIC_SCALE_FORCE"] = "0.75"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["mode"]["basic"]["scale_force"], 0.75)

    def test_env_var_bool_conversion(self):
        """Test that environment variables are converted to bool."""
        config_dict = {"scaling": {}}
        self._create_config_file(config_dict)

        os.environ["AUTOSCALING_AUTOMATIC_UPDATE"] = "true"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["automatic_update"], True)

    def test_env_var_list_conversion(self):
        """Test that comma-separated env vars are converted to list."""
        config_dict = {"scaling": {}}
        self._create_config_file(config_dict)

        os.environ["AUTOSCALING_IGNORE_WORKERS"] = "node1,node2,node3"

        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertEqual(config["scaling"]["ignore_workers"], ["node1", "node2", "node3"])


class TestConfigLoaderEdgeCases(unittest.TestCase):
    """Tests for edge cases in ConfigLoader."""

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

    def test_load_with_nested_config(self):
        """Test that mode settings only include known ModeDefaults attributes."""
        # The _merge_mode method only includes attributes from ModeDefaults
        # Custom fields not in ModeDefaults are not included in the merged config
        config_dict = {
            "scaling": {
                "mode": {
                    "basic": {
                        "custom_field": "custom_value",
                        "scale_force": 0.8
                    }
                }
            }
        }
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        config = loader.load()

        # Only known ModeDefaults attributes are included
        # scale_force is overridden from 0.6 to 0.8
        self.assertEqual(config["scaling"]["mode"]["basic"]["scale_force"], 0.8)
        # custom_field is not in ModeDefaults, so it's not in merged config
        self.assertNotIn("custom_field", config["scaling"]["mode"]["basic"])

    def test_get_with_partial_key_match(self):
        """Test that get() doesn't match partial keys."""
        config_dict = {
            "scaling": {
                "mode": {
                    "basic": {"scale_force": 0.6},
                    "basic_extra": {"scale_force": 0.7}
                }
            }
        }
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        # Should only match exact key "basic"
        mode_config = loader.get_mode("basic")
        self.assertEqual(mode_config["scale_force"], 0.6)
        self.assertNotIn("basic_extra", mode_config)

    def test_get_mode_case_sensitive(self):
        """Test that mode names are case-sensitive."""
        # The get_mode method looks up from merged config which has MODE_DEFAULTS keys
        config_dict = {"scaling": {"mode": {"Basic": {"scale_force": 0.6}}}}
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        loader.load()

        # "basic" mode returns the default config since "Basic" != "basic"
        mode_config = loader.get_mode("basic")
        # Should return the default values for "basic" mode
        self.assertEqual(mode_config["scale_force"], 0.6)  # default value from MODE_DEFAULTS

        # "Basic" is not a known mode in MODE_DEFAULTS, so get_mode returns empty dict
        # The config is stored under "Basic" in the raw config, but get_mode looks in merged mode config
        mode_config_basic = loader.get_mode("Basic")
        # Since "Basic" is not in MODE_DEFAULTS, it returns empty dict
        self.assertEqual(mode_config_basic, {})

    def test_load_with_invalid_yaml_raises_error(self):
        """Test that invalid YAML raises ConfigError."""
        # Create invalid YAML file
        with open(self.config_file, "w") as f:
            f.write("key: value\n  invalid: indent")

        loader = ConfigLoader(self.config_file)
        with self.assertRaises(ConfigError):
            loader.load()

    def test_raw_config_property(self):
        """Test the raw_config property."""
        config_dict = {"scaling": {"active_mode": "basic"}}
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        config = loader.load()

        # raw_config should be None after load (YAML was not stored)
        # This may need adjustment based on implementation
        self.assertIsNone(loader.raw_config)

    def test_merged_config_property(self):
        """Test the merged_config property."""
        config_dict = {"scaling": {"active_mode": "basic"}}
        self._create_config_file(config_dict)
        loader = ConfigLoader(self.config_file)
        config = loader.load()

        self.assertIsNotNone(loader.merged_config)
        self.assertEqual(loader.merged_config["scaling"]["active_mode"], "basic")


if __name__ == "__main__":
    unittest.main()
