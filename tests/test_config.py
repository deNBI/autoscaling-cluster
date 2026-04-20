"""Tests for configuration loading."""

import os
import tempfile
from unittest.mock import patch

import pytest

from autoscaling.config.loader import ConfigError, load_config


class TestConfigLoader:
    """Tests for configuration loader."""

    def test_load_config_missing_file(self):
        """Test loading config when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "nonexistent.yaml")
            # The function returns default config if file not found
            result = load_config(config_path)
            assert result is not None
            assert isinstance(result, dict)

    def test_load_config_with_valid_file(self):
        """Test loading config from valid YAML file."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "autoscaling_config.yaml")
            config_data = {
                "scaling": {
                    "portal_scaling_link": "https://test.denbi.de",
                    "active_mode": "basic",
                    "ignore_workers": ["worker-01"],
                }
            }
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            loaded = load_config(config_path)
            assert loaded["portal_scaling_link"] == "https://test.denbi.de"
            assert loaded["active_mode"] == "basic"
            assert loaded["ignore_workers"] == ["worker-01"]

    def test_load_config_missing_scaling_section(self):
        """Test loading config with missing scaling section."""
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "autoscaling_config.yaml")
            config_data = {"other_section": {"key": "value"}}
            with open(config_path, "w") as f:
                yaml.dump(config_data, f)

            with pytest.raises(ConfigError, match="missing 'scaling' section"):
                load_config(config_path)


class TestConfigFunctions:
    """Tests for configuration helper functions."""

    def test_read_cluster_id_valid(self):
        """Test reading cluster ID from valid hostname."""
        with patch("builtins.open") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = (
                "bibigrid-master-cluster123\n"
            )
            from autoscaling.config.loader import read_cluster_id

            cluster_id = read_cluster_id()
            assert cluster_id == "cluster123"

    def test_read_cluster_id_invalid(self):
        """Test reading cluster ID from invalid hostname."""
        with patch("builtins.open") as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = (
                "invalid-hostname\n"
            )
            from autoscaling.config.loader import read_cluster_id

            with pytest.raises(ConfigError):
                read_cluster_id()
