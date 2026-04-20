"""Tests for API client."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from autoscaling.api.client import PortalClient


class TestPortalClient:
    """Tests for PortalClient class."""

    def test_client_init(self):
        """Test PortalClient initialization."""
        config = {"portal_scaling_link": "https://test.denbi.de"}
        client = PortalClient(config, "cluster123")
        assert client.config == config
        assert client.cluster_id == "cluster123"

    def test_get_cluster_data_success(self):
        """Test successful cluster data retrieval."""
        config = {"portal_scaling_link": "https://test.denbi.de"}
        client = PortalClient(config, "cluster123")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "AUTOSCALING_VERSION": "3.0.0",
            "workers": [{"hostname": "worker-01", "status": "ACTIVE"}],
        }

        with patch("autoscaling.api.client.requests.post", return_value=mock_response):
            with patch("autoscaling.utils.helpers.CLUSTER_PASSWORD_FILE", "/dev/null"):
                with patch(
                    "builtins.open", mock_open(read_data='{"password": "test"}')
                ):
                    result = client.get_cluster_data("test")

        assert result is not None
        assert len(result.get("workers", [])) == 1

    def test_get_cluster_data_unauthorized(self):
        """Test cluster data retrieval with unauthorized response."""
        config = {"portal_scaling_link": "https://test.denbi.de"}
        client = PortalClient(config, "cluster123")

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"message": "Invalid Password"}

        with patch("autoscaling.api.client.requests.post", return_value=mock_response):
            with pytest.raises(SystemExit):
                client.get_cluster_data("wrong_password")

    def test_scale_up_success(self):
        """Test successful scale up."""
        config = {"portal_scaling_link": "https://test.denbi.de"}
        client = PortalClient(config, "cluster123")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "success"}

        with patch("autoscaling.api.client.requests.post", return_value=mock_response):
            with patch("autoscaling.utils.helpers.CLUSTER_PASSWORD_FILE", "/dev/null"):
                with patch(
                    "builtins.open", mock_open(read_data='{"password": "test"}')
                ):
                    result = client.scale_up(
                        {"worker_flavor_name": "de.NBI large", "upscale_count": 2},
                        "test",
                    )

        assert result is not None
        assert result.get("message") == "success"

    def test_scale_down_specific_success(self):
        """Test successful scale down."""
        config = {"portal_scaling_link": "https://test.denbi.de"}
        client = PortalClient(config, "cluster123")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "success"}

        with patch("autoscaling.api.client.requests.post", return_value=mock_response):
            with patch("autoscaling.utils.helpers.CLUSTER_PASSWORD_FILE", "/dev/null"):
                with patch(
                    "builtins.open", mock_open(read_data='{"password": "test"}')
                ):
                    result = client.scale_down_specific(
                        ["worker-01", "worker-02"], "test"
                    )

        assert result is not None

    def test_get_url_scale_up(self):
        """Test scale up URL generation."""
        config = {"portal_scaling_link": "https://test.denbi.de/portal/api/autoscaling"}
        client = PortalClient(config, "cluster123")

        url = client._get_url_scale_up()
        assert (
            url == "https://test.denbi.de/portal/api/autoscaling/cluster123/scale-up/"
        )

    def test_get_url_info_cluster(self):
        """Test cluster info URL generation."""
        config = {"portal_scaling_link": "https://test.denbi.de/portal/api/autoscaling"}
        client = PortalClient(config, "cluster123")

        url = client._get_url_info_cluster()
        assert (
            url == "https://test.denbi.de/portal/api/autoscaling/cluster123/scale-data/"
        )
