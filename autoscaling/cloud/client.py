"""
Portal API client for autoscaling.
Provides access to the cluster management API.
"""
import os
from typing import Optional

import requests

from autoscaling.cloud.exceptions import APIError, AuthError


# Constants
SCALING_TYPE = "autoscaling"
AUTOSCALING_VERSION = "2.3.0"


class PortalClient:
    """
    Client for the deNBI portal autoscaling API.
    """

    def __init__(
        self,
        api_url: str,
        webapp_url: str,
        password_file: str = "cluster_pw.json",
        timeout: int = 60,
    ):
        """
        Initialize the portal client.

        Args:
            api_url: Base URL of the autoscaling API
            webapp_url: URL of the web application
            password_file: Path to the cluster password file
            timeout: Request timeout in seconds
        """
        self.api_url = api_url.rstrip("/")
        self.webapp_url = webapp_url.rstrip("/")
        self.password_file = password_file
        self.timeout = timeout
        self._password = None

    def get_cluster_data(self) -> Optional[dict]:
        """
        Get cluster information from the API.

        Returns:
            Cluster data dictionary or None on error
        """
        url = f"{self.api_url}/cluster"
        response = self._post(url, {})
        return response

    def get_flavors(self) -> Optional[list[dict]]:
        """
        Get available flavors from the API.

        Returns:
            List of flavor dictionaries or None on error
        """
        url = f"{self.api_url}/flavors"
        response = self._post(url, {})
        return response

    def scale_up(self, flavor_name: str, count: int) -> Optional[dict]:
        """
        Scale up workers of a specific flavor.

        Args:
            flavor_name: Name of the flavor to scale up
            count: Number of workers to add

        Returns:
            API response or None on error
        """
        url = f"{self.api_url}/scale/up"
        payload = {
            "worker_flavor_name": flavor_name,
            "upscale_count": count,
        }
        return self._post(url, payload)

    def scale_down(self, hostnames: list[str]) -> Optional[dict]:
        """
        Scale down specific workers by hostname.

        Args:
            hostnames: List of worker hostnames to remove

        Returns:
            API response or None on error
        """
        url = f"{self.api_url}/scale/down"
        payload = {
            "worker_hostnames": hostnames,
        }
        return self._post(url, payload)

    def get_cluster_password(self) -> Optional[str]:
        """
        Get the cluster password from the password file.

        Returns:
            Password string or None if not found
        """
        if self._password is not None:
            return self._password

        if not os.path.exists(self.password_file):
            return None

        try:
            with open(self.password_file, "r", encoding="utf8") as f:
                data = f.read().strip()
                self._password = data
                return data
        except OSError:
            return None

    def _get(self, url: str) -> Optional[dict]:
        """
        Perform a GET request.

        Args:
            url: Full URL for the request

        Returns:
            Response JSON or None on error
        """
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise APIError(f"GET request failed: {e}")

    def _add_auth_headers(self, payload: dict) -> dict:
        """
        Add authentication headers and scaling context to payload.

        Args:
            payload: Original payload dictionary

        Returns:
            Payload with auth headers added
        """
        password = self.get_cluster_password()
        if password:
            payload["password"] = password
        payload["scaling_type"] = SCALING_TYPE
        payload["version"] = AUTOSCALING_VERSION
        return payload

    def _post(self, url: str, payload: dict) -> Optional[dict]:
        """
        Perform a POST request with JSON payload.

        Args:
            url: Full URL for the request
            payload: JSON payload to send

        Returns:
            Response JSON or None on error
        """
        try:
            payload = self._add_auth_headers(payload)
            response = requests.post(
                url, json=payload, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise APIError(f"POST request failed: {e}")

    def get_portal_link(self) -> str:
        """
        Get the web application URL.

        Returns:
            Web application URL
        """
        return self.webapp_url

    def get_scaling_link(self) -> str:
        """
        Get the scaling API URL.

        Returns:
            Scaling API URL
        """
        return self.api_url


class ClusterContext:
    """
    Context manager for cluster API access.
    Handles password setup and cleanup.
    """

    def __init__(self, client: PortalClient):
        """
        Initialize the cluster context.

        Args:
            client: PortalClient instance
        """
        self.client = client

    def __enter__(self):
        """Enter the context."""
        # Setup cluster password if needed
        password = self.client.get_cluster_password()
        if password is None:
            raise AuthError(
                "Cluster password not available. "
                "Run with -p to set password."
            )
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context."""
        # Cleanup if needed
        pass