"""
Cluster API functions for autoscaling.
Provides access to cluster and flavor data from the portal API.
"""
from typing import Optional

import requests

# Constants
SCALING_TYPE = "autoscaling"
AUTOSCALING_VERSION = "2.3.0"
REQUEST_TIMEOUT = (30, 60)
HTTP_CODE_OK = 200
HTTP_CODE_UNAUTHORIZED = 401


def get_url_scale_up(scaling_link: str, cluster_id: str) -> str:
    """Get scale up URL."""
    return scaling_link.rstrip("/") + "/" + cluster_id + "/scale-up/"


def get_url_scale_down_specific(scaling_link: str, cluster_id: str) -> str:
    """Get scale down specific URL."""
    return scaling_link.rstrip("/") + "/" + cluster_id + "/scale-down/specific/"


def get_url_info_cluster(scaling_link: str, cluster_id: str) -> str:
    """Get cluster info URL."""
    return scaling_link.rstrip("/") + "/" + cluster_id + "/scale-data/"


def get_url_info_flavors(scaling_link: str, cluster_id: str) -> str:
    """Get flavors info URL."""
    return scaling_link.rstrip("/") + "/" + cluster_id + "/usable_flavors/"


class ClusterAPI:
    """
    API client for cluster operations.
    """

    def __init__(self, scaling_link: str, cluster_id: str, password_file: str = "cluster_pw.json"):
        """
        Initialize the ClusterAPI.

        Args:
            scaling_link: Base URL of the scaling API
            cluster_id: Cluster ID
            password_file: Path to password file
        """
        self.scaling_link = scaling_link
        self.cluster_id = cluster_id
        self.password_file = password_file

    def _get_password(self) -> str:
        """Get cluster password from file."""
        from autoscaling.utils.helpers import get_cluster_password
        password = get_cluster_password(self.password_file)
        if password is None:
            raise ValueError(
                "Cluster password not available. "
                "Run with -p to set password."
            )
        return password

    def get_cluster_data(self) -> Optional[dict]:
        """
        Get cluster information from the API.

        Returns:
            Cluster data dictionary or None on error
        """
        url = get_url_info_cluster(self.scaling_link, self.cluster_id)
        json_data = {
            "password": self._get_password(),
            "scaling_type": SCALING_TYPE,
            "version": AUTOSCALING_VERSION,
        }

        try:
            response = requests.post(url, json=json_data, timeout=REQUEST_TIMEOUT)

            if response.status_code == HTTP_CODE_OK:
                res = response.json()
                self._version_check_scale_data(res.get("AUTOSCALING_VERSION"))
                return res
            else:
                self._handle_code_unauthorized(response)

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        return None

    def get_flavors(self) -> Optional[list[dict]]:
        """
        Get available flavors from the API.

        Returns:
            List of flavor dictionaries or None on error
        """
        url = get_url_info_flavors(self.scaling_link, self.cluster_id)
        json_data = {
            "password": self._get_password(),
            "version": AUTOSCALING_VERSION,
            "scaling_type": SCALING_TYPE,
        }

        try:
            response = requests.post(url, json=json_data, timeout=REQUEST_TIMEOUT)

            if response.status_code == HTTP_CODE_OK:
                flavors_data = response.json()
                return flavors_data

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        return None

    def scale_up(self, flavor_name: str, count: int) -> Optional[dict]:
        """
        Scale up workers of a specific flavor.

        Args:
            flavor_name: Name of the flavor to scale up
            count: Number of workers to add

        Returns:
            API response or None on error
        """
        url = get_url_scale_up(self.scaling_link, self.cluster_id)
        payload = {
            "password": self._get_password(),
            "worker_flavor_name": flavor_name,
            "upscale_count": count,
            "version": AUTOSCALING_VERSION,
        }

        try:
            response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Scale up failed: {e}")
            return None

    def scale_down(self, hostnames: list[str]) -> Optional[dict]:
        """
        Scale down specific workers by hostname.

        Args:
            hostnames: List of worker hostnames to remove

        Returns:
            API response or None on error
        """
        url = get_url_scale_down_specific(self.scaling_link, self.cluster_id)
        payload = {
            "password": self._get_password(),
            "worker_hostnames": hostnames,
            "version": AUTOSCALING_VERSION,
        }

        try:
            response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Scale down failed: {e}")
            return None

    def _version_check_scale_data(self, server_version: str) -> None:
        """Check if server version matches."""
        if server_version and server_version != AUTOSCALING_VERSION:
            print(
                f"Version mismatch: client {AUTOSCALING_VERSION}, "
                f"server {server_version}"
            )

    def _handle_code_unauthorized(self, response) -> None:
        """Handle unauthorized response."""
        if response.status_code == HTTP_CODE_UNAUTHORIZED:
            print(
                "The password seems to be wrong. Please generate a new one "
                "for the cluster on the Cluster Overview and register "
                "the password with 'autoscaling -p'."
            )


def get_flavor_data(scaling_link: str, cluster_id: str, password_file: str = "cluster_pw.json") -> Optional[list[dict]]:
    """
    Get flavor data from the API.

    Args:
        scaling_link: Base URL of the scaling API
        cluster_id: Cluster ID
        password_file: Path to password file

    Returns:
        List of flavor dictionaries or None on error
    """
    api = ClusterAPI(scaling_link, cluster_id, password_file)
    return api.get_flavors()


def get_cluster_workers(cluster_data: dict) -> list[dict]:
    """
    Process cluster worker data.

    Args:
        cluster_data: Raw cluster data dictionary

    Returns:
        Processed list of worker data
    """
    if cluster_data is None:
        return []

    cluster_workers = cluster_data.get("workers", [])

    for w_data in cluster_workers:
        flavor_data = w_data.get("flavor", {})
        w_data.update(
            {
                "memory_usable": reduce_flavor_memory(
                    convert_mib_to_gb(flavor_data.get("ram", 0))
                ),
                "temporary_disk": flavor_data.get("ephemeral", 0),
            }
        )

    return cluster_workers


def reduce_flavor_memory(mem_gb: int) -> int:
    """
    Calculate usable memory from raw GB value.

    Args:
        mem_gb: Raw memory in GB

    Returns:
        Usable memory in MB
    """
    mem = convert_gb_to_mb(mem_gb)
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    sub = int(mem / 16)

    if mem <= mem_border:
        sub = max(sub, mem_min)
    else:
        sub = min(sub, mem_max)

    return int(mem - sub)


def convert_gb_to_mb(value: int) -> int:
    """Convert GB to MB."""
    return int(value) * 1000


def convert_mib_to_gb(value: int) -> int:
    """Convert MiB to GB."""
    return int(int(value) / 1024)
