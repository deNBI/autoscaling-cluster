"""
API client for cloud portal communication.
"""

import json
import logging
import sys
import time
from typing import Any, Optional

import requests

from autoscaling.utils.helpers import (
    AUTOSCALING_VERSION,
    CLUSTER_PASSWORD_FILE,
    HTTP_CODE_OK,
    HTTP_CODE_UNAUTHORIZED,
    REQUEST_TIMEOUT,
    SCALING_TYPE,
    WAIT_CLUSTER_SCALING,
)

logger = logging.getLogger(__name__)


class PortalAPIError(Exception):
    """Exception for portal API errors."""


class PortalClient:
    """
    Client for the portal API.
    """

    def __init__(self, config: dict[str, Any], cluster_id: str):
        """
        Initialize portal client.

        Args:
            config: Configuration dictionary
            cluster_id: Cluster ID
        """
        self.config = config
        self.cluster_id = cluster_id

    def get_cluster_data(self, password: str) -> Optional[dict[str, Any]]:
        """
        Receive worker information from portal.

        Args:
            password: Cluster password

        Returns:
            Cluster data dictionary, or None on error
        """
        try:
            json_data = {
                "password": password,
                "scaling_type": SCALING_TYPE,
                "version": AUTOSCALING_VERSION,
            }

            url = self._get_url_info_cluster()
            response = requests.post(
                url=url, json=json_data, timeout=(30, REQUEST_TIMEOUT)
            )

            if response.status_code == HTTP_CODE_OK:
                res = response.json()
                self._check_version(res.get("AUTOSCALING_VERSION"))
                logger.debug("Received cluster data: %s", res)
                return res
            else:
                self._handle_unauthorized(res=response)

        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error: %s", e.response.text)
            logger.error("Status code: %s", e.response.status_code)
            logger.error("Unable to receive cluster data")
            res = e.response
            if res.status_code == HTTP_CODE_UNAUTHORIZED:
                self._handle_unauthorized(res=res)
            else:
                self._sleep_on_server_error()
        except OSError as error:
            logger.error("OS error: %s", error)
        except Exception as e:
            logger.error("Error accessing cluster data: %s", e)

        return None

    def scale_up(
        self, worker_data: dict[str, Any], password: str
    ) -> Optional[dict[str, Any]]:
        """
        Scale up the cluster.

        Args:
            worker_data: Worker data for scaling
            password: Cluster password

        Returns:
            API response, or None on error
        """
        worker_data.update({"password": password})
        return self._call_scale_api(self._get_url_scale_up(), worker_data)

    def scale_down_specific(
        self, worker_hostnames: list, password: str
    ) -> Optional[dict[str, Any]]:
        """
        Scale down specific workers.

        Args:
            worker_hostnames: List of worker hostnames to remove
            password: Cluster password

        Returns:
            API response, or None on error
        """
        data = {
            "password": password,
            "worker_hostnames": worker_hostnames,
            "version": AUTOSCALING_VERSION,
        }
        return self._call_scale_api(self._get_url_scale_down_specific(), data)

    def _call_scale_api(
        self, url: str, worker_data: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """
        Call scaling API endpoint.

        Args:
            url: API URL
            worker_data: Worker data to send

        Returns:
            API response, or None on error
        """
        from autoscaling.core.state import TerminateProtected

        logger.info("Sending cloud API data to %s", url)
        logger.debug("Data: %s", worker_data)

        try:
            with TerminateProtected():
                return self._call_api_(url, worker_data)
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error: %s", e.response.text)
            logger.error("Status code: %s", e.response.status_code)
            if e.response.status_code == HTTP_CODE_UNAUTHORIZED:
                self._handle_unauthorized(res=e.response)
            else:
                self._sleep_on_server_error()
        except OSError as error:
            logger.error("OS error: %s", error)
        except Exception as e:
            logger.error("Error accessing cloud API: %s", e)

        return None

    def _call_api_(
        self, portal_url_scale: str, worker_data: dict[str, Any]
    ) -> Optional[dict[str, Any]]:
        """
        Make the actual API call.

        Args:
            portal_url_scale: API URL
            worker_data: Worker data to send

        Returns:
            API response, or None on error
        """
        worker_data.update({"scaling_type": SCALING_TYPE})
        response = requests.post(
            url=portal_url_scale, json=worker_data, timeout=(30, REQUEST_TIMEOUT)
        )

        logger.debug(
            "Response code: %s, message: %s", response.status_code, response.text
        )

        try:
            response_json = response.json()
            if "password" in response_json:
                password = response_json["password"]
                logger.debug("Set New Password: %s", password)
                self._save_password(password)
                return response_json
            else:
                logger.debug("No password found in response")
                return response_json
        except json.JSONDecodeError:
            logger.debug("Invalid JSON response")
            return None

    def _save_password(self, password: str) -> None:
        """Save cluster password to file."""
        with open(CLUSTER_PASSWORD_FILE, "w", encoding="utf8") as f:
            json.dump({"password": password}, f, ensure_ascii=False, indent=4)

    def _get_cluster_password(self) -> str:
        """Load cluster password from file."""
        import json

        try:
            with open(CLUSTER_PASSWORD_FILE, "r", encoding="utf8") as f:
                pw_json = json.load(f)
            cluster_pw = pw_json.get("password")
            if not cluster_pw:
                logger.error("No cluster password found in %s", CLUSTER_PASSWORD_FILE)
                sys.exit(1)
            return cluster_pw
        except (IOError, json.JSONDecodeError) as exc:
            logger.error("Error reading password file: %s", exc)
            sys.exit(1)

    def _get_url_scale_up(self) -> str:
        """Get scale-up API URL."""
        base = self._get_portal_url_scaling()
        return base + self.cluster_id + "/scale-up/"

    def _get_url_scale_down_specific(self) -> str:
        """Get scale-down specific API URL."""
        base = self._get_portal_url_scaling()
        return base + self.cluster_id + "/scale-down/specific/"

    def _get_url_info_cluster(self) -> str:
        """Get cluster info API URL."""
        base = self._get_portal_url_scaling()
        return base + self.cluster_id + "/scale-data/"

    def _get_portal_url_scaling(self) -> str:
        """Get portal scaling URL base."""
        scaling_link = self.config.get("portal_scaling_link", "")
        return scaling_link if scaling_link.endswith("/") else scaling_link + "/"

    def _handle_unauthorized(self, res) -> None:
        """Handle unauthorized API response."""
        if res.status_code == HTTP_CODE_UNAUTHORIZED:
            error_msg = res.json().get("message", "")
            logger.error("Unauthorized: %s", error_msg)

            if "Invalid Password" in error_msg:
                logger.error("The password seems to be wrong.")
                sys.exit(1)
            elif "Wrong script version!" in error_msg:
                latest_version = res.json().get("latest_version")
                logger.warning("Outdated version. Latest: %s", latest_version)
                # automatic update would be triggered here

    def _check_version(self, remote_version: str) -> None:
        """Check if remote version is compatible."""
        if remote_version and remote_version != AUTOSCALING_VERSION:
            logger.warning(
                "Version mismatch: local=%s, remote=%s",
                AUTOSCALING_VERSION,
                remote_version,
            )

    def _sleep_on_server_error(self) -> None:
        """Sleep on server error before retry."""
        sleep_time = (
            int(self.config.get("service_frequency", 60)) * WAIT_CLUSTER_SCALING
        )
        logger.error("Server error, sleeping %s seconds", sleep_time)
        time.sleep(sleep_time)
