"""
Base scheduler interface.
"""

import abc
from typing import Any, Optional


class SchedulerInterface(abc.ABC):
    """
    Abstract base class for scheduler interfaces.
    """

    @abc.abstractmethod
    def scheduler_function_test(self) -> bool:
        """
        Test if scheduler can retrieve job and node data.

        Returns:
            True if successful, False otherwise
        """
        return False

    @abc.abstractmethod
    def fetch_scheduler_node_data(self) -> Optional[dict[str, Any]]:
        """
        Read scheduler data from database and return node data.

        Returns:
            Dictionary with node data, or None on error
        """
        return None

    @abc.abstractmethod
    def node_data_live(self) -> Optional[dict[str, Any]]:
        """
        Receive node data from scheduler without database usage.

        Returns:
            Dictionary with node data
        """
        return None

    @abc.abstractmethod
    def job_data_live(self) -> Optional[dict[str, Any]]:
        """
        Receive job data from scheduler without database usage.

        Returns:
            Dictionary with job data
        """
        return None

    @abc.abstractmethod
    def fetch_scheduler_job_data(self, num_days: int) -> Optional[dict[str, Any]]:
        """
        Read scheduler job data from database.

        Args:
            num_days: Number of days to look back

        Returns:
            Dictionary with job data, or None on error
        """
        return None

    @abc.abstractmethod
    def set_node_to_drain(self, w_key: str) -> None:
        """
        Set a node to drain state.

        Args:
            w_key: Node name/hostname
        """

    @abc.abstractmethod
    def set_node_to_resume(self, w_key: str) -> None:
        """
        Resume a drained node.

        Args:
            w_key: Node name/hostname
        """
