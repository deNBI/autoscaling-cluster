"""
Scheduler interface definition for autoscaling.
Defines the contract for different scheduler implementations.
"""
import abc
from dataclasses import dataclass
from typing import Optional


@dataclass
class SchedulerNodeState:
    """
    Represents the state of a scheduler node.
    """
    hostname: str
    state: str  # ALLOC, MIX, IDLE, DRAIN, DOWN
    total_cpus: int
    free_memory: int  # in MB
    real_memory: int  # in MB
    temporary_disk: int  # in MB
    gres: list[str]  # Generic resources (e.g., ["gpu:0"])
    node_hostname: str


@dataclass
class SchedulerJobState:
    """
    Represents the state of a scheduler job.
    """
    jobid: int
    state: int  # 0=PENDING, 1=RUNNING, 3=COMPLETED
    state_str: str
    req_cpus: int
    req_mem: int  # in MB
    temporary_disk: int  # in MB
    priority: int
    jobname: str
    nodes: str
    elapsed: int  # in seconds
    comment: Optional[str] = None


class SchedulerInterface(abc.ABC):
    """
    Abstract base class for scheduler implementations.
    Defines the contract for scheduler interaction.
    """

    @abc.abstractmethod
    def test_connection(self) -> bool:
        """
        Test if scheduler connection is working.

        Returns:
            True if connection successful, False otherwise
        """
        pass

    @abc.abstractmethod
    def get_node_data(self) -> Optional[dict[str, SchedulerNodeState]]:
        """
        Fetch node data from the scheduler.

        Returns:
            Dictionary mapping node names to their states, or None on error
        """
        pass

    @abc.abstractmethod
    def get_job_data(self, days: int) -> Optional[dict[int, SchedulerJobState]]:
        """
        Fetch job data from the scheduler history.

        Args:
            days: Number of days of history to fetch

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        pass

    @abc.abstractmethod
    def get_node_data_live(self) -> Optional[dict[str, SchedulerNodeState]]:
        """
        Get live node data without using database cache.

        Returns:
            Dictionary mapping node names to their states, or None on error
        """
        pass

    @abc.abstractmethod
    def get_job_data_live(self) -> Optional[dict[int, SchedulerJobState]]:
        """
        Get live job data without using database cache.

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        pass

    @abc.abstractmethod
    def drain_node(self, node_name: str) -> bool:
        """
        Set a node to drain state.
        Currently running jobs will keep running, no further jobs will be scheduled.

        Args:
            node_name: Name of the node to drain

        Returns:
            True if successful, False otherwise
        """
        pass

    @abc.abstractmethod
    def resume_node(self, node_name: str) -> bool:
        """
        Remove drain state from a node.
        Further jobs will be scheduled on this node.

        Args:
            node_name: Name of the node to resume

        Returns:
            True if successful, False otherwise
        """
        pass

    @abc.abstractmethod
    def get_job_data_by_range(self, start_time: str, end_time: str) -> Optional[dict[int, SchedulerJobState]]:
        """
        Fetch job data within a specific time range.

        Args:
            start_time: Start time in format "YYYY-MM-DDTHH:MM:SS"
            end_time: End time in format "YYYY-MM-DDTHH:MM:SS"

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        pass
