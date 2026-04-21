"""
State management for autoscaling.
Defines the scaling states and context.
"""
import enum
from dataclasses import dataclass
from typing import Optional


class ScaleState(enum.Enum):
    """
    Possible program scaling states.
    """

    UP = 1  # Scale up
    DOWN = 0  # Scale down
    SKIP = 2  # Skip scaling
    DONE = 3  # Scaling completed
    DOWN_UP = 4  # Scale down then up
    FORCE_UP = 5  # Force scale up
    DELAY = -1  # Delay scaling


@dataclass
class ScalingContext:
    """
    Context for scaling decisions.
    Contains all information needed to make scaling decisions.
    """

    # Configuration
    mode: str
    scale_force: float
    scale_delay: int
    worker_cool_down: int
    limit_memory: int
    limit_worker_starts: int
    limit_workers: int

    # Worker information
    worker_count: int
    worker_in_use: list[str]
    worker_drain: list[str]
    worker_free: int

    # Job information
    jobs_pending: list[dict]
    jobs_running: list[dict]
    jobs_pending_count: int
    jobs_running_count: int

    # Flavor information
    flavor_data: list[dict]
    flavor_default: Optional[str] = None

    # Forecast settings
    forecast_by_flavor_history: bool = False
    forecast_by_job_history: bool = False
    forecast_active_worker: int = 0
    job_time_threshold: float = 0.5
    smoothing_coefficient: float = 0.0

    # Flavor depth options
    flavor_depth: int = -1  # DEPTH_MULTI
    large_flavors: bool = False
    large_flavors_except_hmf: bool = True

    @property
    def need_workers(self) -> bool:
        """
        Check if workers are needed.

        Returns:
            True if workers are needed
        """
        return (
            len(self.jobs_pending) > 0
            and len(self.worker_in_use) == 0
            and self.worker_free == 0
        )

    @property
    def can_scale_up(self) -> bool:
        """
        Check if scaling up is possible.

        Returns:
            True if scaling up is possible
        """
        if self.limit_workers > 0 and self.worker_count >= self.limit_workers:
            return False
        if self.limit_memory > 0:
            # Check memory limit
            pass  # Memory check implemented in scaling engine
        return True

    @property
    def can_scale_down(self) -> bool:
        """
        Check if scaling down is possible.

        Returns:
            True if scaling down is possible
        """
        # Always allow scaling down if we have workers
        return self.worker_count > 0


@dataclass
class ScalingAction:
    """
    A scaling action to be executed.
    """

    # Scale up
    upscale: bool = False
    upscale_flavor: Optional[str] = None
    upscale_count: int = 0

    # Scale down
    downscale: bool = False
    downscale_workers: list[str] = None

    # Reason for action
    reason: str = ""

    def __post_init__(self):
        if self.downscale_workers is None:
            self.downscale_workers = []

    @property
    def is_noop(self) -> bool:
        """
        Check if this is a no-op action.

        Returns:
            True if no action needed
        """
        return not self.upscale and not self.downscale

    @classmethod
    def upscale_action(
        cls, flavor: str, count: int, reason: str = ""
    ) -> "ScalingAction":
        """
        Create an upscale action.

        Args:
            flavor: Flavor to scale up
            count: Number of workers
            reason: Reason for scaling

        Returns:
            ScalingAction instance
        """
        return cls(
            upscale=True,
            upscale_flavor=flavor,
            upscale_count=count,
            reason=reason,
        )

    @classmethod
    def downscale_action(
        cls, workers: list[str], reason: str = ""
    ) -> "ScalingAction":
        """
        Create a downscale action.

        Args:
            workers: List of worker hostnames to remove
            reason: Reason for scaling

        Returns:
            ScalingAction instance
        """
        return cls(
            downscale=True,
            downscale_workers=workers,
            reason=reason,
        )

    @classmethod
    def noop_action(cls, reason: str = "") -> "ScalingAction":
        """
        Create a no-op action.

        Args:
            reason: Reason for no action

        Returns:
            ScalingAction instance
        """
        return cls(reason=reason)


@dataclass
class WorkerResource:
    """
    Resource requirements for a worker.
    """

    cpu: int
    memory: int  # in MB
    disk: int  # in MB
