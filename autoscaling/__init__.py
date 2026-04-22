"""
Autoscaling package for deNBI clusters.
"""
__version__ = "2.3.0"
__author__ = "deNBI"

from autoscaling.cluster.exceptions import (
    ClusterError,
    APIError,
    AuthError,
    ConfigError,
    FlavorError,
    ScaleError,
    VersionMismatchError,
)
from autoscaling.scheduler.exceptions import (
    SchedulerError,
    SchedulerConnectionError,
    SchedulerDataError,
    NodeNotFoundError,
    JobNotFoundError,
)
from autoscaling.cloud.exceptions import (
    CloudError,
    APIError as CloudAPIError,
    AuthError as CloudAuthError,
    PlaybookError,
)

__all__ = [
    "__version__",
    "__author__",
    # Cluster exceptions
    "ClusterError",
    "APIError",
    "AuthError",
    "ConfigError",
    "FlavorError",
    "ScaleError",
    "VersionMismatchError",
    # Scheduler exceptions
    "SchedulerError",
    "SchedulerConnectionError",
    "SchedulerDataError",
    "NodeNotFoundError",
    "JobNotFoundError",
    # Cloud exceptions
    "CloudError",
    "CloudAPIError",
    "CloudAuthError",
    "PlaybookError",
]
