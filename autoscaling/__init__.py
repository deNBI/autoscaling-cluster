"""
Autoscaling package for deNBI clusters.
"""

__version__ = "2.3.0"
__author__ = "deNBI"

from autoscaling.cloud.exceptions import APIError as CloudAPIError
from autoscaling.cloud.exceptions import AuthError as CloudAuthError
from autoscaling.cloud.exceptions import (
    CloudError,
    PlaybookError,
)
from autoscaling.cluster.exceptions import (
    APIError,
    AuthError,
    ClusterError,
    ConfigError,
    FlavorError,
    ScaleError,
    VersionMismatchError,
)
from autoscaling.scheduler.exceptions import (
    JobNotFoundError,
    NodeNotFoundError,
    SchedulerConnectionError,
    SchedulerDataError,
    SchedulerError,
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
