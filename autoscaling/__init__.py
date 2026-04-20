"""
Auto-scaling Cluster Package

A Python module for auto-scaling compute clusters with support for
multiple schedulers (Slurm, Kubernetes) and cloud providers.
"""

__version__ = "3.0.0"
__author__ = "deNBI"

from autoscaling.api.client import PortalClient
from autoscaling.config.loader import load_config
from autoscaling.core.scaling_engine import ScalingEngine
from autoscaling.core.state import Rescale, ScaleState
from autoscaling.data.models import ClusterWorker, Flavor, Job, Worker
from autoscaling.forecasting.predictor import JobPredictor
from autoscaling.scheduler.base import SchedulerInterface

__all__ = [
    "__version__",
    "__author__",
    "ScaleState",
    "Rescale",
    "load_config",
    "Job",
    "Worker",
    "Flavor",
    "ClusterWorker",
    "SchedulerInterface",
    "PortalClient",
    "JobPredictor",
    "ScalingEngine",
]
