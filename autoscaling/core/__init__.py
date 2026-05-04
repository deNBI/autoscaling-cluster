"""
Core module for autoscaling.
Contains the main scaling logic and state management.
"""

from autoscaling.core.autoscaler import Autoscaler
from autoscaling.core.scale_actions import (
    __cluster_scale_down_complete,
    __cluster_shut_down,
    classify_jobs_to_flavors,
    cluster_scale_down_specific,
    cluster_scale_down_specific_hostnames,
    cluster_scale_down_specific_hostnames_list,
    cluster_scale_down_specific_self_check,
    cluster_scale_up,
    rescale_cluster,
)
from autoscaling.core.scaling_engine import ScalingEngine
from autoscaling.core.state import Rescale, ScaleState, ScalingAction, ScalingContext

__all__ = [
    "ScaleState",
    "ScalingContext",
    "ScalingAction",
    "Rescale",
    "ScalingEngine",
    "Autoscaler",
    "cluster_scale_up",
    "cluster_scale_down_specific",
    "cluster_scale_down_specific_hostnames",
    "cluster_scale_down_specific_self_check",
    "cluster_scale_down_specific_hostnames_list",
    "__cluster_scale_down_complete",
    "__cluster_shut_down",
    "rescale_cluster",
    "classify_jobs_to_flavors",
]
