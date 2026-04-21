"""
Core module for autoscaling.
Contains the main scaling logic and state management.
"""
from autoscaling.core.state import ScaleState, ScalingContext, ScalingAction, Rescale
from autoscaling.core.scaling_engine import ScalingEngine
from autoscaling.core.autoscaler import Autoscaler
from autoscaling.core.scale_actions import (
    cluster_scale_up,
    cluster_scale_down_specific,
    cluster_scale_down_specific_hostnames,
    cluster_scale_down_specific_self_check,
    cluster_scale_down_specific_hostnames_list,
    __cluster_scale_down_complete,
    __cluster_shut_down,
    rescale_cluster,
    classify_jobs_to_flavors,
)

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
