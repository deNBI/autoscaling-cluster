"""
Core module for autoscaling.
Contains the main scaling logic and state management.
"""
from autoscaling.core.state import ScaleState, ScalingContext, ScalingAction

__all__ = ["ScaleState", "ScalingContext", "ScalingAction"]
