"""
Exception classes for the scheduler module.
"""


class SchedulerError(Exception):
    """Base exception for scheduler-related errors."""

    pass


class SchedulerConnectionError(SchedulerError):
    """Exception raised when scheduler connection fails."""

    pass


class SchedulerDataError(SchedulerError):
    """Exception raised when scheduler data cannot be retrieved."""

    pass


class NodeNotFoundError(SchedulerError):
    """Exception raised when a node is not found."""

    pass


class JobNotFoundError(SchedulerError):
    """Exception raised when a job is not found."""

    pass
