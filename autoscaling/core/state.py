"""
Core state and configuration module.
"""

import enum
import signal
from functools import total_ordering


class ScaleState(enum.Enum):
    """
    Possible program scaling states.
    """

    UP = 1
    DOWN = 0
    SKIP = 2
    DONE = 3
    DOWN_UP = 4
    FORCE_UP = 5
    DELAY = -1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class Rescale(enum.Enum):
    """
    Rescaling types.
    """

    INIT = 0
    CHECK = 1
    NONE = 2


class TerminateProtected:
    """
    Protect a section of code from being killed by SIGINT or SIGTERM.
    Example:
        with TerminateProtected():
            function_1()
            function_2()
    """

    killed = False

    def _handler(self, signum, frame):
        import logging

        logging.error(
            "Received SIGINT or SIGTERM! Finishing this code block, then exiting."
        )
        self.killed = True

    def __enter__(self):
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, type, value, traceback):
        if self.killed:
            import sys

            sys.exit(0)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)


@total_ordering
class FlavorPriority:
    """
    Flavor priority for sorting.
    """

    def __init__(self, flavor_data: dict):
        self.flavor_data = flavor_data

    @property
    def priority(self) -> int:
        """Get flavor priority based on vCPUs."""
        return self.flavor_data.get("flavor", {}).get("vcpus", 0)

    def __eq__(self, other):
        if not isinstance(other, FlavorPriority):
            return NotImplemented
        return self.priority == other.priority

    def __lt__(self, other):
        if not isinstance(other, FlavorPriority):
            return NotImplemented
        return self.priority < other.priority
