import signal
import sys

from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


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
        logger.error(
            "Received SIGINT or SIGTERM! Finishing this code block, then exiting."
        )
        self.killed = True

    def __enter__(self):
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, type, value, traceback):
        if self.killed:
            sys.exit(0)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)
