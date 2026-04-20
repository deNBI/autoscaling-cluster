"""
Logging utilities.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

# Common log levels
LOG_LEVEL = logging.INFO


def setup_logger(
    log_file: str,
    log_level: int = LOG_LEVEL,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> logging.Logger:
    """
    Setup logger with file and console handlers.

    Args:
        log_file: Path to log file
        log_level: Logging level
        max_bytes: Max log file size before rotation
        backup_count: Number of backup files to keep

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove existing handlers
    logger.handlers = []

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler with rotation
    file_dir = os.path.dirname(log_file)
    if file_dir and not os.path.exists(file_dir):
        os.makedirs(file_dir)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger


def get_logger(name: str = "autoscaling") -> logging.Logger:
    """
    Get a logger instance by name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class ProcessFilter(logging.Filter):
    """
    Only accept log records from a specific PID.
    """

    def __init__(self, pid: int):
        super().__init__()
        self._pid = pid

    def filter(self, record: logging.LogRecord) -> bool:
        return record.process == self._pid
