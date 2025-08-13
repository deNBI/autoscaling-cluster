import logging
import os
import traceback
from logging.handlers import RotatingFileHandler

from colorama import Fore, Style, init

# Initialize colorama to support ANSI color codes on Windows terminals
init()


def setup_custom_logger(name):
    LOG_FILE_HANDLER_ACTIVATED = os.environ.get("LOG_FILE_HANDLER_ACTIVATED", True)
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    LOG_FILE = os.environ.get("LOG_FILE", "log/autoscaling.log")
    LOG_BACKUP_COUNT = int(os.environ.get("LOG_BACKUP_COUNT", 5))
    LOG_MAX_BYTES = int(os.environ.get("LOG_MAX_BATES", 1073741824))

    # Define the formatter with color coding
    class ColoredFormatter(logging.Formatter):
        def format(self, record):
            if record.levelno == logging.ERROR or record.levelno == logging.CRITICAL:
                message = f"{Fore.RED}{super().format(record)}{Style.RESET_ALL}"
            elif record.levelno == logging.WARNING:
                message = f"{Fore.YELLOW}{super().format(record)}{Style.RESET_ALL}"
            else:
                message = super().format(record)

            if record.exc_info:
                tb_lines = traceback.format_exception(*record.exc_info)
                tb_message = "".join(tb_lines)
                message += f"\n{Fore.RED}{tb_message}{Style.RESET_ALL}"
            return message

    formatter = ColoredFormatter(
        fmt="%(asctime)s - [%(levelname)s] - [%(pathname)s:%(funcName)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    # Create the log directory if it does not exist
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    file_handler = RotatingFileHandler(
        maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, filename=LOG_FILE
    )
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    logger.addHandler(handler)
    if LOG_FILE_HANDLER_ACTIVATED:
        logger.addHandler(file_handler)

    return logger
