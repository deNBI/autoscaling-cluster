import os
import subprocess

from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def restart_systemd_service():
    """
    Restart systemd service
    :return:
    """
    os.system("sudo systemctl restart autoscaling.service")
    status_response_systemd_service()


def status_systemd_service():
    """
    Autoscaling status from systemd service
    :return:
    """
    os.system("systemctl status autoscaling.service")


def status_response_systemd_service():
    """
    Print status response call from autoscaling service
    :return:
    """
    get_status = subprocess.Popen(
        "systemctl is-active autoscaling.service", shell=True, stdout=subprocess.PIPE
    ).stdout
    response = get_status.read().decode()
    logger.info("autoscaling service: ", response)
