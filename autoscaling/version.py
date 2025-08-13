import sys

import requests
from constants import (
    AUTOSCALING_VERSION,
    FILE_ID,
    FILE_PROG,
    OUTDATED_SCRIPT_MSG,
    RAW_REPO_LINK,
    REPO_API_LINK,
)
from logger import setup_custom_logger
from systemd import restart_systemd_service
from utils import update_file_via_url

logger = setup_custom_logger(__name__)
def version_check_scale_data(version,systemd_start:bool,automatic_update_active:bool=True):
    """
    Compare passed version and with own version data and initiate an update in case of mismatch.
    If the program does not run via systemd, the user must carry out the update himself.
    :param version: current version from cloud api
    :return:
    """
    if version != AUTOSCALING_VERSION:
        logger.warning(
            OUTDATED_SCRIPT_MSG.format(
                SCRIPT_VERSION=AUTOSCALING_VERSION, LATEST_VERSION=version
            )
        )
        if automatic_update_active:
            automatic_update(latest_version=version)

def automatic_update(latest_version: None,systemd_start:bool):
    """
    Download the program from sources if automatic update is active.
    Initiate update if started with systemd option otherwise just download the updated version.
    :return:
    """
    logger.info(f"Starting Script Autoupdate --> {latest_version}")
    logger.warning("I try to upgrade!")
    if not systemd_start:
            download_autoscaling(latest_version=latest_version)
            sys.exit(1)
    update_autoscaling(latest_version=latest_version)

    else:
        sys.exit(1)

def update_autoscaling(latest_version=None):
    """
    Download current autoscaling version and restart systemd autoscaling service.
    :return:
    """
    logger.debug(f"update autoscaling --> {latest_version}")
    if download_autoscaling(latest_version=latest_version):
        logger.debug("Restart Service")
        restart_systemd_service()
def download_autoscaling(latest_version=None):
    """
    Download current version of the program.
    Use autoscaling url from configuration file.
    :return:
    """
    logger.debug(f"Download Autoscaling provided version: {latest_version}")
    if latest_version:
        logger.info(f"Latest Version provided for update: {latest_version}")
        VERSION = latest_version
    else:
        logger.info("Latest Version not provided, requesting from github")
        VERSION = get_latest_release_tag()
    source_link = RAW_REPO_LINK + VERSION + "/" + FILE_ID
    logger.warning(f"Downloading new script via url: - {source_link}")
    return update_file_via_url(FILE_PROG, source_link, FILE_ID)

def get_latest_release_tag():
    release_url = REPO_API_LINK + "releases/latest"
    response = requests.get(release_url)
    latest_release = response.json()
    logger.info(f"latest release: {latest_release}")

    latest_tag = latest_release["tag_name"]
    return latest_tag
