import sys
from http import HTTPStatus
from pprint import pformat

import requests
from constants import AUTOSCALING_VERSION, SCALING_TYPE
from logger import setup_custom_logger
from utils import get_cluster_password, get_wrong_password_msg, sleep_on_server_error
from version import automatic_update, version_check_scale_data

from config import Configuration

logger = setup_custom_logger(__name__)


def get_usable_flavors_from_api():

    response = requests.post(
        url=get_url_info_flavors(),
        json={
            "password": get_cluster_password(),
            "version": AUTOSCALING_VERSION,
            "scaling_type": SCALING_TYPE,
        },
    )
    if response.status_code == HTTPStatus.OK:
        res = response.json()
        version_check_scale_data(res["AUTOSCALING_VERSION"])
        logger.debug(pformat(res))
        return res
    else:
        handle_code_unauthorized(res=response)


def get_cluster_data(config: Configuration):
    """
    Receive worker information from portal.
    request example:
    :return:
        cluster data dictionary
        if api error: None
    """
    try:
        json_data = {
            "password": get_cluster_password(),
            "scaling_type": SCALING_TYPE,
            "version": AUTOSCALING_VERSION,
        }
        response = requests.post(
            url=config.portal_cluster_info_link, json=json_data, timeout=10000
        )
        logger.debug(
            "response code %s, send json_data %s", response.status_code, json_data
        )

        if response.status_code == HTTPStatus.OK:
            res = response.json()
            version_check_scale_data(res["AUTOSCALING_VERSION"])
            logger.debug(pformat(res))
            return res
        else:
            handle_code_unauthorized(res=response)

    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        logger.error("unable to receive cluster data")
        if res.status_code == HTTPStatus.UNAUTHORIZED:
            handle_code_unauthorized(res=res)
        else:
            sleep_on_server_error()
    except OSError as error:
        logger.error(error)
    except Exception as e:
        logger.error("error by accessing cluster data %s", e)
    return None


def handle_code_unauthorized(res):
    if res.status_code == HTTPStatus.UNAUTHORIZED:
        error_msg = res.json()["message"]
        logger.error(error_msg)

        if "Invalid Password" in error_msg:

            logger.error(get_wrong_password_msg())
            sys.exit(1)

        elif "Wrong script version!" in error_msg:

            latest_version = res.json().get("latest_version", None)

            automatic_update(latest_version=latest_version)
