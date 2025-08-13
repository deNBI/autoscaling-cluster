import json
import sys
import time
from http import HTTPStatus

import requests
import yaml
from constants import (
    AUTOSCALING_VERSION,
    CLUSTER_PASSWORD_FILE,
    FILE_CONFIG_YAML,
    WAIT_CLUSTER_SCALING,
)
from logger import setup_custom_logger

from config import Configuration

logger = setup_custom_logger(__name__)


def read_config_yaml(file_path=FILE_CONFIG_YAML):
    """
    Read config from yaml file.
    :return:
    """
    yaml_config = read_yaml_file(file_path)
    logger.info("Load config from yaml file %s", file_path)
    if not yaml_config:
        logger.error("can not read configuration file")
        return None
    return yaml_config


def ___write_yaml_file(yaml_file_target, yaml_data):
    """
    Write yaml data to file.
    :param yaml_file_target:
    :param yaml_data:
    :return:
    """
    with open(yaml_file_target, "w+", encoding="utf8") as target:
        try:
            yaml.dump(yaml_data, target)
        except yaml.YAMLError as exc:
            logger.debug(exc)
            sys.exit(1)


def read_cluster_id():
    """
    Read cluster id from hostname
        - example hostname: bibigrid-master-clusterID
    :return: cluster id
    """

    with open("/etc/hostname", "r", encoding="utf8") as file:
        try:
            cluster_id_ = file.read().rstrip().split("-")[2]
        except (ValueError, IndexError):
            logger.error(
                "error: wrong cluster name \nexample: bibigrid-master-clusterID"
            )
            sys.exit(1)
    return cluster_id_


def read_yaml_file(file_path):
    """
    Read yaml data from file.
    :param file_path:
    :return:
    """

    try:
        with open(file_path, "r", encoding="utf8") as stream:
            yaml_data = yaml.safe_load(stream)
        return yaml_data
    except yaml.YAMLError as exc:
        logger.error(exc)
        return None
    except EnvironmentError:
        return None


def convert_gb_to_mb(value):
    """
    Convert GB value to MB.
    :param value:
    :return:
    """
    return int(value) * 1000


def convert_gb_to_mib(value):
    """
    Convert GB value to MiB.
    :param value:
    :return:
    """
    return int(value) * 1024


def convert_mib_to_gb(value):
    """
    Convert GB value to MiB.
    :param value:
    :return:
    """
    return int(int(value) / 1024)


def convert_tb_to_mb(value):
    """
    Convert TB value to MB.
    :param value:
    :return:
    """
    return int(value * 1000000)


def convert_tb_to_mib(value):
    """
    Convert TB value to MiB.
    :param value:
    :return:
    """
    return int(value) * 1000 * 1024


def reduce_flavor_memory(mem_gb):
    """
    Receive raw memory in GB and reduce this to a usable value,
    memory reduces to os requirements and other circumstances. Calculation according to BiBiGrid setup.
    :param mem_gb: memory
    :return: memory reduced value (mb)
    """
    mem = convert_gb_to_mb(mem_gb)
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    sub = int(mem / 16)
    if mem <= mem_border:
        sub = max(sub, mem_min)
    else:
        sub = min(sub, mem_max)
    return int(mem - sub)


def get_version():
    """
    Print the current autoscaling version.
    :return:
    """
    logger.debug(f"Version: {AUTOSCALING_VERSION}")


def get_wrong_password_msg(portal_url_webapp: str):
    logger.error(
        f"The password seems to be wrong. Please generate a new one "
        f"for the cluster on the Cluster Overview ({portal_url_webapp})"
        f" and register the password with 'autoscaling -p'."
    )


def get_cluster_password():
    """
    Read cluster password from CLUSTER_PASSWORD_FILE, the file should contain: {"password":"CLUSTER_PASSWORD"}

    :return: "CLUSTER_PASSWORD"
    """
    pw_json = read_json_file(CLUSTER_PASSWORD_FILE)
    cluster_pw = pw_json.get("password", None)
    if not cluster_pw:
        logger.error(
            "No Cluster Password. Please set the password via:\n autoscaling -p \n and restart the service!"
        )
        sys.exit(1)
    logger.debug("pw_json: %s", cluster_pw)
    return cluster_pw


def read_json_file(read_file):
    """
    Read json content from file and return it.
    :param read_file: file to read
    :return: file content as json
    """
    try:
        with open(read_file, "r", encoding="utf-8") as f:
            file_json = json.load(f)
        return file_json
    except IOError:
        logger.error("Error: file does not exist %s", read_file)
        return None
    except ValueError:
        logger.error("Decoding JSON data failed from file %s", read_file)
        return None


def write_json_file(save_file, content):
    """
    Save json content to file.
    :param save_file: file to write
    :return: file content as json
    """
    try:
        with open(save_file, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
            f.flush()
    except IOError:
        logger.error("Error writing file %s", save_file)
        sys.exit(1)


def set_cluster_password():
    """
    Save cluster password to CLUSTER_PASSWORD_FILE, the password can be entered when prompted.
    :return:
    """
    password_ = input("enter cluster password (copy&paste): ")
    tmp_pw = {"password": password_}
    write_json_file(CLUSTER_PASSWORD_FILE, tmp_pw)


def update_file_via_url(file_location, url, filename):
    """
    Update a file from an url link.
    :param file_location: File location with file name.
    :param url: Complete link to the external source.
    :param filename: Filename.
    :return: boolean, updated
    """
    try:
        logger.debug("download new  %s", filename)
        res = requests.get(url, allow_redirects=True)
        if res.status_code == HTTPStatus.OK:
            open(file_location, "wb").write(res.content)
            return True
        logger.error(
            "unable to update autoscaling %s from %s, status: %s",
            filename,
            url,
            res.status_code,
        )
    except requests.exceptions.HTTPError as err:
        logger.debug("error code: ", err.response.status_code)
        logger.debug(err.response.text)
    return False


def sleep_on_server_error(config: Configuration):
    sleep_time = int(config.active_mode.service_frequency) * WAIT_CLUSTER_SCALING
    if not config.systemd_start:
        sleep_time = WAIT_CLUSTER_SCALING
    logger.error("server error, sleep %s seconds", sleep_time)
    time.sleep(sleep_time)
