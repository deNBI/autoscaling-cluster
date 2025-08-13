import sys

import yaml
from constants import AUTOSCALING_VERSION, FILE_CONFIG_YAML
from logger import setup_custom_logger

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
