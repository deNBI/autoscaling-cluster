# Assuming you have the dataclasses and Configuration class defined as before.
from classes.configuration import Configuration
from constants import FILE_CONFIG_YAML
from logger import setup_custom_logger
from utils import read_config_yaml  # Import your utils

logger = setup_custom_logger(__name__)


def load_configuration(file_path=FILE_CONFIG_YAML) -> Configuration:
    """
    Loads configuration from a YAML file, tests for available values,
    relies on BasicMode defaults, and handles a flattened YAML structure.
    """

    yaml_config_data = read_config_yaml(file_path=file_path)

    if yaml_config_data is None:
        logger.warning("No configuration file found, using BasicMode default.")
        yaml_config_data = {}
    configuration = Configuration(kwargs=yaml_config_data)

    return configuration
