from dataclasses import dataclass, field

from constants import BASIC_MODE
from logger import setup_custom_logger
from utils import read_cluster_id

from .config_modes import (
    AdaptiveMode,
    ApproachMode,
    BasicMode,
    DefaultMode,
    FlavorMode,
    MaxMode,
    MinMode,
    MultiMode,
    ReactiveMode,
    SequenceMode,
)

logger = setup_custom_logger(__name__)


# TODO helper function wich provides info for all config fields
@dataclass
class Configuration:
    portal_scaling_link: str = field(
        default="https://simplevm.denbi.de/portal/api/autoscaling/"
    )
    portal_webapp_link: str = field(
        default="https://simplevm.denbi.de/portal/webapp/#/clusters/overview"
    )
    scaling_script_url: str = field(
        default="https://raw.githubusercontent.com/deNBI/user_scripts/refs/heads/master/bibigrid/scaling.py"
    )
    scheduler: str = field(default="slurm")

    active_mode_name: str = field(default=BASIC_MODE)
    automatic_update: bool = field(default=True)
    database_reset: bool = field(default=False)
    pattern_id: str = field(default="")
    history_recall: int = field(default=7)
    ignore_workers: list[str] = field(default_factory=list)
    pending_jobs_percent: float = field(default=1.0)
    resource_sorting: bool = field(default=False)
    systemd_start: bool = field(default=False)

    def __init__(self, **kwargs):
        self.mode_configs = {
            BASIC_MODE: BasicMode(),
            "approach": ApproachMode(),
            "adaptive": AdaptiveMode(),
            "max": MaxMode(),
            "min": MinMode(),
            "multi": MultiMode(),
            "flavor": FlavorMode(),
            "default": DefaultMode(),
            "reactive": ReactiveMode(),
            "sequence": SequenceMode(),
        }

        # Apply overrides from kwargs
        for key, value in kwargs.items():
            key_found = False
            for name, mode in self.mode_configs.items():
                # If the key is a mode key, update the mode configuration
                if hasattr(mode, key):
                    setattr(mode, key, value)
                    key_found = True
            if hasattr(self, key):
                # If the key is a configuration attribute, update the attribute
                setattr(self, key, value)
                key_found = True

            if not key_found:
                logger.warning(f"Unknown configuration key: {key}")

        self.active_mode = self.get_mode_config(mode_name=self.active_mode_name)
        self.cluster_id = read_cluster_id()
        self._set_scaling_urls()
        self.log_active_mode_values()

    def _set_scaling_urls(self):
        self.portal_scaling_link = (
            self.portal_scaling_link
            if self.portal_scaling_link.endswith("/")
            else self.portal_scaling_link + "/"
        )
        self.portal_scaling_up_link = (
            self.portal_scaling_link + self.cluster_id + "/scale-up/"
        )
        self.portal_scaling_down_specific_link = (
            self.portal_scaling_link + self.cluster_id + "/scale-down/specific/"
        )
        self.portal_cluster_info_link = (
            self.portal_scaling_link + self.cluster_id + "/scale-data/"
        )
        self.portal_flavor_infos_link = (
            self.portal_scaling_link + self.cluster_id + "/usable_flavors/"
        )

    def get_mode_config(self, mode_name: str) -> BasicMode:
        """Retrieves the configuration for the specified mode.
        If the mode is not found, it logs a message and returns the BasicMode as the default.
        """
        if mode_name not in self.mode_configs:
            logger.warning(f"Mode '{mode_name}' not found. Using BasicMode as default.")
        else:
            logger.info(f"Using Mode: {mode_name}")
        return self.mode_configs.get(mode_name, BasicMode())

    def log_active_mode_values(self):
        active_mode_dict = self.active_mode.__dict__
        logger.info(f"Active Settings\n\n: {active_mode_dict}")
