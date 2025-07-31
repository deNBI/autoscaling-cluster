import logging
from dataclasses import dataclass, field

from config_modes import (
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

logger = logging.getLogger(__name__)


# TODO helper function wich provides info for all config fields
@dataclass
class Configuration:
    portal_scaling_link: str = field(
        default="https://simplevm.denbi.de/portal/api/autoscaling"
    )
    portal_webapp_link: str = field(
        default="https://simplevm.denbi.de/portal/webapp/#/clusters/overview"
    )
    scaling_script_url: str = field(
        default="https://raw.githubusercontent.com/deNBI/user_scripts/refs/heads/master/bibigrid/scaling.py"
    )
    scheduler: str = field(default="slurm")

    active_mode_name: str = field(default="basic")
    automatic_update: bool = field(default=True)
    database_reset: bool = field(default=False)
    pattern_id: str = field(default="")
    history_recall: int = field(default=7)
    ignore_workers: list[str] = field(default_factory=list)
    pending_jobs_percent: float = field(default=1.0)

    def __post_init__(self):
        self.mode_configs = {
            "basic": BasicMode(),
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
        self.active_mode = self.get_mode_config(mode_name=self.active_mode_name)

    def get_mode_config(self, mode_name: str) -> BasicMode:
        """Retrieves the configuration for the specified mode.
        If the mode is not found, it logs a message and returns the BasicMode as the default.
        """
        if mode_name not in self.mode_configs:
            logger.warning(f"Mode '{mode_name}' not found. Using BasicMode as default.")
        return self.mode_configs.get(mode_name, BasicMode())
