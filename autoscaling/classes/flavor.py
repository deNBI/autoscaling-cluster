from dataclasses import dataclass, field

from logger import setup_custom_logger
from utils import convert_gb_to_mib

logger = setup_custom_logger(__name__)


@dataclass
class FlavorType:
    shortcut: str


@dataclass
class Flavor:
    name: str  # Name of the flavor
    ram: int  # RAM in MiB
    ram_gib: int  # RAM in GiB
    vcpus: int  # Number of vCPUs
    root_disk: int  # Root disk size
    gpu: int  # Number of GPUs
    ephemeral_disk: int  # Ephemeral disk size (GB)
    temporary_disk: int = field(init=False)  # Wird nachträglich befüllt
    type = FlavorType

    def __post_init__(self):
        # Rechne ephemeral_disk (in GB) um und speichere als MiB
        self.temporary_disk = (
            convert_gb_to_mib(self.ephemeral_disk) if self.ephemeral_disk > 0 else 0
        )

    def log_info(self, prefix="flavor"):
        logger.info(
            "%s: %s, ram: %s GB, cpu: %s, ephemeral: %sGB, credit: %s, id: %s, type: %s",
            prefix,
            self.name,
            self.ram_gib,
            self.vcpus,
            self.ephemeral_disk,
            getattr(self, "credits_costs_per_hour", 0),
            getattr(self, "id", "n/a"),
            getattr(getattr(self, "type", None), "shortcut", "n/a"),
        )
