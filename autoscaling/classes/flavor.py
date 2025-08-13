from dataclasses import dataclass, field

from utils import convert_gb_to_mib


@dataclass
class Flavor:
    name: str  # Name of the flavor
    ram: int  # RAM in MiB
    ram_gib: int  # RAM in GiB
    vcpus: int  # Number of vCPUs
    root_disk: int  # Root disk size
    gpu: int  # Number of GPUs
    ephemeral_disk: int  # Ephemeral disk size (GB)
    type_shortcut: str  # Shortcut from flavor type
    temporary_disk: int = field(init=False)  # Wird nachträglich befüllt

    def __post_init__(self):
        # Rechne ephemeral_disk (in GB) um und speichere als MiB
        self.temporary_disk = (
            convert_gb_to_mib(self.ephemeral_disk) if self.ephemeral_disk > 0 else 0
        )
