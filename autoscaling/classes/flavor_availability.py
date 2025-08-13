from dataclasses import dataclass, field
from typing import Optional

from flavor import Flavor
from logger import setup_custom_logger
from utils import reduce_flavor_memory

logger = setup_custom_logger(__name__)


@dataclass
class FlavorAvailability:
    flavor: Flavor
    used_count: int
    available_count: int
    real_available_count_openstack: int
    available_memory: int = field(init=False)
    custom_limit: int = 0

    def __post_init__(self):
        self.available_memory = reduce_flavor_memory(self.flavor.ram_gib)

    def set_custom_limit(self, custom_limit: int):
        self.custom_limit = custom_limit

    @property
    def usable_count(self) -> int:
        # Only include nonzero custom_limit if it's set, else ignore it in min()
        limits = [self.available_count, self.real_available_count_openstack]
        if self.custom_limit > 0:
            limits.append(self.custom_limit)
        return min(limits)

    def log_info(self):
        logger.info(
            "fv: %s, %s, available: %sx, ram: %s GB, cpu: %s, ephemeral: %sGB, credit: %s",
            self.flavor.name,
            self.usable_count,
            self.flavor.ram_gib,
            self.flavor.vcpus,
            self.flavor.ephemeral_disk,
            getattr(self.flavor, 'credits_costs_per_hour', 0)
        )
        logger.debug(
            " project available %sx, real available: %sx, real memory: %s, tmpDisk %s,  type: %s",
            self.available_count,
            self.real_available_count_openstack,
            self.available_memory,
            self.flavor.temporary_disk,
            getattr(getattr(self.flavor, 'type', None), 'shortcut', 'n/a')
