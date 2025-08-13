from dataclasses import dataclass, field

from flavor import Flavor
from utils import reduce_flavor_memory


@dataclass
class FlavorAvailability:
    flavor: Flavor
    used_count: int
    available_count: int
    real_available_count_openstack: int
    available_memory: int = field(init=False)

    def __post_init__(self):
        # Nutzt flavor.ram_gib (Stelle sicher, dass Flavor dieses Feld hat!)
        self.available_memory = reduce_flavor_memory(self.flavor.ram_gib)
