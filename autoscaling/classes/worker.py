from dataclasses import dataclass, field

import utils
from classes.flavor import Flavor
from constants import (
    NODE_DOWN,
    WORKER_ACTIVE,
    WORKER_CREATION_FAILED,
    WORKER_ERROR,
    WORKER_FAILED,
)
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


@dataclass
class Worker:
    hostname: str
    flavor: Flavor
    status: str
    task_state: str
    memory_usable: int = field(init=False)

    def __post_init__(self):
        self.status == self.status.upper()
        self.memory_usable = utils.reduce_flavor_memory(self.flavor.ram_gib)

    def is_down(self):
        return NODE_DOWN in self.status

    def is_in_error(self):
        return WORKER_ERROR == self.status

    def is_creation_failed(self):
        return WORKER_CREATION_FAILED == self.status

    def is_failed(self):
        return WORKER_FAILED == self.status

    def is_failed(self):
        return WORKER_ACTIVE == self.status
