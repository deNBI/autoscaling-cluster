from dataclasses import dataclass
from typing import Any, Optional

from constants import JOB_FINISHED, JOB_PENDING, JOB_RUNNING
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


@dataclass
class JobInfo:
    job_id: int
    req_cpus: int
    req_mem: int
    state: int
    state_str: str
    temporary_disk: int = 0
    priority: int
    name: str
    req_gres: Any  # type as needed; adjust to str, dict, etc.
    nodes: Any  # type as needed; adjust to str, list, etc.
    elapsed_time: int
    end_time: Optional[int]
    comment: Optional[str] = None

    def is_finished(self) -> bool:
        return self.state == JOB_FINISHED

    def is_pending(self) -> bool:
        return self.state == JOB_PENDING

    def is_running(self) -> bool:
        return self.state == JOB_RUNNING

    def print_job_data(self):
        logger.info(
            "id: %s - jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s - elapsed %s",
            self.job_id,
            self.name,
            self.req_mem,
            self.req_cpus,
            self.priority,
            self.temporary_disk,
            self.elapsed_time,
        )
