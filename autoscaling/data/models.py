"""
Data models for the autoscaling system.
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Flavor:
    """
    Represents a compute flavor.
    """

    name: str
    vcpus: int
    ram: int  # in MB
    ram_gib: int  # in GiB
    ephemeral_disk: int  # in MB
    ephemerals: list[dict[str, Any]] = field(default_factory=list)
    gpu: int = 0
    temporary_disk: int = 0
    usable_count: int = 0
    available_memory: int = 0
    credits_costs_per_hour: float = 0.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Flavor":
        """Create Flavor from dictionary."""
        flavor_data = data.get("flavor", {})
        # Map ephemeral_disk to temporary_disk for compatibility
        ephemeral_disk = flavor_data.get("ephemeral_disk", 0)
        temporary_disk = flavor_data.get("temporary_disk", ephemeral_disk)
        return cls(
            name=flavor_data.get("name", ""),
            vcpus=flavor_data.get("vcpus", 0),
            ram=flavor_data.get("ram", 0),
            ram_gib=flavor_data.get("ram_gib", 0),
            ephemeral_disk=ephemeral_disk,
            ephemerals=flavor_data.get("ephemerals", []),
            gpu=flavor_data.get("gpu", 0),
            temporary_disk=temporary_disk,
            usable_count=data.get("usable_count", 0),
            available_memory=data.get("available_memory", 0),
            credits_costs_per_hour=flavor_data.get("credits_costs_per_hour", 0.0),
        )

    def fits_job(self, cpu: int, mem: int, disk: int, ephemeral_enabled: bool) -> bool:
        """
        Check if this flavor can fit a job with given requirements.

        Args:
            cpu: Required CPUs
            mem: Required memory in MB
            disk: Required disk in MB
            ephemeral_enabled: Whether ephemeral disks are required

        Returns:
            True if flavor fits job requirements
        """
        if ephemeral_enabled and self.ephemeral_disk == 0:
            return False

        return (
            cpu <= self.vcpus
            and mem <= self.available_memory
            and (
                disk <= self.temporary_disk or (disk == 0 and self.temporary_disk >= 0)
            )
        )


@dataclass
class Worker:
    """
    Represents a compute worker node.
    """

    hostname: str
    total_cpus: int
    real_memory: int  # in MB
    state: str
    temporary_disk: int = 0
    node_hostname: str = ""
    gres: list[str] = field(default_factory=list)
    free_mem: Optional[int] = None

    @classmethod
    def from_dict(cls, hostname: str, data: dict[str, Any]) -> "Worker":
        """Create Worker from dictionary."""
        return cls(
            hostname=hostname,
            total_cpus=data.get("total_cpus", 0),
            real_memory=data.get("real_memory", 0),
            state=data.get("state", ""),
            temporary_disk=data.get("temporary_disk", 0),
            node_hostname=data.get("node_hostname", hostname),
            gres=data.get("gres", []),
            free_mem=data.get("free_mem"),
        )

    def is_idle(self) -> bool:
        """Check if worker is in idle state."""
        return "IDLE" in self.state

    def is_allocated(self) -> bool:
        """Check if worker is in allocated state."""
        return "ALLOC" in self.state

    def is_mix(self) -> bool:
        """Check if worker is in mixed state."""
        return "MIX" in self.state

    def is_drain(self) -> bool:
        """Check if worker is in drain state."""
        return "DRAIN" in self.state

    def is_down(self) -> bool:
        """Check if worker is in down state."""
        return "DOWN" in self.state

    def is_in_use(self) -> bool:
        """Check if worker is currently in use."""
        return self.is_allocated() or self.is_mix()


@dataclass
class Job:
    """
    Represents a scheduler job.
    """

    jobid: int
    jobname: str
    state: int  # JOB_PENDING, JOB_RUNNING, JOB_FINISHED
    state_str: str
    req_cpus: int
    req_mem: int  # in MB
    req_gres: Optional[str] = None
    nodes: Optional[str] = None
    priority: int = 0
    temporary_disk: int = 0
    elapsed: int = 0
    comment: Optional[str] = None
    cluster: str = ""
    partition: str = ""
    tmp_disk: int = 0

    @classmethod
    def from_dict(cls, jobid: int, data: dict[str, Any]) -> "Job":
        """Create Job from dictionary."""
        return cls(
            jobid=jobid,
            jobname=data.get("jobname", ""),
            state=data.get("state", 0),
            state_str=data.get("state_str", ""),
            req_cpus=data.get("req_cpus", 0),
            req_mem=data.get("req_mem", 0),
            req_gres=data.get("req_gres"),
            nodes=data.get("nodes"),
            priority=data.get("priority", 0),
            temporary_disk=data.get("temporary_disk", 0),
            elapsed=data.get("elapsed", 0),
            comment=data.get("comment"),
            cluster=data.get("cluster", ""),
            partition=data.get("partition", ""),
            tmp_disk=data.get("tmp_disk", 0),
        )

    def is_pending(self) -> bool:
        """Check if job is pending."""
        return self.state == 0

    def is_running(self) -> bool:
        """Check if job is running."""
        return self.state == 1

    def is_finished(self) -> bool:
        """Check if job is finished."""
        return self.state == 3


@dataclass
class ClusterWorker:
    """
    Represents a worker from the cluster API.
    """

    hostname: str
    flavor: dict[str, Any]
    status: str
    ip: str = ""
    gpu: int = 0
    memory_usable: int = 0
    temporary_disk: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClusterWorker":
        """Create ClusterWorker from dictionary."""
        return cls(
            hostname=data.get("hostname", ""),
            flavor=data.get("flavor", {}),
            status=data.get("status", ""),
            ip=data.get("ip", ""),
            gpu=data.get("gpu", 0),
            memory_usable=data.get("memory_usable", 0),
            temporary_disk=data.get("temporary_disk", 0),
        )

    def is_active(self) -> bool:
        """Check if worker is active."""
        return self.status.upper() == "ACTIVE"

    def is_error(self) -> bool:
        """Check if worker is in error state."""
        return self.status.upper() in ("ERROR", "CREATION_FAILED", "FAILED")

    def is_down(self) -> bool:
        """Check if worker is down."""
        return "DOWN" in self.status.upper()
