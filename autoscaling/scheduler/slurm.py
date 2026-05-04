"""
Slurm scheduler implementation for autoscaling.
"""

import re
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from typing import Optional

from autoscaling.scheduler.interface import (
    SchedulerInterface,
    SchedulerJobState,
    SchedulerNodeState,
)


class SlurmScheduler(SchedulerInterface):
    """
    Scheduler implementation for Slurm.
    """

    # Node state mappings
    NODE_ALLOCATED = "ALLOC"
    NODE_MIX = "MIX"
    NODE_IDLE = "IDLE"
    NODE_DRAIN = "DRAIN"
    NODE_DOWN = "DOWN"

    # Job state mappings
    JOB_PENDING = 0
    JOB_RUNNING = 1
    JOB_COMPLETED = 3

    def __init__(self):
        """Initialize the Slurm scheduler."""
        self._logger = None

    def set_logger(self, logger):
        """Set the logger instance."""
        self._logger = logger

    def _log_debug(self, msg: str):
        """Log a debug message."""
        if self._logger:
            self._logger.debug(msg)

    def _log_error(self, msg: str):
        """Log an error message."""
        if self._logger:
            self._logger.error(msg)

    def test_connection(self) -> bool:
        """
        Test if scheduler connection is working.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            import pyslurm

            # Test node data
            try:
                pyslurm.Nodes.load()
            except ValueError as e:
                self._log_error(f"Unable to receive node data: {e}")
                return False

            # Test job data
            try:
                pyslurm.db.Jobs.load()
            except ValueError as e:
                self._log_error(f"Unable to receive job data: {e}")
                return False

            return True
        except ImportError:
            self._log_error("pyslurm module not available")
            return False

    def get_node_data(self) -> Optional[dict[str, SchedulerNodeState]]:
        """
        Fetch node data from the Slurm database.

        Returns:
            Dictionary mapping node names to their states, or None on error
        """
        try:
            import pyslurm

            nodes = pyslurm.Nodes.load()
            node_dict = {}

            for key, value in nodes.items():
                node_data = value.to_dict()

                state = self._convert_node_state(node_data.get("state", "UNKNOWN"))

                node_dict[key] = SchedulerNodeState(
                    hostname=key,
                    state=state,
                    total_cpus=node_data.get("cpus", 0),
                    free_memory=node_data.get("free_mem", 0),
                    real_memory=node_data.get("real_memory", 0),
                    temporary_disk=node_data.get("tmp_disk", 0),
                    gres=node_data.get("gres", ["gpu:0"]),
                    node_hostname=key,
                )

            self._log_debug(f"Node data: {node_dict}")
            return node_dict
        except ValueError as e:
            self._log_error(f"Unable to receive node data: {e}")
            return None
        except ImportError:
            self._log_error("pyslurm module not available")
            return None

    def get_job_data(self, days: int) -> Optional[dict[int, SchedulerJobState]]:
        """
        Fetch job data from the Slurm database.

        Args:
            days: Number of days of history to fetch

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        start_time = (__utc_datetime() - __timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")
        end_time = (__utc_datetime() + __timedelta(days=days)).strftime("%Y-%m-%dT00:00:00")

        return self.get_job_data_by_range(start_time, end_time)

    def get_node_data_live(self) -> Optional[dict[str, SchedulerNodeState]]:
        """
        Get live node data from sinfo command.

        Returns:
            Dictionary mapping node names to their states, or None on error
        """
        try:
            sinfo_output = self._run_sinfo()
            return self._parse_node_data(sinfo_output)
        except Exception as e:
            self._log_error(f"Error getting live node data: {e}")
            return None

    def get_job_data_live(self) -> Optional[dict[int, SchedulerJobState]]:
        """
        Get live job data from squeue command.

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        try:
            squeue_output = self._run_squeue()
            return self._parse_job_data(squeue_output)
        except Exception as e:
            self._log_error(f"Error getting live job data: {e}")
            return None

    def drain_node(self, node_name: str) -> bool:
        """
        Set a node to drain state.

        Args:
            node_name: Name of the node to drain

        Returns:
            True if successful, False otherwise
        """
        self._log_debug(f"Draining node {node_name}")
        cmd = ["sudo", "scontrol", "update", f"nodename={node_name}", "state=drain", "reason=REPLACE"]
        result = self._run_command(cmd)
        return result == 0

    def resume_node(self, node_name: str) -> bool:
        """
        Remove drain state from a node.

        Args:
            node_name: Name of the node to resume

        Returns:
            True if successful, False otherwise
        """
        self._log_debug(f"Resuming node {node_name}")
        cmd = ["sudo", "scontrol", "update", f"nodename={node_name}", "state=resume"]
        result = self._run_command(cmd)
        return result == 0

    def get_job_data_by_range(self, start_time: str, end_time: str) -> Optional[dict[int, SchedulerJobState]]:
        """
        Fetch job data within a specific time range.

        Args:
            start_time: Start time in format "YYYY-MM-DDTHH:MM:SS"
            end_time: End time in format "YYYY-MM-DDTHH:MM:SS"

        Returns:
            Dictionary mapping job IDs to their states, or None on error
        """
        try:
            import pyslurm

            db_filter = pyslurm.db.JobFilter(start_time=start_time, end_time=end_time)
            jobs = pyslurm.db.Jobs.load(db_filter)

            job_dict = {}
            for job in jobs.values():
                job_dict[job.id] = SchedulerJobState(
                    jobid=job.id,
                    state=self._convert_job_state(job.state),
                    state_str=job.state,
                    req_cpus=job.req_cpus,
                    req_mem=job.req_mem,
                    temporary_disk=job.temporary_disk or 0,
                    priority=job.priority,
                    jobname=job.jobname,
                    nodes=job.nodes,
                    elapsed=job.elapsed,
                    comment=job.comment,
                )

            return job_dict
        except ValueError as e:
            self._log_error(f"Unable to receive job data: {e}")
            return None
        except ImportError:
            self._log_error("pyslurm module not available")
            return None

    # --- Helper Methods ---

    def _run_sinfo(self) -> str:
        """Run sinfo command and return output."""
        process = Popen(
            ["sinfo", "-o", "%all"],
            stdout=PIPE,
            stderr=PIPE,
            universal_newlines=True,
        )
        stdout, _ = process.communicate()
        return stdout

    def _run_squeue(self) -> str:
        """Run squeue command and return output."""
        process = Popen(
            ["squeue", "-o", "%all"],
            stdout=PIPE,
            stderr=PIPE,
            universal_newlines=True,
        )
        stdout, _ = process.communicate()
        return stdout

    def _run_command(self, cmd: list[str]) -> int:
        """Run a shell command and return the exit code."""
        process = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        _, _ = process.communicate()
        return process.returncode

    def _parse_node_data(self, output: str) -> dict[str, SchedulerNodeState]:
        """Parse sinfo output into node data dictionary."""
        lines = output.strip().split("\n")
        if len(lines) < 2:
            return {}

        header = lines[0].split("|")
        node_dict = {}

        for line in lines[1:]:
            values = line.split("|")
            if len(values) != len(header):
                continue

            data = dict(zip(header, values))

            state = self._convert_node_state(data.get("STATE", ""))

            free_mem = 0
            free_mem_str = data.get("FREE_MEM", "0")
            if free_mem_str.isnumeric():
                free_mem = int(free_mem_str)

            gres = data.get("GRES", "null").replace(" ", "")
            gres_list: list[str] = ["gpu:0"] if "null" in gres else [gres]

            node_dict[data["HOSTNAMES"]] = SchedulerNodeState(
                hostname=data["HOSTNAMES"],
                state=state,
                total_cpus=int(data.get("CPUS", 0)),
                free_memory=free_mem,
                real_memory=int(data.get("MEMORY", 0)),
                temporary_disk=int(data.get("TMP_DISK", 0)),
                gres=gres_list,
                node_hostname=data["HOSTNAMES"],
            )

        return node_dict

    def _parse_job_data(self, output: str) -> dict[int, SchedulerJobState]:
        """Parse squeue output into job data dictionary."""
        lines = output.strip().split("\n")
        if len(lines) < 2:
            return {}

        header = lines[0].split("|")
        job_dict = {}

        for line in lines[1:]:
            values = line.split("|")
            if len(values) != len(header):
                continue

            data = dict(zip(header, values))

            job_id = int(data.get("JOBID", 0))

            job_dict[job_id] = SchedulerJobState(
                jobid=job_id,
                state=self._convert_job_state(data.get("STATE", "")),
                state_str=data.get("STATE", ""),
                req_cpus=int(data.get("MIN_CPUS", 0)),
                req_mem=self._parse_memory(data.get("MIN_MEMORY", "0M")),
                temporary_disk=self._parse_memory(data.get("MIN_TMP_DISK", "0M")),
                priority=int(data.get("PRIORITY", 0)),
                jobname=data.get("NAME", ""),
                nodes=data.get("NODELIST", ""),
                elapsed=self._parse_time(data.get("TIME", "0")),
                comment=None if data.get("COMMENT") == "(null)" else data.get("COMMENT"),
            )

        return job_dict

    def _convert_node_state(self, state: str) -> str:
        """Convert sinfo state to internal format."""
        state = state.upper()
        if "DRNG" in state:
            return f"{self.NODE_ALLOCATED}+{self.NODE_DRAIN}"
        elif "DRAIN" in state:
            return f"{self.NODE_IDLE}+{self.NODE_DRAIN}"
        elif "ALLOC" in state:
            return self.NODE_ALLOCATED
        elif "IDLE" in state:
            return self.NODE_IDLE
        elif "MIX" in state:
            return self.NODE_MIX
        elif "DOWN" in state:
            return self.NODE_DOWN
        return state

    def _convert_job_state(self, state: str) -> int:
        """Convert job state string to internal ID."""
        state = state.upper()
        _state_map = {
            "PENDING": self.JOB_PENDING,
            "RUNNING": self.JOB_RUNNING,
            "COMPLETED": self.JOB_COMPLETED,
        }
        return _state_map.get(state, -1)

    def _parse_memory(self, mem_str: str) -> int:
        """Parse memory string to MB."""
        match = re.match(r"(\d+)([KMGTP]?)", mem_str.upper())
        if not match:
            return 0

        value = int(match.group(1))
        unit = match.group(2)

        multipliers = {
            "K": 1,  # KB to MB (divide by 1024, but we use approximate)
            "M": 1,
            "G": 1024,
            "T": 1024 * 1024,
            "P": 1024 * 1024 * 1024,
        }

        return value * multipliers.get(unit, 1)

    def _parse_time(self, time_str: str) -> int:
        """Parse time string to seconds."""
        # Format: "DD-HH:MM:SS" or "HH:MM:SS" or "MM:SS"
        total_seconds = 0

        try:
            parts = time_str.split("-")
            if len(parts) > 1:
                # Has days
                total_seconds += int(parts[0]) * 86400
                time_str = parts[1]

            time_parts = time_str.split(":")
            if len(time_parts) == 3:
                # HH:MM:SS
                total_seconds += int(time_parts[0]) * 3600
                total_seconds += int(time_parts[1]) * 60
                total_seconds += int(time_parts[2])
            elif len(time_parts) == 2:
                # MM:SS
                total_seconds += int(time_parts[0]) * 60
                total_seconds += int(time_parts[1])
        except (ValueError, IndexError):
            pass

        return total_seconds


def __utc_datetime() -> datetime:
    """Get current UTC datetime."""
    return datetime.utcnow()


def __timedelta(**kwargs) -> timedelta:
    """Create a timedelta."""
    return timedelta(**kwargs)
