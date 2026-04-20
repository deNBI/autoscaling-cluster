"""
Slurm scheduler interface implementation.
"""

import logging
import os
import re
from subprocess import PIPE, Popen
from typing import Any, Optional

from autoscaling.scheduler.base import SchedulerInterface
from autoscaling.utils.helpers import (
    JOB_PENDING,
    JOB_RUNNING,
    NODE_ALLOCATED,
    NODE_DOWN,
    NODE_DRAIN,
    NODE_IDLE,
    NODE_MIX,
)

logger = logging.getLogger(__name__)


class SlurmInterface(SchedulerInterface):
    """
    Interface for the Slurm scheduler.
    """

    def __init__(self):
        """Initialize the Slurm interface."""
        self._pyslurm = None
        self._db = None

    def _import_pyslurm(self) -> bool:
        """Import pyslurm module."""
        if self._pyslurm is not None:
            return True

        try:
            import pyslurm

            self._pyslurm = pyslurm
            self._db = pyslurm.db
            return True
        except ImportError:
            logger.error("pyslurm module not available")
            return False

    def scheduler_function_test(self) -> bool:
        """
        Test if scheduler can retrieve job and node data.

        Returns:
            True if successful, False otherwise
        """
        if not self._import_pyslurm():
            return False

        # Test node data
        try:
            self._pyslurm.Nodes.load()
        except ValueError as e:
            logger.error("Error: unable to receive node data: %s", e)
            return False

        # Test job data
        try:
            self._db.Jobs.load()
        except ValueError as e:
            logger.error("Error: unable to receive job data: %s", e)
            return False

        return True

    def fetch_scheduler_node_data(self) -> Optional[dict[str, Any]]:
        """
        Read scheduler data from database and return node data.

        Returns:
            Dictionary with node data, or None on error
        """
        if not self._import_pyslurm():
            return None

        try:
            nodes = self._pyslurm.Nodes.load()
            node_dict = {}
            for key, value in nodes.items():
                node_dict[key] = value.to_dict()
            logger.debug("Fetched %d nodes from Slurm", len(node_dict))
            return node_dict
        except ValueError as e:
            logger.error("Error: unable to receive node data: %s", e)
            return None

    @staticmethod
    def get_json(slurm_command: str) -> list:
        """
        Get slurm data as JSON from squeue or sinfo.

        Args:
            slurm_command: Command to run (sinfo or squeue)

        Returns:
            List of dictionaries with command output
        """
        process = Popen(
            [slurm_command, "-o", "%all"],
            stdout=PIPE,
            stderr=PIPE,
            shell=False,
            universal_newlines=True,
        )
        proc_stdout, proc_stderr = process.communicate()

        lines = proc_stdout.split("\n")
        if not lines:
            return []

        header_line = lines.pop(0)
        header_cols = header_line.split("|")

        entries = []
        for line in lines:
            parts = line.split("|")
            if len(parts) == len(header_cols):
                val_dict = dict(zip(header_cols, parts))
                entries.append(val_dict)

        return entries

    @staticmethod
    def _convert_node_state(state: str) -> str:
        """
        Convert sinfo state to detailed database version.

        Args:
            state: Raw state from sinfo

        Returns:
            Converted state string
        """
        state = state.upper()

        if "DRNG" in state:
            return NODE_ALLOCATED + "+" + NODE_DRAIN
        elif "DRAIN" in state:
            return NODE_IDLE + "+" + NODE_DRAIN
        elif "ALLOC" in state:
            return NODE_ALLOCATED
        elif "IDLE" in state:
            return NODE_IDLE
        elif "MIX" in state:
            return NODE_MIX
        elif "DOWN" in state:
            return NODE_DOWN

        return state

    @staticmethod
    def _convert_job_state(state: str) -> int:
        """
        Convert job state string to id.

        Args:
            state: Raw job state string

        Returns:
            State ID (0=PENDING, 1=RUNNING, 3=FINISHED)
        """
        state = state.upper()

        if state == "PENDING":
            return JOB_PENDING
        elif state == "RUNNING":
            return JOB_RUNNING
        elif state == "COMPLETED":
            return 3  # JOB_FINISHED

        return -1

    def node_data_live(self) -> Optional[dict[str, Any]]:
        """
        Receive node data from sinfo.

        Returns:
            Dictionary with node data
        """
        node_dict = self.get_json("sinfo")
        node_dict_format = {}

        for node in node_dict:
            gres = node.get("GRES ", "").replace(" ", "")
            free_mem = None

            if "null" in gres:
                gpu = ["gpu:0"]
            else:
                gpu = [gres]

            if node.get("FREE_MEM", "").isnumeric():
                free_mem = int(node["FREE_MEM"])

            node_dict_format.update(
                {
                    node["HOSTNAMES"]: {
                        "total_cpus": int(node["CPUS"]),
                        "real_memory": int(node["MEMORY"]),
                        "state": self._convert_node_state(node["STATE"]),
                        "temporary_disk": int(node["TMP_DISK"]),
                        "node_hostname": node["HOSTNAMES"],
                        "gres": gpu,
                        "free_mem": free_mem,
                    }
                }
            )

        return node_dict_format

    @staticmethod
    def _time_to_seconds(time_str: str) -> int:
        """
        Convert a time string to seconds.

        Args:
            time_str: Time string like "1-04:27:12"

        Returns:
            Time in seconds
        """
        value = 0
        ftr = [60, 1]

        try:
            time_split = time_str.split("-")
            if len(time_split) > 1:
                value += int(time_split[0]) * 3600 * 24
                time_split = time_split[1]
            else:
                time_split = time_split[0]

            time_split = time_split.split(":")
            if len(time_split) == 3:
                ftr = [3600, 60, 1]
            value += sum(a * b for a, b in zip(ftr, map(int, time_split)))
        except (ValueError, IndexError) as e:
            logger.error("Error parsing time data: %s", e)

        return value

    def job_data_live(self) -> Optional[dict[str, Any]]:
        """
        Fetch live data from scheduler without database usage.

        Returns:
            Dictionary with job data
        """
        squeue_json = self.get_json("squeue")
        job_live_dict = {}

        for job in squeue_json:
            mem_parts = re.split(r"(\d+)", job.get("MIN_MEMORY", ""))
            disk_parts = re.split(r"(\d+)", job.get("MIN_TMP_DISK", ""))
            comment = None if job.get("COMMENT") == "(null)" else job.get("COMMENT")

            state_id = self._convert_job_state(job.get("STATE", ""))

            if state_id >= 0:
                job_live_dict.update(
                    {
                        job["JOBID"]: {
                            "jobid": int(job["JOBID"]),
                            "req_cpus": int(job["MIN_CPUS"]),
                            "req_mem": self._memory_size_ib(mem_parts[1], mem_parts[2]),
                            "state": state_id,
                            "state_str": job["STATE"],
                            "temporary_disk": self._memory_size(
                                disk_parts[1], disk_parts[2]
                            ),
                            "priority": int(job["PRIORITY"]),
                            "jobname": job["NAME"],
                            "req_gres": job.get("TRES_PER_NODE"),
                            "nodes": job.get("NODELIST"),
                            "elapsed": self._time_to_seconds(job.get("TIME", "0")),
                            "comment": comment,
                        }
                    }
                )

        return job_live_dict

    @staticmethod
    def _memory_size_ib(num: str, ending: str) -> int:
        """Convert memory size to MiB."""
        if ending == "G":
            return int(num) * 1024
        elif ending == "T":
            return int(num) * 1000 * 1024
        return int(num)

    @staticmethod
    def _memory_size(num: str, ending: str) -> int:
        """Convert memory size to MB."""
        if ending == "G":
            return int(num) * 1000
        elif ending == "T":
            return int(num) * 1000000
        return int(num)

    def add_jobs_tmp_disk(self, jobs_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Add tmp disk data value to job dictionary.

        Args:
            jobs_dict: Jobs dictionary

        Returns:
            Updated jobs dictionary
        """
        for key, entry in jobs_dict.items():
            if "temporary_disk" not in entry:
                jobs_dict[key]["temporary_disk"] = 0
        return jobs_dict

    def fetch_scheduler_job_data_by_range(
        self, start: str, end: str
    ) -> Optional[dict[str, Any]]:
        """
        Read scheduler data from database within time range.

        Args:
            start: Start time in format "YYYY-MM-DDTHH:MM:SS"
            end: End time in format "YYYY-MM-DDTHH:MM:SS"

        Returns:
            Dictionary with job data, or None on error
        """
        if not self._import_pyslurm():
            return None

        try:
            db_filter = self._db.JobFilter(
                start_time=start.encode("utf-8"),
                end_time=end.encode("utf-8"),
            )
            jobs_dict = {
                job.id: job.to_dict() for job in self._db.Jobs.load(db_filter).values()
            }
            jobs_dict = self.add_jobs_tmp_disk(jobs_dict)
            return jobs_dict
        except ValueError as e:
            logger.debug("Error: unable to receive job data: %s", e)
            return None

    def fetch_scheduler_job_data(self, num_days: int) -> Optional[dict[str, Any]]:
        """
        Read job data from database.

        Args:
            num_days: Number of days to look back

        Returns:
            Dictionary with job data
        """
        import datetime

        start = (
            datetime.datetime.utcnow() - datetime.timedelta(days=num_days)
        ).strftime("%Y-%m-%dT00:00:00")

        end = (datetime.datetime.utcnow() + datetime.timedelta(days=num_days)).strftime(
            "%Y-%m-%dT00:00:00"
        )

        return self.fetch_scheduler_job_data_by_range(start, end)

    def set_node_to_drain(self, w_key: str) -> None:
        """
        Set a node to drain state.

        Args:
            w_key: Node name/hostname
        """
        logger.debug("Draining node %s", w_key)
        os.system(f"sudo scontrol update nodename={w_key} state=drain reason=REPLACE")

    def set_node_to_resume(self, w_key: str) -> None:
        """
        Resume a drained node.

        Args:
            w_key: Node name/hostname
        """
        logger.debug("Resuming node %s", w_key)
        os.system(f"sudo scontrol update nodename={w_key} state=resume")
