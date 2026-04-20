"""
Data fetchers for scheduler and cluster data.
"""

import logging
import time
from typing import Any, Optional

from autoscaling.data.models import Job, Worker
from autoscaling.scheduler.base import SchedulerInterface
from autoscaling.utils.helpers import (
    JOB_FINISHED,
    JOB_PENDING,
    JOB_RUNNING,
    NODE_ALLOCATED,
    NODE_DOWN,
    NODE_DRAIN,
    NODE_DUMMY,
    NODE_DUMMY_REQ,
    NODE_IDLE,
    NODE_MIX,
)

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    Fetches data from scheduler and cluster API.
    """

    def __init__(self, scheduler: SchedulerInterface):
        """
        Initialize data fetcher.

        Args:
            scheduler: Scheduler interface instance
        """
        self.scheduler = scheduler
        self.ignore_workers: list[str] = []

    def set_ignore_workers(self, workers: list[str]) -> None:
        """Set workers to ignore in data fetching."""
        self.ignore_workers = workers
        logger.debug("Set ignore_workers: %s", workers)

    def fetch_node_data_live(self) -> dict[str, Worker]:
        """
        Query node data from scheduler without database.

        Returns:
            Dictionary of workers keyed by hostname
        """
        node_dict = self.scheduler.node_data_live()
        if node_dict is None:
            node_dict = {}
        return self._parse_workers(node_dict)

    def fetch_node_data_db(
        self, quiet: bool = False
    ) -> tuple[Optional[dict[str, Worker]], int, list[str], list[str], list[str]]:
        """
        Query node data from scheduler database.

        Args:
            quiet: Suppress logging

        Returns:
            Tuple of (workers dict, count, in_use, drain, drain_idle)
        """
        node_dict = self.scheduler.fetch_scheduler_node_data()
        return self._parse_node_stats(node_dict, quiet)

    def fetch_job_data(self) -> tuple[dict[int, Job], dict[int, Job]]:
        """
        Fetch current job data from scheduler.

        Returns:
            Tuple of (pending_jobs, running_jobs)
        """
        jobs_pending: dict[int, Job] = {}
        jobs_running: dict[int, Job] = {}

        job_live_dict = self.scheduler.job_data_live()
        if not job_live_dict:
            # Retry for logging issues
            time.sleep(2)
            job_live_dict = self.scheduler.job_data_live()

        if job_live_dict:
            for j_key, j_val in job_live_dict.items():
                job = Job.from_dict(int(j_key), j_val)
                if job.state == JOB_PENDING:
                    jobs_pending[j_key] = job
                elif job.state == JOB_RUNNING:
                    jobs_running[j_key] = job

        return jobs_pending, jobs_running

    def fetch_completed_jobs(self, days: int) -> dict[int, Job]:
        """
        Fetch completed jobs from the last x days.

        Args:
            days: Number of days to look back

        Returns:
            Dictionary of completed jobs
        """
        jobs_dict = self.scheduler.fetch_scheduler_job_data(days)

        if jobs_dict:
            # Filter for finished jobs only
            jobs_dict = {
                k: v for k, v in jobs_dict.items() if v.get("state") == JOB_FINISHED
            }
            # Sort by end time
            jobs_dict = dict(
                sorted(
                    jobs_dict.items(), key=lambda k: k[1].get("end", 0), reverse=False
                )
            )

        return {k: Job.from_dict(k, v) for k, v in jobs_dict.items()}

    def _parse_workers(self, node_dict: dict[str, Any]) -> dict[str, Worker]:
        """
        Parse node dictionary into Worker objects.

        Args:
            node_dict: Raw node data from scheduler

        Returns:
            Dictionary of Worker objects
        """
        workers: dict[str, Worker] = {}

        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            logger.error("%s found, but dummy mode is not active", NODE_DUMMY)

        for key, value in list(node_dict.items()):
            if self.ignore_workers and key in self.ignore_workers:
                del node_dict[key]
                continue

            if "worker" in key:
                workers[key] = Worker.from_dict(key, value)
            else:
                del node_dict[key]

        return workers

    def _parse_node_stats(
        self, node_dict: Optional[dict[str, Any]], quiet: bool
    ) -> tuple[Optional[dict[str, Worker]], int, list[str], list[str], list[str]]:
        """
        Parse node dictionary and return statistics.

        Args:
            node_dict: Raw node data
            quiet: Suppress logging

        Returns:
            Tuple of (workers, count, in_use, drain, drain_idle)
        """
        if node_dict is None:
            return None, 0, [], [], []

        worker_in_use: list[str] = []
        worker_count = 0
        worker_drain: list[str] = []
        worker_drain_idle: list[str] = []

        if not quiet:
            logger.debug("Node dict: %s", node_dict)

        for key, value in list(node_dict.items()):
            if "worker" not in key:
                continue

            worker_count += 1

            # Track drain status
            if NODE_DRAIN in value.get("state", ""):
                worker_drain.append(key)
            if NODE_DRAIN in value.get("state", "") and NODE_IDLE in value.get(
                "state", ""
            ):
                worker_drain_idle.append(key)

            # Track in-use status
            state = value.get("state", "")
            if NODE_ALLOCATED in state or NODE_MIX in state:
                worker_in_use.append(key)

            if NODE_DOWN in state and NODE_IDLE not in state:
                logger.error("Worker %s is in DOWN state", key)

        if not quiet:
            logger.info(
                "Found %d workers: %d allocated/mix, %d drain, %d drain+idle",
                worker_count,
                len(worker_in_use),
                len(worker_drain),
                len(worker_drain_idle),
            )

        workers = self._parse_workers(node_dict)
        return workers, worker_count, worker_in_use, worker_drain, worker_drain_idle

    def filter_workers(
        self, cluster_workers: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Filter out ignored workers from cluster data.

        Args:
            cluster_workers: List of worker data from API

        Returns:
            Filtered list of worker data
        """
        if not self.ignore_workers:
            return cluster_workers

        filtered = []
        for worker in cluster_workers:
            if worker.get("hostname") not in self.ignore_workers:
                filtered.append(worker)
            else:
                logger.debug("Removing ignored worker: %s", worker.get("hostname"))

        if filtered:
            logger.warning("Ignoring workers: %s", self.ignore_workers)
        return filtered
