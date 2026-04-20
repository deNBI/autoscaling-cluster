"""
Scale-up logic for the scaling engine.
"""

import logging
from typing import Any, Optional

from autoscaling.core.state import ScaleState
from autoscaling.forecasting.predictor import JobPredictor

logger = logging.getLogger(__name__)


class UpscaleCalculator:
    """
    Calculates scale-up data for pending jobs.
    """

    def __init__(self, config: dict[str, Any], predictor: JobPredictor):
        """
        Initialize upscale calculator.

        Args:
            config: Configuration dictionary
            predictor: Job predictor instance
        """
        self.config = config
        self.predictor = predictor
        self.mode_config = config.get("mode", {}).get(
            config.get("active_mode", "basic"), {}
        )

    def calculate_scale_up_data(
        self,
        flavor_job_list: list[tuple[int, Any]],
        jobs_pending_flavor: int,
        worker_count: int,
        worker_json: dict[str, Any],
        worker_drain: list[str],
        state: ScaleState,
        flavor_data: list[dict[str, Any]],
        flavor_next: dict[str, Any],
        level: int,
        worker_memory_usage: int,
        jobs_running_dict: dict[int, Any],
        cluster_worker: list[dict[str, Any]],
        flavors_started_cnt: int,
        level_pending: int,
        worker_claimed: dict[str, Any],
    ) -> tuple[Optional[dict[str, Any]], int]:
        """
        Calculate scale-up data for pending jobs with a specific flavor.

        Args:
            flavor_job_list: Jobs matching this flavor
            jobs_pending_flavor: Number of pending jobs
            worker_count: Current worker count
            worker_json: Worker data
            worker_drain: Workers in drain state
            state: Current scaling state
            flavor_data: All available flavors
            flavor_next: Target flavor for scaling
            level: Current flavor depth level
            worker_memory_usage: Total worker memory usage
            jobs_running_dict: Running jobs
            cluster_worker: Cluster worker data
            flavors_started_cnt: Flavors already started
            level_pending: Remaining levels to process
            worker_claimed: Workers already claimed for scaling

        Returns:
            Tuple of (scale_up_data, worker_memory_usage)
        """
        logger.debug(
            "Calculating scale-up for flavor %s, level %d, pending %d jobs",
            flavor_next["flavor"]["name"],
            level,
            jobs_pending_flavor,
        )

        need_workers = False

        # Get worker capability information
        worker_capable, _, worker_useful, _ = self._current_workers_capable(
            flavor_job_list, worker_json, None
        )
        worker_usable = [x for x in worker_useful if x not in worker_drain]
        len(worker_usable)

        current_force = self._get_scale_force(flavor_next["flavor"]["name"])
        limit_memory = int(self.mode_config.get("limit_memory", 0))

        if flavor_next is None or not flavor_data:
            return None, worker_memory_usage

        logger.info(
            "Calculate worker for %s: pending %d jobs, worker active %d, capable %d, memory %d, need %s, state %s",
            flavor_next["flavor"]["name"],
            jobs_pending_flavor,
            worker_count,
            worker_capable,
            len(worker_usable),
            need_workers,
            state.name,
        )

        # Generate upscale limit
        upscale_limit = self._generate_upscale_limit(
            current_force, jobs_pending_flavor, worker_count, level
        )

        # Check if multiple jobs can run on single worker
        jobs_per_worker = self._multiple_jobs_per_flavor(
            flavor_next,
            self._get_average_job_resources(jobs_pending_flavor, flavor_job_list),
        )

        if jobs_per_worker > 1:
            upscale_limit = int(upscale_limit / jobs_per_worker)
            logger.debug(
                "Multiple jobs per worker: upscale_limit adjusted to %d", upscale_limit
            )

        # Adjust for worker memory usage
        if worker_memory_usage > 0 and limit_memory > 0:
            worker_memory_usage = worker_memory_usage - int(
                flavor_next["flavor"]["ram"] * 1000
            )

        scale_up_data = {
            "password": None,  # Will be added by API client
            "worker_flavor_name": flavor_next["flavor"]["name"],
            "upscale_count": upscale_limit,
            "version": "3.0.0",
        }

        logger.info(
            "Scale-up data for %s: %d workers",
            flavor_next["flavor"]["name"],
            upscale_limit,
        )

        return scale_up_data, worker_memory_usage

    def _get_scale_force(self, flavor_name: str) -> float:
        """Get scale force for a specific flavor."""
        if "flavor_force" in self.mode_config:
            return self.mode_config["flavor_force"].get(
                flavor_name, self.mode_config.get("scale_force", 0.6)
            )
        return self.mode_config.get("scale_force", 0.6)

    def _generate_upscale_limit(
        self,
        current_force: float,
        jobs_pending_flavor: int,
        worker_count: int,
        level: int,
    ) -> int:
        """Generate upscale limit based on force and jobs."""
        worker_weight = float(self.mode_config.get("worker_weight", 0))

        if jobs_pending_flavor == 0:
            return 0

        # Reduce starting workers based on current workers
        if worker_weight != 0:
            force = current_force - (worker_weight * worker_count)
            if force < 0.0001:
                force = 0.0001
        else:
            force = current_force

        upscale_limit = int(jobs_pending_flavor * force)
        logger.info("Calculated up-scale limit: %d for level %d", upscale_limit, level)

        return upscale_limit

    def _multiple_jobs_per_flavor(
        self, flavor_tmp: dict[str, Any], average_resources: dict[str, int]
    ) -> int:
        """Check how many jobs can run on a single worker."""
        if average_resources["cpu"] == 0:
            return 1

        jobs_cpu = flavor_tmp["flavor"]["vcpus"] / average_resources["cpu"]
        jobs_mem = flavor_tmp["available_memory"] / average_resources["memory"]

        return min(int(jobs_cpu), int(jobs_mem))

    def _get_average_job_resources(
        self, jobs_pending_flavor: int, flavor_job_list: list
    ) -> dict[str, int]:
        """Get average job resource requirements."""
        sum_cpu = 0
        sum_mem = 0
        sum_disk = 0

        for _, job in flavor_job_list:
            sum_cpu += job.get("req_cpus", 0)
            sum_mem += job.get("req_mem", 0)
            sum_disk += job.get("temporary_disk", 0)

        if jobs_pending_flavor > 0:
            return {
                "cpu": int(sum_cpu / jobs_pending_flavor),
                "memory": int(sum_mem / jobs_pending_flavor),
                "disk": int(sum_disk / jobs_pending_flavor),
            }

        return {"cpu": 0, "memory": 0, "disk": 0}

    def _current_workers_capable(
        self,
        jobs_pending_dict: list,
        worker_json: dict[str, Any],
        worker_to_check: Optional[list[str]],
    ) -> tuple[bool, list[str], list[str], list[str]]:
        """
        Check if workers are capable for pending jobs.

        Returns:
            Tuple of (capable, all_workers, useful_workers, useless_workers)
        """
        worker_all = worker_to_check if worker_to_check else list(worker_json.keys())
        worker_useless = worker_all.copy()
        worker_useful = []

        found_pending_jobs = False
        worker_not_capable_counter = 0

        for _, j_value in jobs_pending_dict:
            if j_value.get("state") == 0:  # JOB_PENDING
                found_pending_jobs = True
                found_match = False

                for w_key, w_value in worker_json.items():
                    if w_key not in worker_all:
                        continue
                    if "worker" not in w_key:
                        continue

                    if self._worker_match_to_job(j_value, w_value):
                        worker_useless = [w for w in worker_useless if w != w_key]
                        found_match = True
                        break

                if not found_match:
                    worker_not_capable_counter += 1

        worker_useful = [x for x in worker_all if x not in worker_useless]

        if found_pending_jobs and worker_not_capable_counter > 0:
            return False, worker_all, worker_useful, worker_useless

        return True, worker_all, worker_useful, worker_useless

    def _worker_match_to_job(
        self, j_value: dict[str, Any], w_value: dict[str, Any]
    ) -> bool:
        """Test if a worker matches a job's resource requirements."""
        try:
            w_mem = int(w_value.get("real_memory", 0))
            w_cpu = int(w_value.get("total_cpus", 0))
            w_disk = int(w_value.get("temporary_disk", 0) or 0)

            j_cpu = int(j_value.get("req_cpus", 0))
            j_mem = int(j_value.get("req_mem", 0))
            j_disk = int(j_value.get("temporary_disk", 0))

            return (
                j_mem <= w_mem and j_cpu <= w_cpu and (j_disk < w_disk or j_disk == 0)
            )
        except (ValueError, TypeError):
            return False
