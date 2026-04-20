"""
Scale-down logic for the scaling engine.
"""

import logging
from typing import Any, Optional

from autoscaling.core.state import ScaleState
from autoscaling.utils.helpers import DOWNSCALE_LIMIT

logger = logging.getLogger(__name__)


class DownscaleCalculator:
    """
    Calculates scale-down actions.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize downscale calculator.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.mode_config = config.get("mode", {}).get(
            config.get("active_mode", "basic"), {}
        )

    def calculate_scale_down_value(
        self, worker_count: int, worker_free: int, state: ScaleState
    ) -> int:
        """
        Calculate number of workers to scale down.

        Args:
            worker_count: Total worker count
            worker_free: Number of free (idle) workers
            state: Current scaling state

        Returns:
            Number of workers to scale down
        """
        scale_force = float(self.mode_config.get("scale_force", 0.6))

        if worker_count > DOWNSCALE_LIMIT:
            if worker_free < worker_count and state != ScaleState.DOWN_UP:
                return round(worker_free * scale_force, 0)

            max_scale_down = worker_free - DOWNSCALE_LIMIT
            if max_scale_down > 0:
                return max_scale_down

        return 0

    def generate_downscale_list(
        self,
        worker_json: dict[str, Any],
        count: int,
        jobs_dict: Optional[dict[int, Any]],
    ) -> list[str]:
        """
        Generate list of workers to scale down.

        Args:
            worker_json: Worker data dictionary
            count: Number of workers to remove
            jobs_dict: Current jobs (for checking capability)

        Returns:
            List of worker hostnames to scale down
        """
        logger.debug("Generate downscale list: need to remove %d workers", count)

        worker_remove = []
        worker_idle = []

        # Sort workers by state for prioritized removal
        for w_key, w_value in worker_json.items():
            state = w_value.get("state", "")

            # Skip non-worker nodes
            if "worker" not in w_key:
                continue

            # Skip workers in unknown states
            if not any(s in state for s in ["ALLOC", "MIX", "IDLE"]):
                logger.error("Worker %s is in unknown state: %s", w_key, state)
                worker_remove.append(w_key)
                continue

            # Track idle workers
            if "IDLE" in state:
                worker_idle.append(w_key)
            elif "ALLOC" in state or "MIX" in state:
                # Allocated or mixed - not suitable for immediate removal
                continue
            elif "DRAIN" in state:
                worker_remove.append(w_key)

        # Apply cool-down to idle workers
        if self.mode_config.get("worker_cool_down", 0) > 0 and worker_idle:
            worker_idle = self._scale_frequency_worker_check(worker_idle)

        # Check if idle workers can be safely removed
        if worker_idle and jobs_dict:
            _, _, _, worker_useless = self._current_workers_capable(
                jobs_dict.items(), worker_json, worker_idle
            )
            worker_idle = [x for x in worker_idle if x in worker_useless]

        # Merge lists
        worker_remove.extend(worker_idle)
        logger.debug("Workers to remove: %s", worker_remove)

        return worker_remove

    def _scale_frequency_worker_check(self, worker_useless: list[str]) -> list[str]:
        """
        Check cool-down for workers to prevent rapid scaling.

        Args:
            worker_useless: List of idle workers

        Returns:
            Filtered list of workers safe to remove
        """
        cool_down = int(self.mode_config.get("worker_cool_down", 60))
        if cool_down <= 0:
            return worker_useless

        # Get job history for cool-down checking
        from autoscaling.config.loader import load_config
        from autoscaling.data.fetcher import DataFetcher

        try:
            load_config()
            fetcher = DataFetcher(None)  # Will be set in real usage
            jobs_history = fetcher.fetch_completed_jobs(1)
        except Exception:
            return worker_useless

        time_now = int(__import__("time").time())
        worker_prevent = []

        for worker in worker_useless:
            for job_id, job_data in jobs_history.items():
                if worker in job_data.get("nodes", ""):
                    elapsed = float(time_now) - float(job_data.get("end", 0))
                    if elapsed < cool_down:
                        worker_prevent.append(worker)
                    break

        logger.debug("Prevent workers from removal (cool-down): %s", worker_prevent)
        return [w for w in worker_useless if w not in worker_prevent]

    def _current_workers_capable(
        self,
        jobs_pending: Any,
        worker_json: dict[str, Any],
        worker_to_check: Optional[list[str]],
    ) -> tuple:
        """
        Check if workers can handle pending jobs.
        """
        worker_all = worker_to_check if worker_to_check else list(worker_json.keys())
        worker_useless = worker_all.copy()
        worker_useful = []

        for _, j_value in jobs_pending:
            if j_value.get("state") == 0:  # PENDING
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
                    break

        worker_useful = [x for x in worker_all if x not in worker_useless]
        return True, worker_all, worker_useful, worker_useless

    def _worker_match_to_job(
        self, j_value: dict[str, Any], w_value: dict[str, Any]
    ) -> bool:
        """Check if worker can handle job."""
        try:
            return int(j_value.get("req_mem", 0)) <= int(
                w_value.get("real_memory", 0)
            ) and int(j_value.get("req_cpus", 0)) <= int(w_value.get("total_cpus", 0))
        except (ValueError, TypeError):
            return False
