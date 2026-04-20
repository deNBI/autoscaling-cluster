"""
Scaling engine - main scaling logic.
"""

import logging
import time
from typing import Any, Optional

from autoscaling.core.state import Rescale, ScaleState
from autoscaling.data.models import Job, Worker
from autoscaling.forecasting.predictor import JobPredictor
from autoscaling.utils.helpers import (
    DOWNSCALE_LIMIT,
    NODE_DRAIN,
    NODE_IDLE,
    WAIT_CLUSTER_SCALING,
)

logger = logging.getLogger(__name__)


class ScalingEngine:
    """
    Main scaling engine that orchestrates upscaling and downscaling.
    """

    def __init__(
        self,
        config: dict[str, Any],
        scheduler: Any,
        api_client: Any,
        fetcher: Any,
        predictor: JobPredictor,
    ):
        """
        Initialize scaling engine.

        Args:
            config: Configuration dictionary
            scheduler: Scheduler interface
            api_client: Portal API client
            fetcher: Data fetcher
            predictor: Job predictor
        """
        self.config = config
        self.scheduler = scheduler
        self.api_client = api_client
        self.fetcher = fetcher
        self.predictor = predictor

        # Get mode-specific configuration
        self.mode_config = config.get("mode", {}).get(
            config.get("active_mode", "basic"), {}
        )
        self.scale_force = float(self.mode_config.get("scale_force", 0.6))
        self.scale_delay = int(self.mode_config.get("scale_delay", 60))
        self.worker_cool_down = int(self.mode_config.get("worker_cool_down", 60))
        self.smoothing_coefficient = float(
            self.mode_config.get("smoothing_coefficient", 0.0)
        )
        self.forecast_by_flavor_history = self.mode_config.get(
            "forecast_by_flavor_history", False
        )
        self.forecast_by_job_history = self.mode_config.get(
            "forecast_by_job_history", False
        )
        self.forecast_active_worker = int(
            self.mode_config.get("forecast_active_worker", 0)
        )
        self.job_time_threshold = float(self.mode_config.get("job_time_threshold", 0.5))
        self.flavor_depth = int(self.mode_config.get("flavor_depth", -1))
        self.drain_large_nodes = self.mode_config.get("drain_large_nodes", False)
        self.drain_only_hmf = self.mode_config.get("drain_only_hmf", False)
        self.drain_delay = int(self.mode_config.get("drain_delay", 0))

    def multiscale(self, flavor_data: list[dict[str, Any]]) -> None:
        """
        Main scaling loop - checks cluster state and performs scaling actions.

        Args:
            flavor_data: List of available flavors
        """
        state = ScaleState.SKIP
        worker_copy = None
        cluster_workers = None
        changed_data = False
        scale_down_value = 0
        drained = False

        cluster_data = self.api_client.get_cluster_data()

        while state not in (ScaleState.SKIP, ScaleState.DONE):
            jobs_pending_dict, jobs_running_dict = self.fetcher.fetch_job_data()
            jobs_pending = len(jobs_pending_dict)

            (
                worker_json,
                worker_count,
                worker_in_use_list,
                worker_drain,
                _,
            ) = self.fetcher.fetch_node_data_db(quiet=True)

            if not worker_json:
                logger.warning("Unable to receive worker data, reconfiguring cluster")
                self._rescale_cluster(0)
                return

            worker_in_use = len(worker_in_use_list)
            worker_free = worker_count - worker_in_use

            logger.info(
                "worker_count: %d, worker_in_use: %d, worker_idle: %d, jobs_pending: %d",
                worker_count,
                worker_in_use,
                worker_free,
                jobs_pending,
            )

            # STATE: DELAY
            if state == ScaleState.DELAY:
                cluster_workers = self._verify_cluster_workers(
                    cluster_data, worker_json
                )
                if cluster_workers is None:
                    state = ScaleState.SKIP
                    logger.error("Unable to receive cluster workers")
                    continue

                if self.drain_large_nodes and jobs_pending > 0 and not drained:
                    if self._set_nodes_to_drain(
                        jobs_pending_dict,
                        worker_json,
                        flavor_data,
                        cluster_workers,
                        jobs_running_dict,
                    ):
                        cluster_data = self.api_client.get_cluster_data()
                        drained = True
                        continue

            # Create worker copy for state comparison
            worker_copy = self._worker_same_states(worker_json, worker_copy)

            # SCALE DOWN: All workers free, no pending jobs
            if (
                worker_count > DOWNSCALE_LIMIT
                and worker_in_use == 0
                and jobs_pending == 0
            ):
                logger.info("SCALE DOWN - No workers in use")
                state = self._multiscale_scale_down(
                    state, worker_copy, worker_count, worker_free, jobs_pending_dict
                )

            # SCALE DOWN: Some workers free, no pending jobs
            elif (
                worker_count > DOWNSCALE_LIMIT
                and worker_free >= 1
                and worker_in_use > 0
                and jobs_pending == 0
            ):
                logger.info("SCALE DOWN - Workers free, zero jobs pending")
                state = self._multiscale_scale_down(
                    state, worker_copy, worker_count, worker_free, jobs_pending_dict
                )

            # SCALE DOWN_UP: Workers idle but pending jobs
            elif worker_free > 0 and jobs_pending >= 1 and state != ScaleState.FORCE_UP:
                if state == ScaleState.DOWN_UP:
                    logger.info("SCALE DOWN_UP - Cluster scale down")
                    scale_down_value = self._calculate_scale_down_value(
                        worker_count, worker_free, state
                    )
                    changed_data = self._cluster_scale_down_specific(
                        worker_copy, scale_down_value, Rescale.NONE, jobs_pending_dict
                    )
                    if not changed_data:
                        scale_down_value = 0
                    state = ScaleState.FORCE_UP
                elif state == ScaleState.DELAY:
                    logger.debug("SCALE DOWN_UP - SCALE_DELAY")
                    state = ScaleState.DOWN_UP
                    time.sleep(self.scale_delay)
                else:
                    state = ScaleState.SKIP
                    logger.info("SCALE DOWN_UP - Condition changed, skip")

            # SCALE UP: All workers busy with pending jobs
            elif (
                worker_count == worker_in_use and jobs_pending >= 1
            ) or state == ScaleState.FORCE_UP:
                if state == ScaleState.DELAY:
                    state = ScaleState.UP
                    time.sleep(self.scale_delay)
                elif state == ScaleState.UP:
                    self._cluster_scale_up(
                        jobs_pending_dict,
                        jobs_running_dict,
                        worker_count,
                        worker_json,
                        worker_drain,
                        flavor_data,
                        cluster_workers,
                    )
                    state = ScaleState.DONE
                elif state == ScaleState.FORCE_UP:
                    if (
                        not self._cluster_scale_up(
                            jobs_pending_dict,
                            jobs_running_dict,
                            worker_count - scale_down_value,
                            worker_json,
                            worker_drain,
                            flavor_data,
                            cluster_workers,
                        )
                        and changed_data
                    ):
                        self._rescale_cluster(worker_count - scale_down_value)
                    state = ScaleState.DONE
                else:
                    logger.info("SCALE UP - Condition changed, skip")
                    state = ScaleState.SKIP
            else:
                if state in (ScaleState.UP, ScaleState.DOWN):
                    logger.info("Scaling condition changed - skip")
                state = ScaleState.SKIP

            logger.info("Scaling state: %s", state.name)

    def _cluster_scale_up(
        self,
        jobs_pending_dict: dict[int, Job],
        jobs_running_dict: dict[int, Job],
        worker_count: int,
        worker_json: dict[str, Worker],
        worker_drain: list[str],
        flavor_data: list[dict[str, Any]],
        cluster_worker: list[dict[str, Any]],
    ) -> bool:
        """
        Execute cluster scale-up.

        Args:
            jobs_pending_dict: Pending jobs
            jobs_running_dict: Running jobs
            worker_count: Current worker count
            worker_json: Worker data
            worker_drain: Workers in drain state
            flavor_data: Flavor data
            cluster_worker: Cluster worker data

        Returns:
            True on success
        """
        job_priority = self._sort_jobs(jobs_pending_dict)
        flavor_depth_list = self._classify_jobs_to_flavors(job_priority, flavor_data)

        if not flavor_depth_list:
            return False

        flavor_index_cnt = 0
        upscale_cnt = 0
        flavors_started_cnt = 0
        worker_memory_usage = self._get_worker_memory_usage(worker_json)
        worker_claimed = {}

        skip_starts = False
        while flavor_index_cnt < len(flavor_depth_list):
            jobs_pending_flavor, flavor_next, flavor_job_list = flavor_depth_list[
                flavor_index_cnt
            ]

            if not flavor_next:
                flavor_index_cnt += 1
                continue

            if (
                flavors_started_cnt > 0 and self.flavor_depth == -3
            ):  # DEPTH_MULTI_SINGLE
                skip_starts = True

            # Wait for flavor data update if workers started
            if skip_starts is False and upscale_cnt > 0:
                time.sleep(WAIT_CLUSTER_SCALING)
                flavor_data = self.api_client.get_usable_flavors(True, True)

            data_tmp, worker_memory_usage = self._calculate_scale_up_data(
                flavor_job_list,
                jobs_pending_flavor,
                worker_count,
                worker_json,
                worker_drain,
                flavor_data,
                flavor_next,
                flavor_index_cnt,
                worker_memory_usage,
                jobs_running_dict,
                cluster_worker,
                flavors_started_cnt,
                len(flavor_depth_list) - flavor_index_cnt - 1,
                worker_claimed,
            )

            if data_tmp:
                if self.api_client.scale_up(data_tmp):
                    worker_count += data_tmp["upscale_count"]
                    upscale_cnt += data_tmp["upscale_count"]
                    flavors_started_cnt += 1
                    logger.info(
                        "Started %d workers for flavor %s",
                        data_tmp["upscale_count"],
                        flavor_next["flavor"]["name"],
                    )

            flavor_index_cnt += 1

        if upscale_cnt > 0:
            self._rescale_cluster(worker_count)

        return upscale_cnt > 0

    def _cluster_scale_down_specific(
        self,
        worker_json: dict[str, Worker],
        worker_num: int,
        rescale: Rescale,
        jobs_dict: dict[int, Job],
    ) -> bool:
        """
        Scale down specific number of workers.

        Args:
            worker_json: Worker data
            worker_num: Number of workers to remove
            rescale: Rescaling mode
            jobs_dict: Jobs dictionary

        Returns:
            True on success
        """
        worker_list = self._generate_downscale_list(worker_json, worker_num, jobs_dict)
        return self._cluster_scale_down_specific_self_check(worker_list, rescale)

    def _rescale_cluster(self, worker_count: int) -> bool:
        """
        Apply new worker configuration via rescale.

        Args:
            worker_count: Expected worker count

        Returns:
            True on success
        """
        if worker_count == 0:
            time.sleep(WAIT_CLUSTER_SCALING)

        no_error_scale, cluster_data = self._check_workers(Rescale.NONE, worker_count)
        rescale_success = self._rescale_init()

        if no_error_scale and rescale_success:
            return True
        return False

    def _rescale_init(self) -> bool:
        """
        Initialize rescale - update playbook and run.

        Returns:
            True on success
        """
        # Update playbook files
        scaling_script_url = self.config.get("scaling_script_url", "")
        self._update_scaling_script(scaling_script_url)

        # Run ansible playbook
        return self._run_ansible_playbook()

    def _run_ansible_playbook(self) -> bool:
        """
        Run ansible playbook.

        Returns:
            True on success
        """
        import os
        import subprocess

        playbook_dir = "/root/playbook"
        if not os.path.exists(playbook_dir):
            logger.error("Playbook directory not found: %s", playbook_dir)
            return False

        os.chdir(playbook_dir)
        forks_num = str(os.cpu_count() * 4)

        try:
            process = subprocess.Popen(
                [
                    "ansible-playbook",
                    "--forks",
                    forks_num,
                    "-v",
                    "-i",
                    "ansible_hosts",
                    "site.yml",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )

            for line in process.stdout:
                logger.debug("[ANSIBLE] %s", line.strip())

            return_code = process.wait()
            return return_code == 0

        except Exception as e:
            logger.error("Error running ansible-playbook: %s", e)
            return False

    def _update_scaling_script(self, url: str, filename: str = None) -> bool:
        """
        Update scaling script from URL.

        Args:
            url: Script URL
            filename: Target filename

        Returns:
            True on success
        """
        if filename is None:
            filename = "/root/playbook/scaling.py"

        try:
            import requests

            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(filename, "w") as f:
                    f.write(response.text)
                logger.debug("Updated scaling script")
                return True
        except Exception as e:
            logger.error("Error updating scaling script: %s", e)

        return False

    def _set_nodes_to_drain(
        self,
        jobs_pending_dict: dict[int, Job],
        worker_json: dict[str, Worker],
        flavor_data: list[dict[str, Any]],
        cluster_workers: list[dict[str, Any]],
        jobs_running_dict: dict[int, Job],
    ) -> bool:
        """
        Set appropriate nodes to drain state.

        Args:
            jobs_pending_dict: Pending jobs
            worker_json: Worker data
            flavor_data: Flavor data
            cluster_workers: Cluster worker data
            jobs_running_dict: Running jobs

        Returns:
            True if nodes were drained
        """

        worker_drain = []
        nodes_resume = 0

        for w_key, w_value in worker_json.items():
            if NODE_DRAIN in w_value.state:
                if NODE_IDLE in w_value.state:
                    # Worker is drain+idle, keep drain
                    worker_drain.append(w_value.hostname)
                else:
                    # Worker is allocated+drain, resume it
                    self.scheduler.set_node_to_resume(w_key)
                    nodes_resume += 1

            elif NODE_IDLE in w_value.state:
                # Check if idle worker should be drained
                if self._drain_worker_check(
                    w_key, w_value, flavor_data, cluster_workers, jobs_running_dict
                ):
                    worker_drain.append(w_key)
                    logger.debug("Worker %s marked for drain", w_key)

        if nodes_resume > 0:
            logger.info("Resumed %d nodes", nodes_resume)
        if worker_drain:
            for w_key in worker_drain:
                self.scheduler.set_node_to_drain(w_key)
            logger.info("Drained %d nodes", len(worker_drain))

        return len(worker_drain) > 0 or nodes_resume > 0

    def _verify_cluster_workers(
        self,
        cluster_data: dict[str, Any],
        worker_json: dict[str, Worker],
    ) -> Optional[list[dict[str, Any]]]:
        """
        Verify cluster workers match scheduler data.

        Args:
            cluster_data: Cluster data from API
            worker_json: Worker data from scheduler

        Returns:
            List of cluster workers, or None on error
        """
        if cluster_data is None:
            return None

        cluster_workers = cluster_data.get("workers", [])
        return cluster_workers

    def _worker_same_states(
        self,
        worker_json: dict[str, Worker],
        worker_copy: Optional[dict[str, Worker]],
    ) -> dict[str, Worker]:
        """
        Create worker copy for state comparison.

        Args:
            worker_json: Current worker data
            worker_copy: Previous worker copy

        Returns:
            New worker copy
        """
        if worker_copy is None:
            return dict(worker_json)
        return dict(worker_json)

    def _multiscale_scale_down(
        self,
        state: ScaleState,
        worker_copy: dict[str, Worker],
        worker_count: int,
        worker_free: int,
        jobs_pending_dict: dict[int, Job],
    ) -> ScaleState:
        """
        Handle scale down logic.

        Args:
            state: Current scaling state
            worker_copy: Worker data copy
            worker_count: Total worker count
            worker_free: Free worker count
            jobs_pending_dict: Pending jobs

        Returns:
            New scaling state
        """
        scale_down_value = self._calculate_scale_down_value(
            worker_count, worker_free, state
        )

        if scale_down_value > 0:
            changed_data = self._cluster_scale_down_specific(
                worker_copy, scale_down_value, Rescale.NONE, jobs_pending_dict
            )
            if changed_data:
                return ScaleState.DELAY
        return ScaleState.SKIP

    def _calculate_scale_down_value(
        self,
        worker_count: int,
        worker_free: int,
        state: ScaleState,
    ) -> int:
        """
        Calculate number of workers to scale down.

        Args:
            worker_count: Total worker count
            worker_free: Free worker count
            state: Current scaling state

        Returns:
            Number of workers to scale down
        """
        if worker_count > DOWNSCALE_LIMIT:
            if worker_free < worker_count and state != ScaleState.DOWN_UP:
                return int(round(worker_free * self.scale_force))

            max_scale_down = worker_free - DOWNSCALE_LIMIT
            if max_scale_down > 0:
                return max_scale_down
        return 0

    def _sort_jobs(self, jobs_pending_dict: dict[int, Job]) -> list:
        """
        Sort pending jobs by resources or priority.

        Args:
            jobs_pending_dict: Pending jobs

        Returns:
            Sorted list of (job_id, job) tuples
        """
        if self.config.get("resource_sorting", False):
            # Sort by resources (memory, cpu, disk)
            return sorted(
                jobs_pending_dict.items(),
                key=lambda k: (k[1].req_mem, k[1].req_cpus, k[1].temporary_disk),
                reverse=True,
            )
        else:
            # Sort by priority
            return sorted(
                jobs_pending_dict.items(),
                key=lambda k: (
                    k[1].priority,
                    k[1].req_mem,
                    k[1].req_cpus,
                    k[1].temporary_disk,
                ),
                reverse=True,
            )

    def _classify_jobs_to_flavors(
        self,
        job_priority: list,
        flavor_data: list[dict[str, Any]],
    ) -> list:
        """
        Classify pending jobs by required flavor.

        Args:
            job_priority: Sorted job list
            flavor_data: Available flavors

        Returns:
            List of (count, flavor, job_list) tuples
        """
        from autoscaling.utils.converter import translate_metrics_to_flavor

        flavor_depth = []
        max_depth = int(self.mode_config.get("flavor_depth", -1))
        depth_limit = len(flavor_data)

        if max_depth > 0 and max_depth > depth_limit:
            max_depth = depth_limit

        if max_depth in (-1, -3):  # DEPTH_MULTI, DEPTH_MULTI_SINGLE
            depth_limit = len(flavor_data)
        elif max_depth == -2 or self.mode_config.get("flavor_default"):
            # Single flavor mode
            job_list = []
            flavor_next = None
            for key, value in job_priority:
                if value.is_pending():
                    fv_tmp = translate_metrics_to_flavor(
                        value.req_cpus,
                        value.req_mem,
                        value.temporary_disk,
                        flavor_data,
                        True,
                        False,
                    )
                    if fv_tmp:
                        if flavor_next is None:
                            flavor_next = fv_tmp
                        job_list.append((key, value))
            if job_list and flavor_next:
                return [(len(job_list), flavor_next, job_list)]
            return []

        # Multi-flavor classification
        counter = 0
        flavor_job_list = []
        flavor_next = None

        for key, value in job_priority:
            if value.is_pending():
                fv_tmp = translate_metrics_to_flavor(
                    value.req_cpus,
                    value.req_mem,
                    value.temporary_disk,
                    flavor_data,
                    True,
                    False,
                )

                if fv_tmp is None:
                    continue
                elif flavor_next is None:
                    flavor_next = fv_tmp
                    counter += 1
                    flavor_job_list.append((key, value))
                elif flavor_next["flavor"]["name"] == fv_tmp["flavor"]["name"]:
                    counter += 1
                    flavor_job_list.append((key, value))
                else:
                    if counter > 0 and flavor_next:
                        flavor_depth.append((counter, flavor_next, flavor_job_list))
                    if len(flavor_depth) >= max_depth:
                        break
                    counter = 1
                    flavor_next = fv_tmp
                    flavor_job_list = [(key, value)]

        if flavor_job_list and flavor_next:
            flavor_depth.append((counter, flavor_next, flavor_job_list))

        return flavor_depth

    def _get_worker_memory_usage(self, worker_json: dict[str, Worker]) -> dict:
        """
        Calculate memory usage per worker.

        Args:
            worker_json: Worker data

        Returns:
            Dictionary of memory usage per worker
        """
        return {w.hostname: w.real_memory for w in worker_json.values()}

    def _calculate_scale_up_data(
        self,
        flavor_job_list: list,
        jobs_pending_flavor: int,
        worker_count: int,
        worker_json: dict[str, Worker],
        worker_drain: list[str],
        flavor_data: list[dict[str, Any]],
        flavor_next: dict[str, Any],
        flavor_index_cnt: int,
        worker_memory_usage: dict,
        jobs_running_dict: dict[int, Job],
        cluster_worker: list[dict[str, Any]],
        flavors_started_cnt: int,
        remaining_flavors: int,
        worker_claimed: dict,
    ) -> tuple[Optional[dict[str, Any]], dict]:
        """
        Calculate scale up data for a flavor.

        Args:
            flavor_job_list: Jobs for this flavor
            jobs_pending_flavor: Number of pending jobs
            worker_count: Current worker count
            worker_json: Worker data
            worker_drain: Drained workers
            flavor_data: All available flavors
            flavor_next: Next flavor to use
            flavor_index_cnt: Current flavor index
            worker_memory_usage: Memory usage per worker
            jobs_running_dict: Running jobs
            cluster_worker: Cluster worker data
            flavors_started_cnt: Number of flavors started
            remaining_flavors: Remaining flavors to process
            worker_claimed: Claimed workers

        Returns:
            Tuple of (scale up data, updated memory usage)
        """

        # Calculate required resources
        sum(j.req_cpus for _, j in flavor_job_list)
        sum(j.req_mem for _, j in flavor_job_list)
        sum(j.temporary_disk for _, j in flavor_job_list)

        # Get flavor specs
        fv = flavor_next["flavor"]
        flavor_cpu = fv["vcpus"]
        flavor_mem = flavor_next["available_memory"]
        flavor_disk = fv.get("temporary_disk", 0)

        # Calculate how many workers needed
        workers_needed = 0
        for job_id, job in flavor_job_list:
            cpu_fit = flavor_cpu // job.req_cpus if job.req_cpus > 0 else 0
            mem_fit = flavor_mem // job.req_mem if job.req_mem > 0 else 0
            disk_fit = (
                flavor_disk // job.temporary_disk if job.temporary_disk > 0 else 0
            )

            if cpu_fit > 0 and mem_fit > 0:
                jobs_per_worker = (
                    min(cpu_fit, mem_fit, disk_fit)
                    if disk_fit > 0
                    else min(cpu_fit, mem_fit)
                )
                if jobs_per_worker > 0:
                    workers_needed = (
                        len(flavor_job_list) + jobs_per_worker - 1
                    ) // jobs_per_worker
                break

        # Apply scale force limit
        max_new_workers = max(1, int(len(flavor_job_list) * self.scale_force))

        # Limit by worker weight
        if self.scale_force > 0:
            active_workers = len([w for w in worker_json.values() if w.is_in_use()])
            weight_factor = 1.0 - min(active_workers * 0.02, 0.9)
            max_new_workers = max(1, int(max_new_workers * weight_factor))

        upscale_count = min(workers_needed, max_new_workers, 10)  # Max 10 per iteration

        if upscale_count <= 0:
            return None, worker_memory_usage

        data = {
            "worker_flavor_name": fv["name"],
            "upscale_count": upscale_count,
        }

        return data, worker_memory_usage

    def _check_workers(
        self,
        rescale: Rescale,
        worker_count: int,
    ) -> tuple[bool, Optional[dict[str, Any]]]:
        """
        Check worker status and wait for all to be active.

        Args:
            rescale: Rescaling mode
            worker_count: Expected worker count

        Returns:
            Tuple of (success, cluster_data)
        """
        import time

        max_wait_time = 1200
        wait_interval = 10
        elapsed = 0

        while elapsed < max_wait_time:
            cluster_data = self.api_client.get_cluster_data(
                self.api_client._get_cluster_password()
            )
            if cluster_data is None:
                time.sleep(wait_interval)
                elapsed += wait_interval
                continue

            cluster_workers = cluster_data.get("workers", [])
            error_workers = [
                w
                for w in cluster_workers
                if w.get("status", "").upper() in ("ERROR", "CREATION_FAILED", "FAILED")
            ]

            if len(cluster_workers) >= worker_count and len(error_workers) == 0:
                return True, cluster_data

            # Handle error workers
            if error_workers:
                hostnames = [w["hostname"] for w in error_workers]
                self._cluster_scale_down_specific_hostnames_list(hostnames, rescale)
                worker_count -= len(error_workers)

            time.sleep(wait_interval)
            elapsed += wait_interval

        return False, cluster_data

    def _cluster_scale_down_specific_worker(
        self,
        worker_json: dict[str, Worker],
        worker_num: int,
        jobs_dict: dict[int, Job],
    ) -> list[str]:
        """
        Generate list of workers to scale down.

        Args:
            worker_json: Worker data
            worker_num: Number of workers to remove
            jobs_dict: Jobs dictionary

        Returns:
            List of worker hostnames to remove
        """
        worker_remove = []
        worker_idle = []
        worker_cool_down = int(self.mode_config.get("worker_cool_down", 0))

        if worker_num == 0:
            return worker_remove

        # Sort by resources (low memory first)
        worker_mem_sort = sorted(
            worker_json.items(), key=lambda k: k[1].real_memory, reverse=False
        )

        for key, value in worker_mem_sort:
            if "worker" in key and worker_num > 0:
                state = value.state
                if "DRAIN" in state and "IDLE" in state:
                    worker_remove.append(key)
                    worker_num -= 1
                elif "IDLE" in state and "ALLOC" not in state and "MIX" not in state:
                    worker_idle.append(key)
                    worker_num -= 1

        # Check cool down
        if worker_cool_down > 0 and worker_idle:
            jobs_history = self.fetcher.fetch_completed_jobs(1)
            time_now = int(time.time())
            for key in list(worker_idle):
                for job_id, job_data in jobs_history.items():
                    elapsed = float(time_now) - float(job_data.get("end", 0))
                    if key in job_data.get("nodes", ""):
                        if elapsed < worker_cool_down:
                            worker_idle.remove(key)
                        break

        # Check if idle workers can handle pending jobs
        if worker_idle and jobs_dict:
            worker_useless = []
            for key in worker_idle:
                worker_useless.append(key)

            for job_id, job in jobs_dict.items():
                found_match = False
                for key in worker_useless:
                    if key in worker_json:
                        w = worker_json[key]
                        if (
                            job.req_mem <= w.real_memory
                            and job.req_cpus <= w.total_cpus
                            and (
                                job.temporary_disk == 0
                                or job.temporary_disk < w.temporary_disk
                            )
                        ):
                            found_match = True
                            break
                if not found_match:
                    worker_useless = [w for w in worker_useless if w != key]

            worker_idle = [w for w in worker_idle if w in worker_useless]

        worker_remove.extend(worker_idle)
        return worker_remove

    def _cluster_scale_down_specific_self_check(
        self, worker_hostnames: list[str], rescale: Rescale
    ) -> bool:
        """
        Scale down with final worker check.

        Args:
            worker_hostnames: List of worker hostnames
            rescale: Rescaling mode

        Returns:
            True on success
        """
        if not worker_hostnames:
            return False

        time.sleep(WAIT_CLUSTER_SCALING)

        worker_json, _, _, worker_drain_idle, _ = self.fetcher.fetch_node_data_db(False)
        scale_down_list = []

        for wb in worker_hostnames:
            if wb in worker_json:
                if wb not in worker_drain_idle:
                    state = worker_json[wb].state
                    if "ALLOC" in state or "MIX" in state:
                        logger.debug("Worker allocated, rescue: %s", wb)
                    else:
                        scale_down_list.append(wb)
                else:
                    scale_down_list.append(wb)
            else:
                scale_down_list.append(wb)

        return self._cluster_scale_down_specific_hostnames_list(
            scale_down_list, rescale
        )

    def _cluster_scale_down_specific_hostnames_list(
        self, worker_hostnames: list[str], rescale: Rescale
    ) -> bool:
        """
        Scale down specific hostnames.

        Args:
            worker_hostnames: List of hostnames
            rescale: Rescaling mode

        Returns:
            True on success
        """
        if not worker_hostnames:
            return False

        try:
            password = self.api_client._get_cluster_password()
        except Exception:
            logger.error("Failed to get cluster password")
            return False

        result = self.api_client.scale_down_specific(worker_hostnames, password)
        if result:
            if rescale == Rescale.CHECK:
                self._rescale_cluster(0)
            elif rescale == Rescale.INIT:
                self._rescale_init()
            return True
        return False

    def _drain_worker_check(
        self,
        worker_key: str,
        worker_value: Worker,
        flavor_data: list[dict[str, Any]],
        cluster_workers: list[dict[str, Any]],
        jobs_running_dict: dict[int, Job],
    ) -> bool:
        """
        Check if worker should be drained.

        Args:
            worker_key: Worker key/hostname
            worker_value: Worker data
            flavor_data: Flavor data
            cluster_workers: Cluster worker data
            jobs_running_dict: Running jobs

        Returns:
            True if worker should be drained
        """
        wait_time = self.drain_delay

        if wait_time > 0:
            # Check if worker is currently running a job
            for job in jobs_running_dict.values():
                if worker_key in job.nodes:
                    # Worker is busy, don't drain
                    return False

            # Check recent job history
            jobs_history = self.fetcher.fetch_completed_jobs(1)
            time_now = int(time.time())

            for job_id, job_data in jobs_history.items():
                elapsed_time = float(time_now) - float(job_data.get("end", 0))
                if worker_key in job_data.get("nodes", ""):
                    if elapsed_time < wait_time:
                        # Worker was recently used, don't drain yet
                        return False
                    break

        return True
