"""
Resource watcher for autoscaling.
Monitors cluster resources and logs metrics.
"""

import csv
import os
import time
from multiprocessing import Process
from typing import Optional

from autoscaling.cloud.client import PortalClient
from autoscaling.data.manager import DatabaseManager
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.utils.logging import setup_logger


class ResourceWatcher:
    """
    Watcher for cluster resources.
    Monitors worker and job resources, writes CSV logs.
    """

    def __init__(
        self,
        scheduler: SchedulerInterface,
        config: dict,
        csv_file: str = "autoscaling.csv",
        log_file: str = "autoscaling_csv.log",
    ):
        """
        Initialize the resource watcher.

        Args:
            scheduler: Scheduler interface implementation
            config: Configuration dictionary
            csv_file: Path to CSV log file
            log_file: Path to log file
        """
        self.scheduler = scheduler
        self.config = config
        self.csv_file = csv_file
        self.log_file = log_file

        # Get scaling settings
        scaling_config = config.get("scaling", {})
        self.service_frequency = scaling_config.get("service_frequency", 60)

        # Create components
        self.portal_client = PortalClient(
            api_url=scaling_config.get("portal_scaling_link", ""),
            webapp_url=scaling_config.get("portal_webapp_link", ""),
            password_file="cluster_pw.json",
        )
        self.db_manager = DatabaseManager(scaling_config.get("database_file", "autoscaling_database.json"))

        # Setup logger
        self._logger = setup_logger(log_file)

        # CSV header
        self.header = [
            "time",
            "scale",
            "worker_cnt",
            "worker_cnt_use",
            "worker_cnt_free",
            "cpu_alloc",
            "cpu_idle",
            "mem_alloc",
            "mem_idle",
            "jobs_cnt_running",
            "job_cnt_pending",
            "jobs_cpu_alloc",
            "jobs_cpu_pending",
            "jobs_mem_alloc",
            "jobs_mem_pending",
            "worker_mem_alloc_drain",
            "worker_mem_alloc",
            "worker_mem_idle_drain",
            "worker_mem_idle",
            "worker_mem_mix_drain",
            "worker_mem_mix",
            "worker_drain_cnt",
            "w_change",
            "reason",
            "credit",
            "cluster_worker",
            "worker_credit_data",
        ]

    def run(self) -> None:
        """
        Run the watcher loop.
        """
        self._logger.info("Starting resource watcher")

        # Initialize CSV file
        self._init_csv()

        # Main loop - runs every 60 seconds
        while True:
            try:
                start_time = time.time()

                # Log current state
                self._log_entry("C", "0", "0")

                # Calculate elapsed time
                elapsed = time.time() - start_time

                # Sleep for remainder of 60 second interval
                sleep_time = max(0, 60.0 - (elapsed % 60.0))
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                self._logger.info("Watcher stopped")
                return

            except Exception as e:
                self._logger.error(f"Error in watcher: {e}")
                time.sleep(60)

    def _init_csv(self) -> None:
        """Initialize the CSV file with header if needed."""
        if not os.path.exists(self.csv_file) or os.path.getsize(self.csv_file) == 0:
            self._write_csv_row(self.header)

    def _log_entry(self, scale_marker: str, worker_change: str, reason: str) -> None:
        """
        Log a single entry to CSV.

        Args:
            scale_marker: Scale marker (C=check, U=up, D=down, etc.)
            worker_change: Worker change count
            reason: Reason for scaling
        """
        try:
            # Get current time
            current_time = int(time.time())

            # Get job data
            jobs_pending, jobs_running = self._get_job_data()

            # Get worker data
            worker_json, worker_count, worker_in_use, worker_drain = self._get_worker_data()

            if worker_json is None:
                self._logger.error("Failed to get worker data")
                return

            # Calculate usage
            usage = self._calculate_usage(worker_json, jobs_pending, jobs_running)

            # Get credit data
            credit, cluster_worker, worker_credit_data = self._get_credit_data()

            # Build log entry
            log_entry = {
                "time": current_time,
                "scale": scale_marker,
                "job_cnt_pending": len(jobs_pending),
                "jobs_cnt_running": len(jobs_running),
                "worker_cnt": worker_count,
                "worker_cnt_use": len(worker_in_use),
                "worker_cnt_free": worker_count - len(worker_in_use),
                "worker_drain_cnt": len(worker_drain),
                "w_change": worker_change,
                "reason": reason,
                "credit": credit,
                "cluster_worker": len(cluster_worker) if cluster_worker else 0,
                "worker_credit_data": len(worker_credit_data) if worker_credit_data else 0,
                **usage,
            }

            # Write row
            row = [log_entry.get(h, "") for h in self.header]
            self._write_csv_row(row)

        except Exception as e:
            self._logger.error(f"Error creating log entry: {e}")

    def _get_job_data(self) -> tuple[list, list]:
        """
        Get job data from scheduler.

        Returns:
            Tuple of (pending_jobs, running_jobs)
        """
        try:
            job_data = self.scheduler.get_job_data_live()
            if job_data is None:
                return [], []

            pending = []
            running = []

            for _job_id, job in job_data.items():
                if job.state == 0:  # PENDING
                    pending.append(job.__dict__)
                elif job.state == 1:  # RUNNING
                    running.append(job.__dict__)

            return pending, running

        except Exception as e:
            self._logger.error(f"Error getting job data: {e}")
            return [], []

    def _get_worker_data(self) -> tuple[Optional[dict], int, list, list]:
        """
        Get worker data from scheduler.

        Returns:
            Tuple of (worker_json, worker_count, worker_in_use, worker_drain)
        """
        try:
            node_data = self.scheduler.get_node_data()
            if node_data is None:
                return None, 0, [], []

            worker_json = {}
            worker_in_use = []
            worker_drain = []

            for hostname, node in node_data.items():
                if "worker" not in hostname:
                    continue

                worker_json[hostname] = {
                    "total_cpus": node.total_cpus,
                    "real_memory": node.real_memory,
                    "state": node.state,
                    "temporary_disk": node.temporary_disk,
                    "node_hostname": node.hostname,
                    "gres": node.gres,
                    "free_memory": node.free_memory,
                }

                if "DRAIN" in node.state:
                    worker_drain.append(hostname)
                elif node.state in ["ALLOC", "MIX"]:
                    worker_in_use.append(hostname)

            worker_count = len(worker_json)

            return worker_json, worker_count, worker_in_use, worker_drain

        except Exception as e:
            self._logger.error(f"Error getting worker data: {e}")
            return None, 0, [], []

    def _calculate_usage(
        self,
        worker_json: dict,
        jobs_pending: list,
        jobs_running: list,
    ) -> dict:
        """
        Calculate current resource usage.

        Args:
            worker_json: Worker data dictionary
            jobs_pending: List of pending jobs
            jobs_running: List of running jobs

        Returns:
            Dictionary with usage metrics
        """
        cpu_alloc = mem_alloc = mem_alloc_drain = mem_alloc_no_drain = 0
        mem_mix_drain = mem_mix_no_drain = 0
        cpu_idle = mem_idle = mem_idle_drain = mem_idle_no_drain = 0
        jobs_cpu_alloc = jobs_mem_alloc = 0
        jobs_cpu_pending = jobs_mem_pending = 0

        # Process jobs
        for job in jobs_pending:
            jobs_cpu_pending += job.get("req_cpus", 0)
            jobs_mem_pending += job.get("req_mem", 0)

        for job in jobs_running:
            jobs_cpu_alloc += job.get("req_cpus", 0)
            jobs_mem_alloc += job.get("req_mem", 0)

        # Process workers
        for worker in worker_json.values():
            if "worker" not in worker.get("node_hostname", ""):
                continue

            state = worker.get("state", "")
            cpu = worker.get("total_cpus", 0)
            mem = worker.get("real_memory", 0)

            if state in ["ALLOC", "MIX"]:
                cpu_alloc += cpu
                mem_alloc += mem

                if "DRAIN" in state:
                    if "ALLOC" in state:
                        mem_alloc_drain += mem
                    else:
                        mem_mix_drain += mem
                else:
                    if "ALLOC" in state:
                        mem_alloc_no_drain += mem
                    else:
                        mem_mix_no_drain += mem

            elif state == "IDLE":
                cpu_idle += cpu
                mem_idle += mem

                if "DRAIN" in state:
                    mem_idle_drain += mem
                else:
                    mem_idle_no_drain += mem

        return {
            "cpu_alloc": cpu_alloc,
            "mem_alloc": mem_alloc,
            "cpu_idle": cpu_idle,
            "mem_idle": mem_idle,
            "jobs_cpu_alloc": jobs_cpu_alloc,
            "jobs_mem_alloc": jobs_mem_alloc,
            "jobs_cpu_pending": jobs_cpu_pending,
            "jobs_mem_pending": jobs_mem_pending,
            "worker_mem_alloc_drain": mem_alloc_drain,
            "worker_mem_alloc": mem_alloc_no_drain,
            "worker_mem_idle_drain": mem_idle_drain,
            "worker_mem_idle": mem_idle_no_drain,
            "worker_mem_mix_drain": mem_mix_drain,
            "worker_mem_mix": mem_mix_no_drain,
        }

    def _get_credit_data(self) -> tuple[int, Optional[dict], dict[str, dict]]:
        """
        Get current credit usage data.

        Returns:
            Tuple of (credit_sum, cluster_workers, worker_credit_data)
        """
        try:
            raw_flavors = self.portal_client.get_flavors()
            flavors_list: list = raw_flavors if isinstance(raw_flavors, list) else []
            cluster_workers = self.portal_client.get_cluster_data()

            credit_sum: float = 0.0
            worker_credit_data: dict[str, dict] = {}

            if cluster_workers:
                for worker in cluster_workers:
                    if worker.get("status", "").upper() == "ACTIVE":
                        worker_flavor = worker.get("flavor", {})
                        if flavors_list and worker_flavor:
                            credit = self._get_worker_credit(flavors_list, worker_flavor)
                            credit_sum += credit

                        flavor_name = worker.get("flavor", {}).get("name", "")
                        if flavor_name in worker_credit_data:
                            worker_credit_data[flavor_name]["cnt"] += 1
                        else:
                            worker_credit_data[flavor_name] = {
                                "cnt": 1,
                                "credit": credit,
                            }

            return int(credit_sum), cluster_workers, worker_credit_data

        except Exception as e:
            self._logger.error(f"Error getting credit data: {e}")
            return 0, None, {}

    def _get_worker_credit(self, flavors: list, worker_flavor: dict) -> float:
        """
        Get credit cost for a worker.

        Args:
            flavors: List of flavor data
            worker_flavor: Worker's flavor

        Returns:
            Credit cost per hour
        """
        if not flavors or not worker_flavor:
            return 0.0

        for flavor in flavors:
            name = flavor.get("flavor", {}).get("name")
            if name == worker_flavor.get("name"):
                return float(flavor.get("flavor", {}).get("credits_costs_per_hour", 0.0))

        return 0.0

    def _write_csv_row(self, row: list) -> None:
        """
        Write a row to the CSV file.

        Args:
            row: List of values to write
        """
        try:
            with open(self.csv_file, "a", newline="", encoding="utf8") as f:
                writer = csv.writer(f)
                writer.writerow(row)
        except Exception as e:
            self._logger.error(f"Error writing CSV row: {e}")


def create_watcher(
    scheduler: SchedulerInterface,
    config: dict,
    csv_file: str = "autoscaling.csv",
    log_file: str = "autoscaling_csv.log",
) -> ResourceWatcher:
    """
    Create a ResourceWatcher instance.

    Args:
        scheduler: Scheduler interface implementation
        config: Configuration dictionary
        csv_file: Path to CSV log file
        log_file: Path to log file

    Returns:
        ResourceWatcher instance
    """
    return ResourceWatcher(
        scheduler=scheduler,
        config=config,
        csv_file=csv_file,
        log_file=log_file,
    )


def run_watcher(
    scheduler: SchedulerInterface,
    config: dict,
    csv_file: str = "autoscaling.csv",
    log_file: str = "autoscaling_csv.log",
) -> None:
    """
    Run the resource watcher.

    Args:
        scheduler: Scheduler interface implementation
        config: Configuration dictionary
        csv_file: Path to CSV log file
        log_file: Path to log file
    """
    watcher = create_watcher(
        scheduler=scheduler,
        config=config,
        csv_file=csv_file,
        log_file=log_file,
    )
    watcher.run()


def run_watcher_process(
    scheduler: SchedulerInterface,
    config: dict,
    csv_file: str = "autoscaling.csv",
    log_file: str = "autoscaling_csv.log",
) -> Process:
    """
    Run the resource watcher in a separate process.

    Args:
        scheduler: Scheduler interface implementation
        config: Configuration dictionary
        csv_file: Path to CSV log file
        log_file: Path to log file

    Returns:
        Multiprocessing Process instance
    """
    p = Process(
        target=run_watcher,
        args=(scheduler, config, csv_file, log_file),
    )
    p.daemon = True
    p.start()
    return p
