"""
CLI commands for the autoscaling system.
"""

import logging
from typing import Any, Optional

from autoscaling.api.client import PortalClient
from autoscaling.api.flavor import get_usable_flavors
from autoscaling.data.database import JobDatabase
from autoscaling.data.fetcher import DataFetcher
from autoscaling.forecasting.predictor import JobPredictor
from autoscaling.scheduler.slurm import SlurmInterface
from autoscaling.utils.helpers import AUTOSCALING_FOLDER, FILE_CONFIG_YAML

logger = logging.getLogger(__name__)


class CLICommands:
    """
    CLI command handlers.
    """

    def __init__(self):
        """Initialize CLI commands."""
        self.config: dict[str, Any] = {}
        self.cluster_id: str = ""
        self.scheduler: Optional[SlurmInterface] = None
        self.fetcher: Optional[DataFetcher] = None
        self.api_client: Optional[PortalClient] = None
        self.database: Optional[JobDatabase] = None
        self.predictor: Optional[JobPredictor] = None

    def setup(
        self,
        config: dict[str, Any],
        cluster_id: str,
        scheduler: SlurmInterface,
    ) -> None:
        """
        Setup CLI with dependencies.

        Args:
            config: Configuration dictionary
            cluster_id: Cluster ID
            scheduler: Scheduler interface
        """
        self.config = config
        self.cluster_id = cluster_id
        self.scheduler = scheduler
        self.fetcher = DataFetcher(scheduler)
        self.api_client = PortalClient(config, cluster_id)
        self.database = JobDatabase(config)
        self.predictor = JobPredictor(config)

    def cmd_version(self) -> None:
        """Print version information."""
        from autoscaling import __version__

        print(f"autoscaling version {__version__}")

    def cmd_password(self) -> None:
        """Set cluster password."""
        from autoscaling.utils.helpers import set_cluster_password

        password = input("Enter cluster password: ")
        set_cluster_password(password)

    def cmd_rescale(self) -> None:
        """Trigger rescale."""
        self._run_scaling_loop()

    def cmd_node_data(self) -> None:
        """Display node data."""
        node_data = self.fetcher.fetch_node_data_live()
        for hostname, worker in node_data.items():
            print(
                f"{hostname}: {worker.state} - {worker.total_cpus} CPUs, {worker.real_memory}MB"
            )

    def cmd_mode(self) -> None:
        """Change active mode."""
        modes = list(self.config.get("mode", {}).keys())
        print("Available modes:")
        for i, mode in enumerate(modes):
            info = self.config["mode"][mode].get("info", "")
            print(f"  {i}: {mode} - {info}")

        selection = input("Enter mode index: ")
        if selection.isdigit() and int(selection) < len(modes):
            self.config["active_mode"] = modes[int(selection)]
            self._save_config()
            print(f"Active mode set to: {modes[int(selection)]}")
        else:
            print("Invalid selection")

    def cmd_ignore_workers(self) -> None:
        """Select workers to ignore."""
        node_data = self.fetcher.fetch_node_data_live()
        print("Available workers:")
        for i, (hostname, worker) in enumerate(node_data.items()):
            print(f"  {i}: {hostname} - {worker.state}")

        print("Enter worker numbers to ignore (or 'quit' to exit):")
        ignored = []
        while True:
            selection = input("> ")
            if selection.lower() == "quit":
                break
            if selection.isdigit():
                idx = int(selection)
                if 0 <= idx < len(node_data):
                    hostname = list(node_data.keys())[idx]
                    ignored.append(hostname)
                    print(f"Added {hostname} to ignore list")

        self.config["ignore_workers"] = ignored
        self._save_config()
        print(f"Ignored workers: {ignored}")

    def cmd_job_data(self) -> None:
        """Display job data."""
        pending, running = self.fetcher.fetch_job_data()
        print(f"Pending jobs: {len(pending)}")
        print(f"Running jobs: {len(running)}")

        if pending:
            print("\nPending jobs:")
            for job_id, job in list(pending.items())[:10]:
                print(
                    f"  {job_id}: {job.jobname} - {job.req_cpus} CPUs, {job.req_mem}MB"
                )

    def cmd_job_history(self) -> None:
        """Display job history."""
        days = self.config.get("history_recall", 7)
        completed = self.fetcher.fetch_completed_jobs(days)
        print(f"Completed jobs (last {days} days): {len(completed)}")

        for job_id, job in list(completed.items())[:20]:
            print(f"  {job_id}: {job.jobname} - {job.elapsed}s")

    def cmd_flavor_data(self) -> None:
        """Display flavor data."""
        flavor_data = get_usable_flavors([], quiet=False, cut=False)
        print(f"Available flavors: {len(flavor_data)}")

        for flavor in flavor_data:
            fv = flavor.get("flavor", {})
            print(f"  {fv.get('name')}: {fv.get('vcpus')} CPUs, {fv.get('ram')}MB RAM")

    def cmd_cluster_data(self) -> None:
        """Display cluster data."""
        try:
            password = self.api_client._get_cluster_password()
        except SystemExit:
            password = input("Enter cluster password: ")

        cluster_data = self.api_client.get_cluster_data(password)
        if cluster_data:
            workers = cluster_data.get("workers", [])
            print(f"Cluster workers: {len(workers)}")
            for w in workers[:20]:
                print(f"  {w.get('hostname')}: {w.get('status')}")

    def cmd_drain(self) -> None:
        """Drain selected workers."""
        node_data = self.fetcher.fetch_node_data_live()
        print("Workers:")
        for i, (hostname, worker) in enumerate(node_data.items()):
            print(f"  {i}: {hostname} - {worker.state}")

        print("Enter worker indices to drain:")
        drain_list = []
        while True:
            selection = input("> ")
            if not selection.isdigit():
                break
            idx = int(selection)
            if 0 <= idx < len(node_data):
                hostname = list(node_data.keys())[idx]
                drain_list.append(hostname)
                self.scheduler.set_node_to_drain(hostname)
                print(f"Drained: {hostname}")

    def cmd_visualize(self, time_range: Optional[str] = None) -> None:
        """Visualize cluster data (placeholder)."""
        print("Visualization feature - requires matplotlib")
        print(f"Time range: {time_range}")

    def _save_config(self) -> None:
        """Save current configuration."""
        import yaml

        with open(FILE_CONFIG_YAML, "w") as f:
            yaml.dump({"scaling": self.config}, f, default_flow_style=False)

    def _run_scaling_loop(self) -> None:
        """Run the main scaling loop."""
        flavor_data = get_usable_flavors([], quiet=True, cut=True)
        if flavor_data:
            self._rescale_init()
            self.database.load(self._get_config_hash())
            self.database.update_with_jobs(
                self.fetcher.fetch_completed_jobs(1), flavor_data
            )
            self._multiscale(flavor_data)

    def _get_config_hash(self) -> str:
        """Get configuration hash."""
        return __import__("autoscaling.utils.helpers").utils.helpers.generate_hash(
            FILE_CONFIG_YAML
        )

    def _rescale_init(self) -> None:
        """Initialize rescale."""
        self._update_scaling_script()
        self._run_ansible_playbook()

    def _update_scaling_script(self) -> None:
        """Update scaling script."""
        url = self.config.get("scaling_script_url", "")
        if url:
            try:
                import requests

                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    with open(AUTOSCALING_FOLDER + "scaling.py", "w") as f:
                        f.write(response.text)
            except Exception as e:
                logger.error("Error updating scaling script: %s", e)

    def _run_ansible_playbook(self) -> bool:
        """Run ansible playbook."""
        import os
        import subprocess

        playbook_dir = "/root/playbook"
        if not os.path.exists(playbook_dir):
            return False

        os.chdir(playbook_dir)
        try:
            process = subprocess.Popen(
                ["ansible-playbook", "-i", "ansible_hosts", "site.yml"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            for line in process.stdout:
                logger.debug("[ANSIBLE] %s", line.strip())
            return process.wait() == 0
        except Exception as e:
            logger.error("Error running playbook: %s", e)
            return False

    def _multiscale(self, flavor_data: list) -> None:
        """Run scaling loop."""
        from autoscaling.core.scaling_engine import ScalingEngine

        engine = ScalingEngine(
            self.config, self.scheduler, self.api_client, self.fetcher, self.predictor
        )
        engine.multiscale(flavor_data)
