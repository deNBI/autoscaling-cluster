#!/usr/bin/env python3
"""
Command-line interface for the autoscaling cluster package.

This module provides the main entry point for the autoscaling system.
It handles command-line argument parsing and dispatches to the appropriate
command handlers.

Usage:
    python -m autoscaling.cli [options]
"""

import argparse
import logging
import os
import sys
from typing import Optional

import yaml

from autoscaling import __version__
from autoscaling.api.client import PortalClient
from autoscaling.api.flavor import get_usable_flavors
from autoscaling.config.loader import load_config, read_cluster_id
from autoscaling.data.database import JobDatabase
from autoscaling.data.fetcher import DataFetcher
from autoscaling.forecasting.predictor import JobPredictor
from autoscaling.scheduler.slurm import SlurmInterface
from autoscaling.utils.helpers import (
    FILE_CONFIG,
    PasswordError,
    get_autoscaling_folder,
    get_cluster_password,
)
from autoscaling.utils.logger import setup_logger

logger = logging.getLogger(__name__)

# Constants from original code
AUTOSCALING_FOLDER = get_autoscaling_folder()
LOG_FILE = AUTOSCALING_FOLDER + "autoscaling.log"
LOG_CSV = AUTOSCALING_FOLDER + "autoscaling.csv"
CLUSTER_PASSWORD_FILE = AUTOSCALING_FOLDER + "cluster_pw.json"
DATABASE_FILE = AUTOSCALING_FOLDER + "autoscaling_database.json"


class AutoScalingCLI:
    """Main CLI handler for autoscaling commands."""

    def __init__(self):
        """Initialize the CLI handler."""
        self.config = {}
        self.cluster_id = ""
        self.scheduler = None
        self.fetcher = None
        self.api_client = None
        self.database = None
        self.predictor = None

    def setup(self, config_path: str = None) -> bool:
        """Setup the CLI with configuration and dependencies.

        Args:
            config_path: Optional path to custom configuration file
        """
        try:
            # Store config path for later use (e.g., hash generation)
            self._config_path = config_path

            # Load configuration
            self.config = load_config(config_path)

            # Get cluster ID
            self.cluster_id = read_cluster_id()

            # Initialize scheduler
            self.scheduler = SlurmInterface()
            if not self.scheduler.scheduler_function_test():
                logger.error("Failed to initialize scheduler")
                return False

            # Initialize fetcher
            self.fetcher = DataFetcher(self.scheduler)
            self.fetcher.set_ignore_workers(self.config.get("ignore_workers", []))

            # Initialize API client
            self.api_client = PortalClient(self.config, self.cluster_id)

            # Initialize database
            self.database = JobDatabase(self.config)

            # Initialize predictor
            self.predictor = JobPredictor(self.config, self.database)

            logger.info("CLI setup completed successfully")
            return True

        except Exception as e:
            logger.error("Failed to setup CLI: %s", e)
            return False

    def run_scaling_loop(self) -> bool:
        """Run the main scaling loop."""
        try:
            flavor_data = get_usable_flavors([], quiet=True, cut=True)
            if not flavor_data:
                logger.error("No usable flavors found")
                return False

            # Update and run ansible playbook
            self._rescale_init()

            # Load and update database
            config_hash = self._get_config_hash()
            self.database.load(config_hash)
            self.database.update_with_jobs(
                self.fetcher.fetch_completed_jobs(1), flavor_data
            )

            # Run scaling engine
            self._multiscale(flavor_data)
            return True

        except Exception as e:
            logger.error("Scaling loop failed: %s", e)
            return False

    def _multiscale(self, flavor_data: list) -> None:
        """Run the scaling engine."""
        from autoscaling.core.scaling_engine import ScalingEngine

        engine = ScalingEngine(
            self.config, self.scheduler, self.api_client, self.fetcher, self.predictor
        )
        engine.multiscale(flavor_data)

    def _get_config_hash(self) -> str:
        """Get configuration file hash.

        If a custom config path was used, hash that file.
        Otherwise, hash the default config in the autoscaling folder.
        """
        from autoscaling.utils.helpers import generate_hash

        # Use config path from setup if available, otherwise default
        config_path = getattr(self, "_config_path", None)
        if config_path:
            return generate_hash(config_path)

        autoscaling_folder = get_autoscaling_folder()
        config_path = autoscaling_folder + FILE_CONFIG
        return generate_hash(config_path)

    def _rescale_init(self) -> bool:
        """Initialize rescale - update playbook and run."""
        try:
            # Update scaling script
            scaling_script_url = self.config.get("scaling_script_url", "")
            if scaling_script_url:
                self._update_scaling_script(scaling_script_url)

            # Run ansible playbook
            return self._run_ansible_playbook()
        except Exception as e:
            logger.error("Rescale init failed: %s", e)
            return False

    def _update_scaling_script(self, url: str, filename: str = None) -> bool:
        """Update scaling script from URL."""
        import requests

        if filename is None:
            filename = AUTOSCALING_FOLDER + "scaling.py"

        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                with open(filename, "w") as f:
                    f.write(response.text)
                logger.debug("Updated scaling script")
                return True
        except Exception as e:
            logger.error("Error updating scaling script: %s", e)

        return False

    def _run_ansible_playbook(self) -> bool:
        """Run ansible playbook."""
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

    def cmd_version(self) -> None:
        """Print version information."""
        print(f"autoscaling version {__version__}")

    def cmd_password(self) -> None:
        """Set cluster password interactively."""
        import getpass

        password = getpass.getpass("Enter cluster password: ")
        from autoscaling.utils.helpers import set_cluster_password

        try:
            set_cluster_password(password)
            print("Password set successfully")
        except Exception as e:
            print(f"Failed to set password: {e}")
            sys.exit(1)

    def cmd_rescale(self) -> None:
        """Trigger rescale operation."""
        self.run_scaling_loop()

    def cmd_node_data(self) -> None:
        """Display node data from scheduler."""
        node_data = self.fetcher.fetch_node_data_live()
        for hostname, worker in node_data.items():
            print(
                f"{hostname}: {worker.state} - {worker.total_cpus} CPUs, {worker.real_memory}MB"
            )

    def cmd_job_data(self) -> None:
        """Display job data from scheduler."""
        pending, running = self.fetcher.fetch_job_data()
        print(f"Pending jobs: {len(pending)}")
        print(f"Running jobs: {len(running)}")

        if pending:
            print("\nPending jobs:")
            for job_id, job in list(pending.items())[:10]:
                print(
                    f"  {job_id}: {job.jobname} - {job.req_cpus} CPUs, {job.req_mem}MB"
                )

    def cmd_flavor_data(self) -> None:
        """Display flavor data from API."""
        flavor_data = get_usable_flavors([], quiet=False, cut=False)
        if flavor_data:
            print(f"Available flavors: {len(flavor_data)}")
            for flavor in flavor_data:
                fv = flavor.get("flavor", {})
                print(
                    f"  {fv.get('name')}: {fv.get('vcpus')} CPUs, {fv.get('ram')}MB RAM"
                )

    def cmd_cluster_data(self) -> None:
        """Display cluster data from API."""
        from autoscaling.api import ApiAuthError, ApiError

        try:
            password = get_cluster_password()
        except (ApiAuthError, ApiError, PasswordError) as e:
            import getpass

            print(f"Password error: {e}")
            password = getpass.getpass("Enter cluster password: ")
        except Exception as e:
            import getpass

            print(f"Error getting password: {e}")
            password = getpass.getpass("Enter cluster password: ")

        try:
            cluster_data = self.api_client.get_cluster_data(password)
            if cluster_data:
                workers = cluster_data.get("workers", [])
                print(f"Cluster workers: {len(workers)}")
                for w in workers[:20]:
                    print(f"  {w.get('hostname')}: {w.get('status')}")
        except (ApiAuthError, ApiError) as e:
            print(f"API error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

    def cmd_drain(self) -> None:
        """Drain selected workers."""
        node_data = self.fetcher.fetch_node_data_live()
        print("Workers:")
        for i, (hostname, worker) in enumerate(node_data.items()):
            print(f"  {i}: {hostname} - {worker.state}")

        print("Enter worker indices to drain (or 'quit' to exit):")
        drain_list = []
        while True:
            selection = input("> ")
            if selection.lower() == "quit":
                break
            if selection.isdigit():
                idx = int(selection)
                if 0 <= idx < len(node_data):
                    hostname = list(node_data.keys())[idx]
                    drain_list.append(hostname)
                    self.scheduler.set_node_to_drain(hostname)
                    print(f"Drained: {hostname}")

    def cmd_visualize(self, time_range: Optional[str] = None) -> None:
        """Visualize cluster data."""
        print("Visualization feature")
        print(f"Time range: {time_range}")
        # TODO: Implement visualization

    def cmd_mode(self) -> None:
        """Interactive mode selection."""
        import yaml

        modes = list(self.config.get("mode", {}).keys())
        print("Available modes:")
        for i, mode in enumerate(modes):
            info = self.config["mode"][mode].get("info", "")
            print(f"  {i}: {mode} - {info}")

        selection = input("Enter mode index: ")
        if selection.isdigit() and int(selection) < len(modes):
            self.config["active_mode"] = modes[int(selection)]
            config_path = get_autoscaling_folder() + FILE_CONFIG
            with open(config_path, "w") as f:
                yaml.dump({"scaling": self.config}, f, default_flow_style=False)
            print(f"Active mode set to: {modes[int(selection)]}")
        else:
            print("Invalid selection")

    def cmd_ignore_workers(self) -> None:
        """Interactive worker selection for ignoring."""
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
        config_path = get_autoscaling_folder() + FILE_CONFIG
        with open(config_path, "w") as f:
            yaml.dump({"scaling": self.config}, f, default_flow_style=False)
        print(f"Ignored workers: {ignored}")


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="autoscaling",
        description="Autoscaling cluster management tool",
        epilog=f"Version {__version__}",
    )

    # Version flag
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Print version information",
    )

    # Password flag
    parser.add_argument(
        "-p",
        "--password",
        action="store_true",
        help="Set cluster password",
    )

    # Rescale flag
    parser.add_argument(
        "-rsc",
        "--rescale",
        action="store_true",
        help="Run scaling with ansible playbook",
    )

    # Service flag
    parser.add_argument(
        "-s",
        "--service",
        action="store_true",
        help="Run as service (continuous monitoring)",
    )

    # Node data flag
    parser.add_argument(
        "-nd",
        "--node-data",
        action="store_true",
        help="Display node data",
    )

    # Job data flag
    parser.add_argument(
        "-jd",
        "--job-data",
        action="store_true",
        help="Display job data",
    )

    # Flavor data flag
    parser.add_argument(
        "-fd",
        "--flavor-data",
        action="store_true",
        help="Display flavor data",
    )

    # Cluster data flag
    parser.add_argument(
        "-cd",
        "--cluster-data",
        action="store_true",
        help="Display cluster data from API",
    )

    # Drain flag
    parser.add_argument(
        "-d",
        "--drain",
        action="store_true",
        help="Interactive mode to drain workers",
    )

    # Visualize flag
    parser.add_argument(
        "-vis",
        "--visualize",
        nargs="?",
        const="",
        metavar="TIME_RANGE",
        help="Visualize log data (optional time range)",
    )

    # Mode flag
    parser.add_argument(
        "-m",
        "--mode",
        action="store_true",
        help="Interactive mode selection",
    )

    # Ignore workers flag
    parser.add_argument(
        "-iw",
        "--ignore-workers",
        action="store_true",
        help="Interactive worker selection for ignoring",
    )

    # Config flag
    parser.add_argument(
        "-c",
        "--config",
        metavar="CONFIG_PATH",
        help="Path to custom configuration file",
    )

    return parser


def main() -> int:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Setup logging
    logger = setup_logger(LOG_FILE)

    # Initialize CLI handler
    cli = AutoScalingCLI()

    # Handle version
    if args.version:
        cli.cmd_version()
        return 0

    # Handle password
    if args.password:
        cli.cmd_password()
        return 0

    # Setup dependencies (required for most commands)
    config_path = args.config if hasattr(args, "config") and args.config else None
    if not cli.setup(config_path):
        logger.error("Failed to setup CLI")
        return 1

    # Handle rescale
    if args.rescale:
        cli.cmd_rescale()
        return 0

    # Handle node data
    if args.node_data:
        cli.cmd_node_data()
        return 0

    # Handle job data
    if args.job_data:
        cli.cmd_job_data()
        return 0

    # Handle flavor data
    if args.flavor_data:
        cli.cmd_flavor_data()
        return 0

    # Handle cluster data
    if args.cluster_data:
        cli.cmd_cluster_data()
        return 0

    # Handle drain
    if args.drain:
        cli.cmd_drain()
        return 0

    # Handle visualize
    if args.visualize is not None:
        cli.cmd_visualize(args.visualize)
        return 0

    # Handle mode
    if args.mode:
        cli.cmd_mode()
        return 0

    # Handle ignore workers
    if args.ignore_workers:
        cli.cmd_ignore_workers()
        return 0

    # Default: run as service
    cli.cmd_rescale()
    return 0


if __name__ == "__main__":
    sys.exit(main())
