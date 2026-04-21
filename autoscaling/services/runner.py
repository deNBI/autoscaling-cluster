"""
Service runner for autoscaling.
Manages the autoscaling service lifecycle.
"""
import os
import signal
import sys
import time
from typing import Optional

from autoscaling.cloud.ansible import AnsibleRunner
from autoscaling.cloud.client import PortalClient
from autoscaling.config.loader import ConfigLoader
from autoscaling.core.autoscaler import Autoscaler
from autoscaling.core.state import ScalingAction
from autoscaling.data.manager import DatabaseManager
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.utils.logging import setup_logger
from autoscaling.utils.helpers import generate_hash


class ServiceRunner:
    """
    Runner for the autoscaling service.
    Manages the service lifecycle and scaling loop.
    """

    def __init__(
        self,
        scheduler: SchedulerInterface,
        config: dict,
        database_file: str = "autoscaling_database.json",
        playbook_dir: str = "~/playbook",
        log_file: str = "autoscaling.log",
    ):
        """
        Initialize the service runner.

        Args:
            scheduler: Scheduler interface implementation
            config: Configuration dictionary
            database_file: Path to database file
            playbook_dir: Directory containing playbooks
            log_file: Path to log file
        """
        self.scheduler = scheduler
        self.config = config
        self.database_file = database_file
        self.playbook_dir = playbook_dir
        self.log_file = log_file

        # Get scaling settings
        scaling_config = config.get("scaling", {})
        self.active_mode = scaling_config.get("active_mode", "basic")
        self.service_frequency = scaling_config.get("service_frequency", 60)
        self.automatic_update = scaling_config.get("automatic_update", True)

        # Create components
        self.db_manager = DatabaseManager(database_file)
        self.ansible_runner = AnsibleRunner(playbook_dir=playbook_dir)

        # Create portal client
        self.portal_client = PortalClient(
            api_url=scaling_config.get("portal_scaling_link", ""),
            webapp_url=scaling_config.get("portal_webapp_link", ""),
            password_file="cluster_pw.json",
        )

        # Create autoscaler
        self.autoscaler = Autoscaler(
            scheduler=scheduler,
            config=config,
            db_manager=self.db_manager,
            ansible_runner=self.ansible_runner,
            portal_client=self.portal_client,
        )

        # State
        self._running = False
        self._logger = None

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self) -> None:
        """
        Run the service loop.
        """
        self._running = True
        self._logger = setup_logger(self.log_file)

        self._logger.info("Starting autoscaling service")
        self._logger.info(f"Active mode: {self.active_mode}")
        self._logger.info(f"Service frequency: {self.service_frequency}s")

        # Initial setup
        self._setup()

        # Main loop
        while self._running:
            try:
                self._logger.debug("=== INIT ===")

                # Run one scaling cycle
                action = self.autoscaler.run_once()

                if action.is_noop:
                    self._logger.debug("No scaling action needed")
                else:
                    self._logger.info(
                        f"Scaling action: {action.reason or 'Unknown'}"
                    )

                # Update database if needed
                self._update_database()

                # Check for updates if enabled
                if self.automatic_update:
                    self._check_updates()

                self._logger.debug(f"=== SLEEP ({self.service_frequency}s) ===")
                time.sleep(self.service_frequency)

            except KeyboardInterrupt:
                self._logger.info("Received interrupt, shutting down...")
                self._running = False
                break

            except Exception as e:
                self._logger.error(f"Error in scaling cycle: {e}")
                time.sleep(60)  # Wait before retry

        self._logger.info("Service stopped")

    def stop(self) -> None:
        """
        Stop the service gracefully.
        """
        self._logger.info("Stopping service...")
        self._running = False

    def _setup(self) -> None:
        """
        Perform initial setup.
        """
        self._logger.info("Performing initial setup")

        # Load database
        self.db_manager.load()

        # Update playbook
        self.ansible_runner.run(verbose=False)

    def _update_database(self) -> None:
        """
        Update the database with new job data.
        """
        try:
            if self._should_update_database():
                self.db_manager.load()
        except Exception as e:
            self._logger.error(f"Error updating database: {e}")

    def _should_update_database(self) -> bool:
        """
        Check if database should be updated.

        Returns:
            True if database should be updated
        """
        mode_config = self.config.get("scaling", {}).get("mode", {}).get(
            self.active_mode, {}
        )

        return (
            mode_config.get("forecast_by_flavor_history", False)
            or mode_config.get("forecast_by_job_history", False)
        )

    def _check_updates(self) -> None:
        """
        Check for and install updates if enabled.
        """
        # Implementation would check GitHub for updates
        # For now, this is a placeholder
        pass

    def _signal_handler(self, signum, frame) -> None:
        """
        Handle signal interrupts.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = signal.Signals(signum).name
        self._logger.info(f"Received {signal_name}")

        if signum in [signal.SIGINT, signal.SIGTERM]:
            self._running = False


def create_service_runner(
    config_path: str = "autoscaling_config.yaml",
    database_file: str = "autoscaling_database.json",
    playbook_dir: str = "~/playbook",
    log_file: str = "autoscaling.log",
) -> Optional[ServiceRunner]:
    """
    Create a ServiceRunner from configuration.

    Args:
        config_path: Path to configuration file
        database_file: Path to database file
        playbook_dir: Directory containing playbooks
        log_file: Path to log file

    Returns:
        ServiceRunner instance or None
    """
    # Load configuration
    try:
        config_loader = ConfigLoader(config_path)
        config = config_loader.load()
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None

    # Get scheduler type
    scheduler_type = config.get("scaling", {}).get("scheduler", "slurm")

    if scheduler_type == "slurm":
        from autoscaling.scheduler.slurm import SlurmScheduler

        scheduler = SlurmScheduler()
        if not scheduler.test_connection():
            print("Failed to connect to Slurm scheduler")
            return None
    else:
        print(f"Unsupported scheduler: {scheduler_type}")
        return None

    return ServiceRunner(
        scheduler=scheduler,
        config=config,
        database_file=database_file,
        playbook_dir=playbook_dir,
        log_file=log_file,
    )


def run_service(
    config_path: str = "autoscaling_config.yaml",
    database_file: str = "autoscaling_database.json",
    playbook_dir: str = "~/playbook",
    log_file: str = "autoscaling.log",
) -> int:
    """
    Run the autoscaling service.

    Args:
        config_path: Path to configuration file
        database_file: Path to database file
        playbook_dir: Directory containing playbooks
        log_file: Path to log file

    Returns:
        Exit code
    """
    runner = create_service_runner(
        config_path=config_path,
        database_file=database_file,
        playbook_dir=playbook_dir,
        log_file=log_file,
    )

    if runner is None:
        return 1

    try:
        runner.run()
    except Exception as e:
        print(f"Service error: {e}")
        return 1

    return 0
