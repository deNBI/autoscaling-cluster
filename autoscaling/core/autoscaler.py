"""
Main autoscaler class for autoscaling.
Orchestrates the scaling process.
"""
import time
from typing import Optional

from autoscaling.cloud.ansible import AnsibleRunner
from autoscaling.cloud.client import PortalClient
from autoscaling.config.loader import ConfigLoader
from autoscaling.data.manager import DatabaseManager
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.core.state import ScalingContext, ScalingAction, ScaleState
from autoscaling.core.scaling_engine import ScalingEngine


class Autoscaler:
    """
    Main autoscaler class that orchestrates the scaling process.
    """

    def __init__(
        self,
        scheduler: SchedulerInterface,
        config: dict,
        db_manager: Optional[DatabaseManager] = None,
        ansible_runner: Optional[AnsibleRunner] = None,
        portal_client: Optional[PortalClient] = None,
    ):
        """
        Initialize the autoscaler.

        Args:
            scheduler: Scheduler interface implementation
            config: Configuration dictionary
            db_manager: Database manager for job history
            ansible_runner: Ansible runner for playbook execution
            portal_client: Portal client for API access
        """
        self.scheduler = scheduler
        self.config = config
        self.db_manager = db_manager
        self.ansible_runner = ansible_runner
        self.portal_client = portal_client

        # Get scaling settings from config
        scaling_config = config.get("scaling", {})
        self.active_mode = scaling_config.get("active_mode", "basic")
        self.service_frequency = scaling_config.get("service_frequency", 60)

        # Get mode-specific settings
        self.mode_config = scaling_config.get("mode", {}).get(
            self.active_mode, {}
        )

        self._running = False

    def run_once(self) -> ScalingAction:
        """
        Run one scaling cycle.

        Returns:
            ScalingAction that was executed
        """
        # Update playbook
        self._update_playbook()

        # Get current state
        node_data = self.scheduler.get_node_data()
        job_data = self.scheduler.get_job_data(7)  # Last 7 days

        if node_data is None:
            print("Failed to get node data")
            return ScalingAction.noop_action("Failed to get node data")

        if job_data is None:
            print("Failed to get job data")
            return ScalingAction.noop_action("Failed to get job data")

        # Prepare context
        context = self._prepare_context(node_data, job_data)

        # Calculate scaling
        engine = ScalingEngine(context)
        action = engine.calculate_scaling()

        # Execute scaling
        if action.upscale:
            self._execute_upscale(action)
        elif action.downscale:
            self._execute_downscale(action)

        return action

    def run_forever(self) -> None:
        """
        Run the autoscaler indefinitely.
        """
        self._running = True

        while self._running:
            try:
                print("=== INIT ===")
                self.run_once()

                print(f"=== SLEEP ({self.service_frequency}s) ===")
                time.sleep(self.service_frequency)

            except KeyboardInterrupt:
                print("Received interrupt, shutting down...")
                self._running = False
                break

            except Exception as e:
                print(f"Error in scaling cycle: {e}")
                time.sleep(60)  # Wait before retry

    def stop(self) -> None:
        """
        Stop the autoscaler.
        """
        self._running = False

    def _prepare_context(
        self, node_data: dict, job_data: dict
    ) -> ScalingContext:
        """
        Prepare the scaling context from node and job data.

        Args:
            node_data: Node data dictionary
            job_data: Job data dictionary

        Returns:
            ScalingContext with all necessary information
        """
        # Categorize workers
        in_use = []
        drain = []
        free = []

        for hostname, node in node_data.items():
            if "worker" not in hostname:
                continue

            state = node.state
            if "DRAIN" in state:
                drain.append(hostname)
            elif state in ["ALLOC", "MIX"]:
                in_use.append(hostname)
            elif state == "IDLE":
                free.append(hostname)

        # Categorize jobs
        pending = []
        running = []

        for job_id, job in job_data.items():
            if job.state == 0:  # PENDING
                pending.append(job.__dict__)
            elif job.state == 1:  # RUNNING
                running.append(job.__dict__)

        # Get flavor data
        flavor_data = self._get_flavor_data()

        # Build context
        return ScalingContext(
            mode=self.active_mode,
            scale_force=self.mode_config.get("scale_force", 0.6),
            scale_delay=self.mode_config.get("scale_delay", 60),
            worker_cool_down=self.mode_config.get("worker_cool_down", 60),
            limit_memory=self.mode_config.get("limit_memory", 0),
            limit_worker_starts=self.mode_config.get("limit_worker_starts", 0),
            limit_workers=self.mode_config.get("limit_workers", 0),
            worker_count=len(in_use) + len(free),
            worker_in_use=in_use,
            worker_drain=drain,
            worker_free=len(free),
            jobs_pending=pending,
            jobs_running=running,
            jobs_pending_count=len(pending),
            jobs_running_count=len(running),
            flavor_data=flavor_data,
            flavor_default=self.mode_config.get("flavor_default"),
            forecast_by_flavor_history=self.mode_config.get(
                "forecast_by_flavor_history", False
            ),
            forecast_by_job_history=self.mode_config.get(
                "forecast_by_job_history", False
            ),
            forecast_active_worker=self.mode_config.get(
                "forecast_active_worker", 0
            ),
            job_time_threshold=self.mode_config.get("job_time_threshold", 0.5),
            smoothing_coefficient=self.mode_config.get(
                "smoothing_coefficient", 0.0
            ),
            flavor_depth=self.mode_config.get("flavor_depth", -1),
            large_flavors=self.mode_config.get("large_flavors", False),
            large_flavors_except_hmf=self.mode_config.get(
                "large_flavors_except_hmf", True
            ),
        )

    def _get_flavor_data(self) -> list[dict]:
        """
        Get flavor data from portal API.

        Returns:
            List of flavor dictionaries
        """
        if self.portal_client:
            flavors = self.portal_client.get_flavors()
            if flavors:
                return flavors

        # Return empty list if no client available
        return []

    def _update_playbook(self) -> bool:
        """
        Update and run the Ansible playbook.

        Returns:
            True if successful
        """
        if self.ansible_runner:
            return self.ansible_runner.run(verbose=False)
        return True

    def _execute_upscale(self, action: ScalingAction) -> bool:
        """
        Execute an upscale action.

        Args:
            action: ScalingAction with upscale details

        Returns:
            True if successful
        """
        if self.portal_client:
            return self.portal_client.scale_up(
                action.upscale_flavor, action.upscale_count
            )
        return False

    def _execute_downscale(self, action: ScalingAction) -> bool:
        """
        Execute a downscale action.

        Args:
            action: ScalingAction with downscale details

        Returns:
            True if successful
        """
        if self.portal_client:
            return self.portal_client.scale_down(action.downscale_workers)
        return False


def create_autoscaler(
    scheduler: SchedulerInterface,
    config_path: str = "autoscaling_config.yaml",
    database_file: str = "autoscaling_database.json",
    playbook_dir: str = "~/playbook",
) -> Autoscaler:
    """
    Create an Autoscaler instance from configuration.

    Args:
        scheduler: Scheduler interface implementation
        config_path: Path to configuration file
        database_file: Path to database file
        playbook_dir: Directory containing playbooks

    Returns:
        Configured Autoscaler instance
    """
    # Load configuration
    config_loader = ConfigLoader(config_path)
    config = config_loader.load()

    # Create database manager
    db_manager = DatabaseManager(database_file)
    db_manager.load()

    # Create ansible runner
    ansible_runner = AnsibleRunner(playbook_dir=playbook_dir)

    # Create portal client
    scaling_config = config.get("scaling", {})
    portal_client = PortalClient(
        api_url=scaling_config.get("portal_scaling_link", ""),
        webapp_url=scaling_config.get("portal_webapp_link", ""),
        password_file="cluster_pw.json",
    )

    return Autoscaler(
        scheduler=scheduler,
        config=config,
        db_manager=db_manager,
        ansible_runner=ansible_runner,
        portal_client=portal_client,
    )
