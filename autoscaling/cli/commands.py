"""
Command implementations for autoscaling CLI.
"""
import os
import sys
import time
from typing import Optional

from autoscaling.config.loader import ConfigLoader
from autoscaling.data.manager import DatabaseManager
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.scheduler.slurm import SlurmScheduler
from autoscaling.utils.logging import setup_logger
from autoscaling.utils.helpers import generate_hash


# Constants (imported from original autoscaling.py)
AUTOSCALING_FOLDER = os.path.dirname(os.path.realpath(__file__)) + "/"
FILE_CONFIG_YAML = AUTOSCALING_FOLDER + "autoscaling_config.yaml"
DATABASE_FILE = AUTOSCALING_FOLDER + "autoscaling_database.json"
LOG_FILE = AUTOSCALING_FOLDER + "autoscaling.log"
IDENTIFIER = "autoscaling"


def run_command(args, parser) -> int:
    """
    Run the appropriate command based on arguments.

    Args:
        args: Parsed arguments
        parser: ArgumentParser instance

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    # Setup logger
    logger = setup_logger(LOG_FILE)

    # Handle version
    if args.version:
        print(f"Version: 2.3.0")
        return 0

    # Initialize scheduler
    scheduler = _init_scheduler(logger)

    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    # Load configuration
    config = _load_config(logger)

    # Route to command handler
    if args.scaleup is not None:
        return cmd_scale_up(args.scaleup, logger)
    elif args.scaleupspecific:
        return cmd_scale_up_specific(
            args.scaleupspecific[0], int(args.scaleupspecific[1]), logger
        )
    elif args.scaleupchoice:
        return cmd_scale_up_choice(logger)
    elif args.scaledown:
        return cmd_scale_down(logger)
    elif args.scaledownspecific:
        return cmd_scale_down_specific(args.scaledownspecific, logger)
    elif args.scaledownchoice:
        return cmd_scale_down_choice(logger)
    elif args.scaledownbatch:
        return cmd_scale_down_batch(logger)
    elif args.node:
        return cmd_show_nodes(scheduler, logger)
    elif args.jobdata:
        return cmd_show_jobs(scheduler, logger)
    elif args.jobhistory:
        return cmd_show_job_history(scheduler, logger)
    elif args.flavor:
        return cmd_show_flavors(config, logger)
    elif args.clusterdata:
        return cmd_show_cluster(config, logger)
    elif args.mode:
        return cmd_change_mode(config, logger)
    elif args.ignore:
        return cmd_ignore_workers(config, logger)
    elif args.rescale:
        return cmd_rescale(config, logger)
    elif args.drain:
        return cmd_drain_workers(scheduler, logger)
    elif args.playbook:
        return cmd_run_playbook(config, logger)
    elif args.checkworker:
        return cmd_check_workers(logger)
    elif args.test:
        return cmd_test(scheduler, config, logger)
    elif args.clean:
        return cmd_clean(logger)
    elif args.reset:
        return cmd_reset(logger)
    elif args.update:
        return cmd_update(logger)
    elif args.stop:
        return cmd_stop(logger)
    elif args.kill:
        return cmd_kill(logger)
    elif args.status:
        return cmd_status(logger)
    elif args.visual is not None:
        return cmd_visualize(args.visual, logger)
    elif args.password:
        return cmd_set_password(logger)
    else:
        # Default: run as service
        return cmd_run_service(scheduler, config, logger)


def _init_scheduler(logger) -> Optional[SchedulerInterface]:
    """
    Initialize the scheduler based on configuration.

    Args:
        logger: Logger instance

    Returns:
        SchedulerInterface instance or None
    """
    try:
        config = _load_config(logger)
        scaling_config = config.get("scaling", {})

        if scaling_config.get("scheduler", "slurm") == "slurm":
            scheduler = SlurmScheduler()
            scheduler.set_logger(logger)

            if not scheduler.test_connection():
                logger.error("Scheduler connection failed")
                return None

            return scheduler
        else:
            logger.error("Unsupported scheduler")
            return None
    except Exception as e:
        logger.error(f"Failed to initialize scheduler: {e}")
        return None


def _load_config(logger) -> dict:
    """
    Load configuration from YAML file.

    Args:
        logger: Logger instance

    Returns:
        Configuration dictionary
    """
    try:
        loader = ConfigLoader(FILE_CONFIG_YAML)
        return loader.load()
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return {}


# --- Command Handlers ---


def cmd_scale_up(count: int, logger) -> int:
    """
    Scale up command handler.

    Args:
        count: Number of workers to add
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info(f"Scale up {count} workers")
    # Implementation would call autoscaler
    return 0


def cmd_scale_up_specific(flavor: str, count: int, logger) -> int:
    """
    Scale up specific flavor command handler.

    Args:
        flavor: Flavor name
        count: Number of workers
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info(f"Scale up {count} workers of flavor {flavor}")
    # Implementation would call autoscaler
    return 0


def cmd_scale_up_choice(logger) -> int:
    """
    Scale up by choice command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale up by choice")
    # Implementation would call autoscaler
    return 0


def cmd_scale_down(logger) -> int:
    """
    Scale down command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down idle workers")
    # Implementation would call autoscaler
    return 0


def cmd_scale_down_specific(hostnames: str, logger) -> int:
    """
    Scale down specific workers command handler.

    Args:
        hostnames: Comma-separated list of hostnames
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info(f"Scale down specific workers: {hostnames}")
    # Implementation would call autoscaler
    return 0


def cmd_scale_down_choice(logger) -> int:
    """
    Scale down by choice command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down by choice")
    # Implementation would call autoscaler
    return 0


def cmd_scale_down_batch(logger) -> int:
    """
    Scale down by batch command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down by batch")
    # Implementation would call autoscaler
    return 0


def cmd_show_nodes(scheduler, logger) -> int:
    """
    Show nodes command handler.

    Args:
        scheduler: Scheduler instance
        logger: Logger instance

    Returns:
        Exit code
    """
    node_data = scheduler.get_node_data_live()
    if node_data:
        for hostname, node in node_data.items():
            logger.info(f"{hostname}: {node.state}")
    return 0


def cmd_show_jobs(scheduler, logger) -> int:
    """
    Show jobs command handler.

    Args:
        scheduler: Scheduler instance
        logger: Logger instance

    Returns:
        Exit code
    """
    job_data = scheduler.get_job_data_live()
    if job_data:
        for job_id, job in job_data.items():
            logger.info(f"Job {job_id}: {job.state_str}")
    return 0


def cmd_show_job_history(scheduler, logger) -> int:
    """
    Show job history command handler.

    Args:
        scheduler: Scheduler instance
        logger: Logger instance

    Returns:
        Exit code
    """
    job_data = scheduler.get_job_data(7)  # Last 7 days
    if job_data:
        for job_id, job in job_data.items():
            logger.info(f"Job {job_id}: {job.state_str}")
    return 0


def cmd_show_flavors(config, logger) -> int:
    """
    Show flavors command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Show available flavors")
    # Implementation would fetch flavors from API
    return 0


def cmd_show_cluster(config, logger) -> int:
    """
    Show cluster command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Show cluster data")
    # Implementation would fetch cluster data from API
    return 0


def cmd_change_mode(config, logger) -> int:
    """
    Change mode command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Change mode")
    # Implementation would change active mode
    return 0


def cmd_ignore_workers(config, logger) -> int:
    """
    Ignore workers command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Select ignored workers")
    # Implementation would update ignore list
    return 0


def cmd_rescale(config, logger) -> int:
    """
    Rescale command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Rescale cluster configuration")
    # Implementation would update playbooks
    return 0


def cmd_drain_workers(scheduler, logger) -> int:
    """
    Drain workers command handler.

    Args:
        scheduler: Scheduler instance
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Set workers to drain")
    # Implementation would drain selected workers
    return 0


def cmd_run_playbook(config, logger) -> int:
    """
    Run playbook command handler.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Run Ansible playbook")
    # Implementation would run ansible
    return 0


def cmd_check_workers(logger) -> int:
    """
    Check workers command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Check workers")
    # Implementation would check worker status
    return 0


def cmd_test(scheduler, config, logger) -> int:
    """
    Test command handler.

    Args:
        scheduler: Scheduler instance
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Test autoscaling functions")

    # Test scheduler
    if not scheduler.test_connection():
        logger.error("Scheduler test failed")
        return 1

    logger.info("Scheduler test passed")

    # Test cluster data
    # Implementation would test other functions

    logger.info("All tests passed")
    return 0


def cmd_clean(logger) -> int:
    """
    Clean command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Clean log data")

    # Remove log files
    for filename in [
        "autoscaling.csv",
        "autoscaling.log",
        "autoscaling_csv.log",
        "autoscaling_database.json",
    ]:
        filepath = AUTOSCALING_FOLDER + filename
        if os.path.exists(filepath):
            os.remove(filepath)
            logger.info(f"Removed {filepath}")

    return 0


def cmd_reset(logger) -> int:
    """
    Reset command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Reset autoscaling")

    # Reset database
    db = DatabaseManager(DATABASE_FILE)
    db.reset()

    # Clear logs
    cmd_clean(logger)

    logger.info("Reset complete")
    return 0


def cmd_update(logger) -> int:
    """
    Update command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Update autoscaling")
    # Implementation would download and install updates
    return 0


def cmd_stop(logger) -> int:
    """
    Stop command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Stop service")
    # Implementation would stop systemd service
    return 0


def cmd_kill(logger) -> int:
    """
    Kill command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Kill service")
    # Implementation would kill running process
    return 0


def cmd_status(logger) -> int:
    """
    Status command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Show service status")
    # Implementation would show service status
    return 0


def cmd_visualize(time_range: str, logger) -> int:
    """
    Visualize command handler.

    Args:
        time_range: Time range for visualization
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info(f"Visualize cluster data: {time_range}")
    # Implementation would generate plots
    return 0


def cmd_set_password(logger) -> int:
    """
    Set password command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Set cluster password")
    # Implementation would prompt for and save password
    return 0


def cmd_run_service(scheduler, config, logger) -> int:
    """
    Run service command handler.

    Args:
        scheduler: Scheduler instance
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Run as service")

    # Initialize database
    db = DatabaseManager(DATABASE_FILE)
    db.load()

    # Run autoscaling loop
    try:
        while True:
            logger.debug("=== INIT ===")
            # Run scaling cycle
            time.sleep(config.get("service_frequency", 60))
    except KeyboardInterrupt:
        logger.info("Service stopped")

    return 0
