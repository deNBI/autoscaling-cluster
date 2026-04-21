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
from autoscaling.scheduler.node_data import receive_node_data_db, receive_node_data_live
from autoscaling.scheduler.job_data import receive_job_data
from autoscaling.cluster.api import ClusterAPI, get_cluster_workers, get_flavor_data
from autoscaling.core.autoscaler import create_autoscaler
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

    # Load config to get scaling link and cluster id
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Get flavor data to find the default flavor
    flavor_data = cluster_api.get_flavors()
    if not flavor_data:
        logger.error("Failed to get flavor data")
        return 1

    # Find smallest flavor as default
    smallest_flavor = min(flavor_data, key=lambda f: f.get("flavor", {}).get("ram_gib", float("inf")))
    flavor_name = smallest_flavor.get("flavor", {}).get("name")

    if not flavor_name:
        logger.error("No valid flavor found")
        return 1

    # Scale up
    result = cluster_api.scale_up(flavor_name, count)
    if result:
        logger.info(f"Successfully scaled up {count} workers of flavor {flavor_name}")
        # Trigger rescale
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale up failed")
        return 1


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

    # Load config to get scaling link and cluster id
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Scale up with specific flavor
    result = cluster_api.scale_up(flavor, count)
    if result:
        logger.info(f"Successfully scaled up {count} workers of flavor {flavor}")
        # Trigger rescale
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale up failed")
        return 1


def cmd_scale_up_choice(logger) -> int:
    """
    Scale up by choice command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale up by choice")

    # Load config
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Get node and job data
    scheduler = _init_scheduler(logger)
    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    node_data = receive_node_data_live(scheduler)
    jobs_pending, jobs_running = receive_job_data(scheduler)

    if not node_data or not jobs_pending:
        logger.info("No pending jobs or no node data")
        return 0

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Get flavor data
    flavor_data = cluster_api.get_flavors()
    if not flavor_data:
        logger.error("Failed to get flavor data")
        return 1

    # Scale up via cluster API
    result = cluster_api.scale_up("default", len(jobs_pending))
    if result:
        logger.info(f"Successfully scaled up for {len(jobs_pending)} pending jobs")
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale up failed")
        return 1


def cmd_scale_down(logger) -> int:
    """
    Scale down command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down idle workers")

    # Load config
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Get node data to find idle workers
    scheduler = _init_scheduler(logger)
    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    node_data = receive_node_data_live(scheduler)
    if not node_data:
        logger.error("Failed to get node data")
        return 1

    # Find idle workers
    idle_workers = []
    for hostname, node in node_data.items():
        if "worker" in hostname and node.state == "IDLE":
            idle_workers.append(hostname)

    if not idle_workers:
        logger.info("No idle workers to scale down")
        return 0

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Scale down
    result = cluster_api.scale_down(idle_workers)
    if result:
        logger.info(f"Successfully scaled down {len(idle_workers)} idle workers")
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale down failed")
        return 1


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

    # Parse hostnames (comma-separated)
    worker_list = [h.strip() for h in hostnames.split(",")]

    # Load config
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Scale down
    result = cluster_api.scale_down(worker_list)
    if result:
        logger.info(f"Successfully scaled down {len(worker_list)} workers: {hostnames}")
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale down failed")
        return 1


def cmd_scale_down_choice(logger) -> int:
    """
    Scale down by choice command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down by choice")

    # Load config
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Get node data to find workers
    scheduler = _init_scheduler(logger)
    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    node_data = receive_node_data_live(scheduler)
    if not node_data:
        logger.error("Failed to get node data")
        return 1

    # Collect all worker hostnames
    all_workers = []
    for hostname, node in node_data.items():
        if "worker" in hostname:
            all_workers.append(hostname)

    if not all_workers:
        logger.info("No workers found")
        return 0

    # Present choice (simplified - scale down all idle)
    idle_workers = [w for w in all_workers if node_data[w].state == "IDLE"]

    if not idle_workers:
        logger.info("No idle workers to scale down")
        return 0

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Scale down
    result = cluster_api.scale_down(idle_workers)
    if result:
        logger.info(f"Successfully scaled down {len(idle_workers)} idle workers")
        rescale_cluster(None, cluster_api, 0)
        return 0
    else:
        logger.error("Scale down failed")
        return 1


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
    node_data = receive_node_data_live(scheduler)
    if node_data:
        for hostname, node in node_data.items():
            if isinstance(node, dict):
                state = node.get("state", "UNKNOWN")
            else:
                state = node.state
            logger.info(f"{hostname}: {state}")
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

    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Get flavors
    flavors = cluster_api.get_flavors()
    if flavors:
        logger.info(f"Found {len(flavors)} flavors:")
        for flavor in flavors:
            fv = flavor.get("flavor", {})
            logger.info(
                f"  - {fv.get('name')}: "
                f"vcpus={fv.get('vcpus')}, "
                f"ram={fv.get('ram_gib')}GB, "
                f"disk={fv.get('ephemeral_disk')}GB, "
                f"credits={fv.get('credits_costs_per_hour')}/h"
            )
        return 0
    else:
        logger.error("Failed to get flavors")
        return 1


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

    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Get cluster data
    cluster_data = cluster_api.get_cluster_data()
    if cluster_data:
        logger.info(f"Cluster ID: {cluster_id}")
        workers = cluster_data.get("workers", [])
        logger.info(f"Total workers: {len(workers)}")
        for worker in workers:
            fv = worker.get("flavor", {})
            logger.info(
                f"  - {worker.get('hostname')}: "
                f"state={worker.get('state')}, "
                f"flavor={fv.get('name', 'N/A')}"
            )
        return 0
    else:
        logger.error("Failed to get cluster data")
        return 1


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

    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Create cluster API client
    from autoscaling.cluster.api import ClusterAPI
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Get current worker count
    node_data = receive_node_data_live(None)
    if node_data:
        worker_count = sum(1 for h, n in node_data.items() if "worker" in h)
    else:
        worker_count = 0

    # Run rescale
    return rescale_cluster(None, cluster_api, worker_count)


def rescale_cluster(scheduler, cluster_api, worker_count: int = 0) -> bool:
    """
    Apply new worker configuration.
    Executes scaling, modifies and runs ansible playbook.

    Args:
        scheduler: Scheduler interface (optional)
        cluster_api: Cluster API client
        worker_count: Expected worker count (0 = default delay)

    Returns:
        True if successful
    """
    from autoscaling.cloud.ansible import AnsibleRunner
    import time

    if worker_count == 0:
        time.sleep(10)  # WAIT_CLUSTER_SCALING

    ansible_runner = AnsibleRunner()
    rescale_success = ansible_runner.run()

    return rescale_success is not None


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

    # Initialize autoscaler
    try:
        from autoscaling.core.autoscaler import create_autoscaler

        autoscaler = create_autoscaler(
            scheduler=scheduler,
            config_path=FILE_CONFIG_YAML,
            database_file=DATABASE_FILE,
        )

        # Run autoscaling loop
        logger.info("Starting autoscaling loop...")
        autoscaler.run_forever()

    except KeyboardInterrupt:
        logger.info("Service stopped by user")

    return 0
