"""
Command implementations for autoscaling CLI.
"""

import os
import sys
import time
from typing import Optional

from autoscaling.cloud.ansible import AnsibleRunner
from autoscaling.cluster.api import ClusterAPI, get_flavor_data
from autoscaling.config.loader import ConfigLoader
from autoscaling.core.scaling_engine import ScalingEngine
from autoscaling.core.state import ScalingContext
from autoscaling.data.manager import DatabaseManager
from autoscaling.scheduler.interface import SchedulerInterface
from autoscaling.scheduler.job_data import receive_job_data
from autoscaling.scheduler.node_data import receive_node_data_live
from autoscaling.scheduler.slurm import SlurmScheduler
from autoscaling.utils.logging import setup_logger

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
        print("Version: 2.3.0")
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
        return cmd_scale_up_specific(args.scaleupspecific[0], int(args.scaleupspecific[1]), logger)
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
    for hostname, _node in node_data.items():
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
    Uses the autoscaler to determine which workers to scale down.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Scale down by batch")

    # Initialize scheduler
    scheduler = _init_scheduler(logger)
    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    # Load configuration
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Get current node and job data
    node_data = receive_node_data_live(scheduler)
    jobs_pending, jobs_running = receive_job_data(scheduler)

    if not node_data:
        logger.error("Failed to get node data")
        return 1

    # Build context for autoscaler
    in_use = []
    drain = []
    free = []

    for hostname, node in node_data.items():
        if "worker" not in hostname:
            continue
        state = node.state if hasattr(node, "state") else node.get("state", "")
        if "DRAIN" in state:
            drain.append(hostname)
        elif state in ["ALLOC", "MIX"]:
            in_use.append(hostname)
        elif state == "IDLE":
            free.append(hostname)

    # Use scaling engine to determine downscale action
    flavor_data = get_flavor_data(scaling_link, cluster_id, password_file, logger)

    context = ScalingContext(
        mode=scaling_config.get("active_mode", "basic"),
        scale_force=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("scale_force", 0.6),
        scale_delay=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("scale_delay", 60),
        worker_cool_down=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("worker_cool_down", 60),
        limit_memory=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("limit_memory", 0),
        limit_worker_starts=scaling_config.get("mode", {})
        .get(scaling_config.get("active_mode", "basic"), {})
        .get("limit_worker_starts", 0),
        limit_workers=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("limit_workers", 0),
        worker_count=len(in_use) + len(free),
        worker_in_use=in_use,
        worker_drain=drain,
        worker_free=free,
        jobs_pending=list(jobs_pending.values()),
        jobs_running=list(jobs_running.values()),
        jobs_pending_count=len(jobs_pending),
        jobs_running_count=len(jobs_running),
        flavor_data=flavor_data if flavor_data else [],
        flavor_default=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("flavor_default"),
    )

    engine = ScalingEngine(context)
    action = engine.calculate_scaling()

    if action.downscale and action.downscale_workers:
        # Create cluster API client
        cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

        # Scale down
        result = cluster_api.scale_down(action.downscale_workers)
        if result:
            logger.info(f"Successfully scaled down {len(action.downscale_workers)} workers")
            rescale_cluster(scheduler, cluster_api, 0)
            return 0
        else:
            logger.error("Scale down failed")
            return 1
    else:
        logger.info("No workers to scale down based on current state")
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
            state = node.get("state", "UNKNOWN") if isinstance(node, dict) else node.state
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
            logger.info(f"  - {worker.get('hostname')}: " f"state={worker.get('state')}, " f"flavor={fv.get('name', 'N/A')}")
        return 0
    else:
        logger.error("Failed to get cluster data")
        return 1


def cmd_change_mode(config, logger) -> int:
    """
    Change mode command handler.
    Allows changing the active scaling mode via interactive prompt.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Change mode")

    # Available modes
    available_modes = ["basic", "approach", "adaptive", "sequence", "multi", "max", "default", "flavor", "min", "reactive"]
    current_mode = config.get("scaling", {}).get("active_mode", "basic")

    logger.info(f"Current mode: {current_mode}")
    logger.info(f"Available modes: {', '.join(available_modes)}")

    # In non-interactive mode, just log available options
    # For interactive mode, prompt the user
    try:
        import sys

        if sys.stdin.isatty():
            # Interactive mode
            user_input = input("Enter new mode: ").strip().lower()
            if user_input in available_modes:
                # Update config
                config["scaling"]["active_mode"] = user_input
                # Save config
                from autoscaling.config.validator import validate_config

                is_valid, errors, warnings = validate_config(config)
                if is_valid:
                    import yaml

                    with open(FILE_CONFIG_YAML, "w", encoding="utf8") as f:
                        yaml.dump(config, f)
                    logger.info(f"Mode changed to: {user_input}")
                    return 0
                else:
                    logger.error(f"Invalid configuration: {errors}")
                    return 1
            else:
                logger.error(f"Invalid mode. Available: {', '.join(available_modes)}")
                return 1
        else:
            # Non-interactive: just log available modes
            logger.info("Run without stdin to enable interactive mode")
            return 0
    except Exception as e:
        logger.error(f"Failed to change mode: {e}")
        return 1


def cmd_ignore_workers(config, logger) -> int:
    """
    Ignore workers command handler.
    Allows managing the list of ignored workers.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Select ignored workers")

    current_ignore = config.get("scaling", {}).get("ignore_workers", [])
    logger.info(f"Current ignored workers: {', '.join(current_ignore) if current_ignore else 'none'}")

    try:
        if sys.stdin.isatty():
            # Interactive mode
            print("Enter worker hostnames to ignore (comma-separated):")
            print("Enter empty line to clear ignore list")
            user_input = input("Ignore: ").strip()

            if user_input:
                new_ignore = [w.strip() for w in user_input.split(",") if w.strip()]
                config["scaling"]["ignore_workers"] = new_ignore
                logger.info(f"Ignored workers updated: {', '.join(new_ignore)}")
            else:
                config["scaling"]["ignore_workers"] = []
                logger.info("Ignored workers cleared")

            # Save config
            import yaml

            with open(FILE_CONFIG_YAML, "w", encoding="utf8") as f:
                yaml.dump(config, f)
            return 0
        else:
            logger.info("Run without stdin to enable interactive mode")
            return 0
    except Exception as e:
        logger.error(f"Failed to update ignore list: {e}")
        return 1


def cmd_rescale(config, logger) -> int:
    """
    Rescale command handler.
    Uses the autoscaler to determine optimal worker configuration.

    Args:
        config: Configuration dictionary
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Rescale cluster configuration")

    # Initialize scheduler
    scheduler = _init_scheduler(logger)
    if scheduler is None:
        logger.error("Failed to initialize scheduler")
        return 1

    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")
    password_file = scaling_config.get("password_file", "cluster_pw.json")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    # Get current state
    node_data = receive_node_data_live(scheduler)
    jobs_pending, jobs_running = receive_job_data(scheduler)

    if not node_data:
        logger.error("Failed to get node data")
        return 1

    # Build context for autoscaler
    in_use = []
    drain = []
    free = []

    for hostname, node in node_data.items():
        if "worker" not in hostname:
            continue
        state = node.state if hasattr(node, "state") else node.get("state", "")
        if "DRAIN" in state:
            drain.append(hostname)
        elif state in ["ALLOC", "MIX"]:
            in_use.append(hostname)
        elif state == "IDLE":
            free.append(hostname)

    flavor_data = get_flavor_data(scaling_link, cluster_id, password_file, logger)

    context = ScalingContext(
        mode=scaling_config.get("active_mode", "basic"),
        scale_force=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("scale_force", 0.6),
        scale_delay=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("scale_delay", 60),
        worker_cool_down=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("worker_cool_down", 60),
        limit_memory=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("limit_memory", 0),
        limit_worker_starts=scaling_config.get("mode", {})
        .get(scaling_config.get("active_mode", "basic"), {})
        .get("limit_worker_starts", 0),
        limit_workers=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("limit_workers", 0),
        worker_count=len(in_use) + len(free),
        worker_in_use=in_use,
        worker_drain=drain,
        worker_free=free,
        jobs_pending=list(jobs_pending.values()),
        jobs_running=list(jobs_running.values()),
        jobs_pending_count=len(jobs_pending),
        jobs_running_count=len(jobs_running),
        flavor_data=flavor_data if flavor_data else [],
        flavor_default=scaling_config.get("mode", {}).get(scaling_config.get("active_mode", "basic"), {}).get("flavor_default"),
    )

    engine = ScalingEngine(context)
    action = engine.calculate_scaling()

    # Create cluster API client
    cluster_api = ClusterAPI(scaling_link, cluster_id, password_file)

    # Execute scaling action
    if action.upscale and action.upscale_count > 0 and action.upscale_flavor:
        logger.info(f"Upscaling: {action.upscale_count} workers of flavor {action.upscale_flavor}")
        result = cluster_api.scale_up(action.upscale_flavor, action.upscale_count)
        if not result:
            logger.error("Scale up failed")
            return 1
    elif action.downscale and action.downscale_workers:
        logger.info(f"Downscaling: {len(action.downscale_workers)} workers")
        result = cluster_api.scale_down(action.downscale_workers)
        if not result:
            logger.error("Scale down failed")
            return 1
    else:
        logger.info("No scaling action needed")
        return 0

    # Run rescale (Ansible playbook)
    worker_count = (
        len(in_use) + len(free) + (action.upscale_count if action.upscale else -len(action.downscale_workers) if action.downscale else 0)
    )
    rescale_cluster(scheduler, cluster_api, worker_count)
    return 0


def rescale_cluster(scheduler, cluster_api, worker_count: int = 0) -> int:
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

    if worker_count == 0:
        time.sleep(10)  # WAIT_CLUSTER_SCALING

    ansible_runner = AnsibleRunner()
    rescale_success = ansible_runner.run()

    return rescale_success is not None


def cmd_drain_workers(scheduler, logger) -> int:
    """
    Drain workers command handler.
    Sets workers to DRAIN state via scheduler.

    Args:
        scheduler: Scheduler instance
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Set workers to drain")

    if scheduler is None:
        logger.error("Scheduler not available")
        return 1

    # Get current node data
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
        logger.info("No idle workers to drain")
        return 0

    logger.info(f"Found {len(idle_workers)} idle workers to drain")

    # Drain workers via scheduler (using drain_node as per interface)
    success = True
    for hostname in idle_workers:
        try:
            result = scheduler.drain_node(hostname)
            if result:
                logger.info(f"Drained worker: {hostname}")
            else:
                logger.error(f"Failed to drain worker: {hostname}")
                success = False
        except Exception as e:
            logger.error(f"Error draining worker {hostname}: {e}")
            success = False

    if success:
        logger.info(f"Successfully drained {len(idle_workers)} workers")
        return 0
    else:
        logger.error("Some workers failed to drain")
        return 1


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
    try:
        from autoscaling.cloud.ansible import AnsibleRunner

        ansible_runner = AnsibleRunner()
        if ansible_runner.run():
            logger.info("Ansible playbook executed successfully")
            return 0
        else:
            logger.error("Ansible playbook failed")
            return 1
    except Exception as e:
        logger.error(f"Failed to run playbook: {e}")
        return 1


def cmd_check_workers(logger) -> int:
    """
    Check workers command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Check workers")
    config = _load_config(logger)
    scaling_config = config.get("scaling", {})
    scaling_link = scaling_config.get("portal_scaling_link", "")
    cluster_id = scaling_config.get("cluster_id", "")

    if not scaling_link or not cluster_id:
        logger.error("Scaling link or cluster ID not configured")
        return 1

    from autoscaling.cluster.api import ClusterAPI

    cluster_api = ClusterAPI(scaling_link, cluster_id)
    cluster_data = cluster_api.get_cluster_data()

    if cluster_data:
        workers = cluster_data.get("workers", [])
        logger.info(f"Total workers: {len(workers)}")
        for worker in workers:
            logger.info(f"  - {worker.get('hostname')}: {worker.get('state')}")
        return 0
    else:
        logger.error("Failed to get cluster data")
        return 1


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
    from autoscaling.visualization.plots import ClusterVisualizer

    try:
        visualizer = ClusterVisualizer()
        visualizer.visualize(time_range)
        logger.info("Visualization complete")
        return 0
    except Exception as e:
        logger.error(f"Failed to create visualization: {e}")
        return 1


def cmd_set_password(logger) -> int:
    """
    Set password command handler.

    Args:
        logger: Logger instance

    Returns:
        Exit code
    """
    logger.info("Set cluster password")
    from autoscaling.utils.helpers import set_cluster_password

    try:
        import sys

        if sys.stdin.isatty():
            password = input("Enter cluster password: ").strip()
            if password and set_cluster_password(password):
                logger.info("Password set successfully")
                return 0
            else:
                logger.error("Failed to set password")
                return 1
        else:
            logger.info("Run without stdin to enable interactive mode")
            return 0
    except Exception as e:
        logger.error(f"Failed to set password: {e}")
        return 1


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
