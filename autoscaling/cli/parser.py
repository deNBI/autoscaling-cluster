"""
Argument parser for autoscaling CLI.
"""

import argparse


def create_argument_parser() -> argparse.ArgumentParser:
    """
    Create the argument parser for autoscaling.

    Returns:
        ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        prog="autoscaling",
        description="Autoscaling tool for deNBI clusters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  autoscaling                    # Run as service
  autoscaling -s                 # Run as service
  autoscaling -t                 # Test mode
  autoscaling -su 2              # Scale up 2 workers
  autoscaling -sdc               # Scale down choice
  autoscaling -reset             # Reset configuration

Modes:
  - basic     : Start multiple flavors for 60% pending jobs
  - approach  : Start workers with approach of 10 workers per flavor
  - adaptive  : Forecast job time, start worker one flavor ahead
  - sequence  : Start worker for next jobs in queue with same flavor
  - multi     : Forecast job time, start multiple flavors
  - max       : Maximum worker - no flavor separation
  - default   : Default mode with multi-flavor support
  - flavor    : Forecast on flavor level only
  - min       : Aggressive drain and scale-down
  - reactive  : More workers per runtime without active worker forecast
""",
    )

    # Version
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="Print version and exit",
    )

    # Service modes
    parser.add_argument(
        "-s",
        "--service",
        action="store_true",
        help="Run as a service",
    )

    parser.add_argument(
        "-systemd",
        action="store_true",
        help="Run with systemd support",
    )

    # Single run modes
    parser.add_argument(
        "-su",
        "--scaleup",
        type=int,
        metavar="COUNT",
        help="Scale up by COUNT workers",
    )

    parser.add_argument(
        "-sus",
        "--scaleupspecific",
        nargs=2,
        metavar=("FLAVOR", "COUNT"),
        help="Scale up specific flavor by COUNT workers",
    )

    parser.add_argument(
        "-suc",
        "--scaleupchoice",
        action="store_true",
        help="Scale up by choice",
    )

    parser.add_argument(
        "-sd",
        "--scaledown",
        action="store_true",
        help="Scale down idle workers",
    )

    parser.add_argument(
        "-sds",
        "--scaledownspecific",
        metavar="HOSTNAMES",
        help="Scale down specific workers by hostname",
    )

    parser.add_argument(
        "-sdc",
        "--scaledownchoice",
        action="store_true",
        help="Scale down by choice",
    )

    parser.add_argument(
        "-sdb",
        "--scaledownbatch",
        action="store_true",
        help="Scale down by batch",
    )

    # Info modes
    parser.add_argument(
        "-nd",
        "--node",
        action="store_true",
        help="Show node data",
    )

    parser.add_argument(
        "-j",
        "--jobdata",
        action="store_true",
        help="Show job data",
    )

    parser.add_argument(
        "-jh",
        "--jobhistory",
        action="store_true",
        help="Show job history",
    )

    parser.add_argument(
        "-fv",
        "--flavor",
        action="store_true",
        help="Show available flavors",
    )

    parser.add_argument(
        "-c",
        "--clusterdata",
        action="store_true",
        help="Show cluster data",
    )

    # Configuration modes
    parser.add_argument(
        "-m",
        "--mode",
        action="store_true",
        help="Change active mode",
    )

    parser.add_argument(
        "-i",
        "--ignore",
        action="store_true",
        help="Select ignored workers",
    )

    # Admin modes
    parser.add_argument(
        "-rsc",
        "--rescale",
        action="store_true",
        help="Rescale cluster configuration",
    )

    parser.add_argument(
        "-d",
        "--drain",
        action="store_true",
        help="Set workers to drain",
    )

    parser.add_argument(
        "-pb",
        "--playbook",
        action="store_true",
        help="Run Ansible playbook",
    )

    parser.add_argument(
        "-cw",
        "--checkworker",
        action="store_true",
        help="Check workers",
    )

    # Utility modes
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="Test autoscaling functions",
    )

    parser.add_argument(
        "-clean",
        action="store_true",
        help="Clean log data",
    )

    parser.add_argument(
        "-reset",
        action="store_true",
        help="Reset autoscaling",
    )

    parser.add_argument(
        "-u",
        "-update",
        "--update",
        action="store_true",
        help="Update autoscaling",
    )

    parser.add_argument(
        "-stop",
        action="store_true",
        help="Stop service",
    )

    parser.add_argument(
        "-kill",
        action="store_true",
        help="Kill service",
    )

    parser.add_argument(
        "-status",
        action="store_true",
        help="Show service status",
    )

    parser.add_argument(
        "-visual",
        metavar="TIME_RANGE",
        nargs="?",
        const="",
        help="Visualize cluster data",
    )

    parser.add_argument(
        "-password",
        "-p",
        "--password",
        action="store_true",
        help="Set cluster password",
    )

    return parser
