#!/usr/bin/env python3
"""
Main entry point for autoscaling.
This is the new entry point for the refactored autoscaling package.

Usage:
    python main.py                    # Run as service
    python main.py -s                 # Run as service
    python main.py -t                 # Test mode
    python main.py -su 2              # Scale up 2 workers
    python main.py -sdc               # Scale down choice
"""
import os
import sys

# Add the package directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from autoscaling.cli.parser import create_argument_parser
from autoscaling.cli.commands import run_command


def main():
    """Main entry point."""
    # Create argument parser
    parser = create_argument_parser()

    # Parse arguments
    args = parser.parse_args()

    # Run command
    exit_code = run_command(args, parser)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
