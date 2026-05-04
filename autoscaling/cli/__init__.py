"""
CLI module for autoscaling.
"""

from autoscaling.cli.commands import run_command
from autoscaling.cli.parser import create_argument_parser

__all__ = ["create_argument_parser", "run_command"]
