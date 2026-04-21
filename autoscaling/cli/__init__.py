"""
CLI module for autoscaling.
"""
from autoscaling.cli.parser import create_argument_parser
from autoscaling.cli.commands import run_command

__all__ = ["create_argument_parser", "run_command"]
