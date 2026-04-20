#!/usr/bin/env python3
"""
Legacy entry point for the autoscaling cluster package.

This file exists for backward compatibility with the old monolithic
implementation. It imports and uses the new modular structure.

For new development, please use the autoscaling package directly:
    from autoscaling.main import main

Usage:
    python autoscaling.py [options]
"""

import sys

# Use the new modular structure
from autoscaling.main import main

if __name__ == "__main__":
    sys.exit(main())
