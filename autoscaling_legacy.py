#!/usr/bin/env python3
"""
Legacy wrapper for the autoscaling cluster package.

This file exists for backward compatibility. It imports and uses
the new modular structure from the autoscaling package.

For new development, please use the autoscaling package directly.
"""

import sys

# Use the new modular structure
from autoscaling.main import main

if __name__ == "__main__":
    sys.exit(main())
