#!/usr/bin/env python3
"""
Legacy entry point for autoscaling.
This file bridges the old single-file autoscaling to the new module structure.
It will be removed once the refactoring is complete.

For now, this file imports and delegates to the new main.py to ensure
backward compatibility with existing CI/CD and deployment scripts.
"""
import os
import sys

# Add the package directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import and run the new main function
from main import main

if __name__ == "__main__":
    main()
