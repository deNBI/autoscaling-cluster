# Autoscaling Cluster Documentation

Welcome to the Autoscaling Cluster project documentation.

## About

Autoscaling Cluster is an intelligent autoscaling tool for deNBI cloud clusters. It monitors
Slurm scheduler queues and automatically provisions/deprovisions cloud workers based on
pending job requirements.

## Quick Start

```bash
# Install
pip install -e .

# Set cluster password
autoscaling -p

# Run as service
autoscaling -s

# Scale up manually
autoscaling -su 2
```

## Documentation

- [Getting Started](index) — Overview and quick start
- [Development Guide](development) — Developer onboarding and contributing

## API Reference

Explore the package modules in the navigation panel.

## Indices and tables

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
