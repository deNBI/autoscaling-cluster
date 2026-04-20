# Autoscaling Cluster Package

A modular Python package for auto-scaling compute clusters with support for
multiple schedulers (Slurm) and cloud providers.

## Features

- **Modular Architecture**: Clean separation of concerns with dedicated modules
- **Slurm Scheduler Integration**: Full support for Slurm job scheduler
- **Portal API Integration**: Connect to SimpleVM cloud portal for cluster management
- **Job Forecasting**: Predict job execution times using history data
- **Adaptive Scaling**: Multiple scaling modes for different use cases
- **CLI Interface**: Command-line interface for manual control

## Installation

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install the package
pip install -e .
```

## Package Structure

```
autoscaling/
├── __init__.py           # Package initialization and exports
├── main.py               # Main CLI entry point
├── autoscaling.py        # Legacy wrapper (backward compatibility)
├── cli/                  # CLI command handlers
│   ├── __init__.py
│   └── commands.py       # Individual command implementations
├── config/               # Configuration management
│   ├── __init__.py
│   └── loader.py         # Config loading and validation
├── core/                 # Core scaling logic
│   ├── __init__.py
│   ├── state.py          # State enums (ScaleState, Rescale)
│   ├── scaling_engine.py # Main scaling engine
│   ├── upscale.py        # Scale-up operations
│   └── downscale.py      # Scale-down operations
├── data/                 # Data models and fetchers
│   ├── __init__.py
│   ├── models.py         # Dataclasses (Job, Worker, Flavor)
│   ├── fetcher.py        # Data fetching from scheduler
│   └── database.py       # Job history database
├── api/                  # API clients
│   ├── __init__.py
│   ├── client.py         # Portal API client
│   └── flavor.py         # Flavor management
├── scheduler/            # Scheduler interfaces
│   ├── __init__.py
│   ├── base.py           # Abstract base class
│   └── slurm.py          # Slurm implementation
├── forecasting/          # Job time prediction
│   ├── __init__.py
│   └── predictor.py      # Job time predictor
└── utils/                # Utility functions
    ├── __init__.py
    ├── helpers.py        # Common helpers
    ├── converter.py      # Unit conversions
    └── logger.py         # Logging setup
```

## Quick Start

```python
from autoscaling.main import main

# Run autoscaling with CLI arguments
main()
```

Command-line usage:
```bash
# Show version
autoscaling --version

# Set cluster password
autoscaling --password

# Run rescale
autoscaling --rescale

# Display node data
autoscaling --node-data

# Display job data
autoscaling --job-data

# Interactive mode selection
autoscaling --mode
```

## Configuration

Configuration is stored in `autoscaling_config.yaml` in the autoscaling folder.

```yaml
scaling:
  portal_scaling_link: "https://cloud.denbi.de/portal/api/autoscaling"
  portal_webapp_link: "https://cloud.denbi.de/portal/webapp/#/clusters/overview"
  scheduler: "slurm"
  active_mode: "basic"
  ignore_workers: []

  # Scaling parameters
  scale_force: 0.6
  scale_delay: 60
  worker_cool_down: 60

  # Forecasting
  smoothing_coefficient: 0.05
  forecast_by_flavor_history: true
  forecast_by_job_history: true

  # Flavor settings
  flavor_gpu: 1  # 0: no change, 1: remove GPU flavors, -1: only GPU flavors
```

## Scaling Modes

- **basic**: Basic scaling based on pending jobs
- **approach**: With worker weight limiting
- **sequence**: By job sequence analysis
- **adaptive**: Balanced approach with drain support
- **multi**: Multiple flavor support
- **reactive**: Light threshold mode
- **flavor**: Per-flavor limiting
- **maximum**: Only largest flavors
- **minimum**: Minimum worker usage

## Testing

Run the test suite:
```bash
source .venv/bin/activate
pytest tests/ -v
```

## Development

### Adding New Features

1. Create a new module in the appropriate directory
2. Add proper docstrings and type hints
3. Add tests in `tests/`
4. Export in `autoscaling/__init__.py`

### Code Style

- Follow PEP 8 style guidelines
- Use type hints where possible
- Add docstrings for all public functions
- Keep modules focused on single responsibility

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request
