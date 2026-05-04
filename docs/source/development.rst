# Development Guide

Complete guide for developers contributing to the Autoscaling Cluster project.

## Prerequisites

- Python 3.9+
- pip (package manager)
- Git

## Development Setup

```bash
# Clone the repository
git clone https://github.com/deNBI/autoscaling-cluster.git
cd autoscaling-cluster

# Install in development mode
pip install -e .

# Install dev dependencies
pip install pytest ruff black mypy sphinx sphinx-rtd-theme sphinx-autodoc-typehints
```

## Project Structure

```
autoscaling/
├── config/           # Configuration management
│   ├── loader.py     # YAML config loader with env var override
│   ├── defaults.py   # Default values and mode presets
│   └── validator.py  # Config validation
├── core/             # Core scaling logic
│   ├── autoscaler.py     # Orchestrates full scaling cycle
│   ├── scaling_engine.py # Decision logic: calculate upscale/downscale
│   ├── scale_actions.py  # Execution: run actual scale operations
│   ├── state.py          # Dataclasses: ScalingContext, ScalingAction
│   └── flavors.py        # Flavor matching and filtering
├── scheduler/        # Scheduler abstraction
│   ├── interface.py     # SchedulerInterface ABC
│   ├── slurm.py         # Slurm implementation (pyslurm + squeue/sinfo)
│   ├── job_data.py      # Job processing and sorting
│   └── node_data.py     # Node processing and filtering
├── cloud/            # Cloud API access
│   ├── client.py        # Portal REST API client
│   ├── ansible.py       # Ansible playbook runner
│   └── exceptions.py    # Cloud-specific exceptions
├── data/             # Job history database
│   ├── models.py        # Data classes: JobHistory, FlavorStats
│   └── manager.py       # Database read/write operations
├── utils/            # Shared utilities
│   ├── logging.py       # Logger setup
│   ├── converters.py    # Unit conversions
│   ├── filters.py       # Worker/job filtering
│   └── helpers.py       # General helpers
├── cluster/          # Cluster API
│   ├── api.py           # Cluster operations
│   └── flavors.py       # Flavor processing
├── cli/              # Command line interface
│   ├── parser.py        # Argument parser setup
│   └── commands.py      # Command handlers
├── services/         # Background services
│   ├── runner.py        # Service lifecycle management
│   └── watcher.py       # Resource metrics collection
└── visualization/    # Data visualization
    └── plots.py         # Matplotlib-based plots
```

## Running the Service Locally

```bash
# Set up config
cp autoscaling_config.yaml.example autoscaling_config.yaml
# Edit config with your cluster settings

# Set cluster password
autoscaling -p

# Run as service
python -c "from autoscaling.services.runner import run_service; run_service()"

# Run a single cycle
python -c "
from autoscaling.core.autoscaler import create_autoscaler
from autoscaling.scheduler.slurm import SlurmScheduler
scheduler = SlurmScheduler()
autoscaler = create_autoscaler(scheduler, 'autoscaling_config.yaml')
action = autoscaler.run_once()
print(f'Action: {action}')
"
```

## Code Standards

### Formatting and Linting

```bash
ruff check autoscaling/        # Linting
black --check autoscaling/     # Formatting
python -m mypy autoscaling/    # Type checking
```

All three must pass for PR submission. Auto-fix with:

```bash
ruff check --fix autoscaling/
black autoscaling/
```

### Writing Tests

```bash
# Run all tests
pytest tests/ -v

# Run single test file
pytest tests/test_core.py -v

# Run with coverage
pytest --cov=autoscaling tests/

# Run with HTML coverage report
pytest --cov=autoscaling --cov-report=html tests/
```

### Docstrings

Use Google-style docstrings:

```python
def process_job(job_id: int) -> dict:
    """
    Process a single job.

    Args:
        job_id: The job identifier

    Returns:
        Dictionary with processed job data

    Raises:
        ValueError: When job_id is negative
    """
```

## Adding a New CLI Command

1. Add argument in `autoscaling/cli/parser.py`:

```python
parser.add_argument(
    "--my-command",
    dest="my_command",
    action="store_true",
    help="My new command",
)
```

2. Add routing in `autoscaling/cli/commands.py::run_command()`:

```python
elif args.my_command:
    return cmd_my_command(logger)
```

3. Implement handler:

```python
def cmd_my_command(logger) -> int:
    logger.info("My command executed")
    return 0
```

## Adding a New Scheduler Backend

1. Create `autoscaling/scheduler/<name>.py`:

```python
from autoscaling.scheduler.interface import (
    SchedulerInterface,
    SchedulerJobState,
    SchedulerNodeState,
)

class MyScheduler(SchedulerInterface):
    def test_connection(self) -> bool:
        # Check if scheduler is reachable
        ...

    def get_node_data(self) -> Optional[dict[str, SchedulerNodeState]]:
        # Return node data from database
        ...

    def get_job_data(self, days: int) -> Optional[dict[int, SchedulerJobState]]:
        # Return job history
        ...

    def get_node_data_live(self) -> Optional[dict[str, SchedulerNodeState]]:
        # Return live node data
        ...

    def get_job_data_live(self) -> Optional[dict[int, SchedulerJobState]]:
        # Return live job data
        ...

    def drain_node(self, node_name: str) -> bool:
        # Set node to drain
        ...

    def resume_node(self, node_name: str) -> bool:
        # Resume a drained node
        ...
```

2. No changes needed in other modules — the interface is abstract.

## Adding a New Scaling Mode

1. Add mode defaults in `autoscaling/config/defaults.py`:

```python
MODE_DEFAULTS = {
    # ... existing modes ...
    "my_mode": ModeDefaults(
        info="My custom mode description",
        scale_force=0.8,
        limit_worker_starts=5,
        # ... other settings ...
    ),
}
```

2. Set as default: `active_mode: my_mode` in config

## Debugging Tips

### Check config is loaded correctly

```python
from autoscaling.config.loader import ConfigLoader
config = ConfigLoader("autoscaling_config.yaml").load()
print(config)
```

### Check scheduler connection

```python
from autoscaling.scheduler.slurm import SlurmScheduler
s = SlurmScheduler()
print(s.test_connection())
print(s.get_node_data_live())
```

### Test scaling engine in isolation

```python
from autoscaling.core.scaling_engine import ScalingEngine
from autoscaling.core.state import ScalingContext

context = ScalingContext(
    mode="basic",
    scale_force=0.6,
    scale_delay=60,
    worker_cool_down=60,
    limit_memory=0,
    limit_worker_starts=10,
    limit_workers=0,
    worker_count=5,
    worker_in_use=[],
    worker_drain=[],
    worker_free=["worker-01"],
    jobs_pending=[],
    jobs_running=[],
    jobs_pending_count=0,
    jobs_running_count=0,
    flavor_data=[],
)

engine = ScalingEngine(context)
action = engine.calculate_scaling()
print(f"Action: {action}")
```

## Building Documentation

```bash
cd docs
make html          # Build HTML docs
make doctest       # Run doctests
make clean         # Remove build artifacts
```

Output: `docs/build/html/index.html`
