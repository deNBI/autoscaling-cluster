# autoscaling-cluster

Intelligent autoscaling tool for deNBI cloud clusters. Monitors Slurm scheduler queues and automatically provisions or deprovisions cloud workers based on pending job requirements.

## Quick Start

```bash
# Install
pip install -e .

# Set cluster password
autoscaling -p

# Run as service (checks every 60s)
autoscaling -s

# Scale up manually
autoscaling -su 2

# Show available commands
autoscaling -h
```

## Package Structure

```
autoscaling/
├── config/           # YAML config loader, defaults, validator
├── core/             # Scaling engine, state management, orchestrator
├── scheduler/        # Scheduler abstraction (Slurm)
├── cloud/            # Portal API client, Ansible runner
├── data/             # Job history database (models, manager)
├── utils/            # Logging, converters, filters, helpers
├── cluster/          # Cluster API client, flavor handling
├── cli/              # Argument parser, command handlers
├── services/         # Service runner, resource watcher
└── visualization/    # Data plotting
```

## CLI Commands

```
autoscaling -v            # Print version
autoscaling -h            # Show help
autoscaling -s            # Run as service
autoscaling -su <N>       # Scale up N workers
autoscaling -sus <F> <N>  # Scale up N workers of flavor F
autoscaling -suc          # Scale up by choice
autoscaling -sd           # Scale down idle workers
autoscaling -sds <HOSTS>  # Scale down specific workers
autoscaling -sdc          # Scale down by choice
autoscaling -sdb          # Scale down by batch
autoscaling -m            # Change active mode
autoscaling -i            # Manage ignored workers
autoscaling -d            # Drain idle workers
autoscaling -rsc          # Rescale cluster
autoscaling -pb           # Run Ansible playbook
autoscaling -flavors      # Show available flavors
autoscaling -clusterdata  # Show cluster info
autoscaling -node         # Show node status
autoscaling -jobdata      # Show job queue
autoscaling -reset        # Reset database and logs
autoscaling -clean        # Clean log files
autoscaling -p            # Set cluster password
autoscaling -v            # Show version
```

## Scaling Modes

| Mode | Description |
|------|-------------|
| `basic` | Default mode, scales by pending jobs and resources |
| `approach` | Limits to 10 workers per flavor per cycle |
| `adaptive` | Balanced with job history and staged drain |
| `multi` | Multiple flavors per cycle, best for most workloads |
| `sequence` | Workers for next jobs of same flavor |
| `reactive` | Light time threshold, no active worker forecast |
| `max` | Only largest required flavor |
| `min` | Aggressive drain, single flavor |
| `flavor` | Flavor-level time forecasting |
| `default` | Balanced multi-flavor with smoothing |

## Configuration

Configuration file: `autoscaling_config.yaml`

```yaml
scaling:
  active_mode: basic
  portal_scaling_link: "https://..."
  portal_webapp_link: "https://..."
  scheduler: slurm
  service_frequency: 60
  mode:
    basic:
      scale_force: 0.6
      limit_worker_starts: 10
      limit_workers: 0
      flavor_depth: 0
```

## Development

```bash
# Install dependencies
pip install -e .

# Run tests
pytest tests/ -v

# Coverage
pytest --cov=autoscaling tests/

# Linting
ruff check autoscaling/
black --check autoscaling/

# Type checking
python -m mypy autoscaling/

# Build docs
cd docs && make html
```

## Architecture

- **SchedulerInterface** — Abstraction layer for different schedulers (Slurm, PBS, LSF)
- **ScalingEngine** — Core decision logic: analyzes pending jobs and available workers
- **Autoscaler** — Orchestrates the full scaling cycle
- **ClusterAPI** — Cloud portal API client for scale-up/scale-down operations
- **DatabaseManager** — Job history for time-based forecasting

## Documentation

- [API Reference](docs/source/index.rst) — Full module documentation
- [Architecture Diagrams](docs/architecture.md) — System diagrams
- [Contributing](CONTRIBUTING.md) — Development guidelines

## QA Status

- **Linting**: Ruff clean
- **Formatting**: Black compliant
- **Type Checking**: mypy clean
- **Tests**: 254 passing
