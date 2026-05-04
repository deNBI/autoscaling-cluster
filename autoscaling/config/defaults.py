"""
Default configuration values for autoscaling.
"""

from dataclasses import dataclass, field
from typing import Optional

# Type aliases for better readability
FlavorNameList = list[str]
FlavorUsageDict = dict[str, int]


@dataclass
class ModeDefaults:
    """
    Default values for each scaling mode.
    """

    info: str = ""
    service_frequency: int = 60
    limit_memory: int = 0
    limit_worker_starts: int = 10
    limit_workers: int = 0
    allowed_flavor_names: list[str] = field(default_factory=list)
    limit_flavor_usage: dict[str, int] = field(default_factory=dict)
    scale_force: float = 0.6
    scale_delay: int = 60
    worker_cool_down: int = 60
    worker_weight: float = 0.0
    smoothing_coefficient: float = 0.0
    forecast_by_flavor_history: bool = False
    job_match_value: float = 0.95
    job_time_threshold: float = 0.5
    job_name_remove_numbers: bool = True
    job_name_remove_num_brackets: bool = True
    job_name_remove_pattern: str = ""
    job_name_remove_text_within_parentheses: bool = True
    forecast_by_job_history: bool = False
    forecast_active_worker: int = 0
    forecast_occupied_worker: bool = False
    flavor_restriction: float = 0.0
    flavor_default: str = "de.NBI small"
    flavor_ephemeral: bool = False
    flavor_gpu: int = 1
    flavor_depth: int = 0
    large_flavors: bool = False
    large_flavors_except_hmf: bool = True
    auto_activate_large_flavors: int = 10
    drain_large_nodes: bool = False
    drain_only_hmf: bool = False
    drain_delay: int = 0


@dataclass
class GlobalDefaults:
    """
    Global default values.
    """

    # Portal configuration
    portal_scaling_link: str = "https://simplevm.denbi.de/portal/api/autoscaling/"
    portal_webapp_link: str = "https://simplevm.denbi.de/portal/webapp/#/clusters/overview"
    scaling_script_url: str = "https://raw.githubusercontent.com/deNBI/user_scripts/refs/heads/master/bibigrid/scaling.py"

    # Scheduler configuration
    scheduler: str = "slurm"

    # Active mode
    active_mode: str = "basic"

    # System configuration
    automatic_update: bool = True
    database_reset: bool = False
    pattern_id: str = ""
    history_recall: int = 7
    ignore_workers: Optional[list[str]] = None
    pending_jobs_percent: float = 1.0
    resource_sorting: bool = True

    # Database configuration
    database_file: str = "autoscaling_database.json"
    time_range_max: int = 1000
    time_range_min: int = 1

    # Log configuration
    log_file: str = "autoscaling.log"
    log_csv: str = "autoscaling.csv"
    log_level: str = "INFO"

    # Cluster configuration
    cluster_password_file: str = "cluster_pw.json"
    cluster_id: Optional[str] = None

    # Playbook paths
    playbook_dir: str = "~/playbook"
    playbook_vars_dir: str = "~/playbook/vars"

    # Version
    autoscaling_version: str = "2.3.0"

    def __post_init__(self):
        if self.ignore_workers is None:
            self.ignore_workers = []


# Default configuration instance
DEFAULTS = GlobalDefaults()

# Mode defaults mapping
MODE_DEFAULTS = {
    "basic": ModeDefaults(
        info="start multiple flavors for 60% pending jobs, no limitations, no time forecast",
        service_frequency=60,
        limit_memory=0,
        limit_worker_starts=10,
        limit_workers=0,
        limit_flavor_usage={},
        scale_force=0.6,
        scale_delay=60,
        worker_cool_down=60,
        smoothing_coefficient=0.0,
        forecast_by_flavor_history=False,
        forecast_by_job_history=False,
        flavor_depth=0,
        large_flavors=False,
        large_flavors_except_hmf=True,
        auto_activate_large_flavors=10,
    ),
    "approach": ModeDefaults(
        info="start workers for 60% pending jobs, approach by 10 workers per flavor and service frequency",
        limit_worker_starts=10,
        scale_force=0.6,
    ),
    "adaptive": ModeDefaults(
        info="forecast job time, start worker one flavor ahead in queue, set max 50% hmf worker per service request to drain",
        limit_worker_starts=10,
        smoothing_coefficient=0.05,
        forecast_by_flavor_history=True,
        forecast_by_job_history=True,
        forecast_active_worker=1,
        flavor_depth=1,
        drain_large_nodes=True,
        drain_only_hmf=True,
        drain_delay=-1,
    ),
    "sequence": ModeDefaults(
        info="forecast job time, start worker for the next jobs in queue with the same flavor",
        forecast_by_flavor_history=True,
        forecast_by_job_history=True,
        forecast_active_worker=1,
    ),
    "multi": ModeDefaults(
        info="forecast job time, start multiple flavors, automatic higher (threshold 10)",
        smoothing_coefficient=0.01,
        forecast_by_flavor_history=True,
        forecast_by_job_history=True,
        forecast_active_worker=1,
        flavor_depth=-1,
        large_flavors=False,
        large_flavors_except_hmf=True,
        auto_activate_large_flavors=10,
    ),
    "max": ModeDefaults(
        info="forecast job time, maximum worker - no flavor separation",
        worker_cool_down=60,
        worker_weight=0.0,
        smoothing_coefficient=0.0,
        forecast_by_flavor_history=True,
        forecast_by_job_history=True,
        forecast_active_worker=1,
        flavor_restriction=0.5,
        flavor_depth=-2,
    ),
    "default": ModeDefaults(
        info="forecast job time, start multiple flavors, prefer higher flavors (except high memory), smooth time",
        scale_delay=100,
        worker_cool_down=70,
        smoothing_coefficient=0.01,
        forecast_by_flavor_history=False,
        forecast_by_job_history=True,
        forecast_active_worker=1,
        forecast_occupied_worker=True,
        flavor_depth=-1,
        large_flavors=True,
        large_flavors_except_hmf=True,
    ),
    "flavor": ModeDefaults(
        info="forecast job time only on flavor level, start multiple flavors, prefer higher flavors (except high memory)",
        limit_worker_starts=10,
        smoothing_coefficient=0.0,
        forecast_by_flavor_history=True,
        forecast_by_job_history=False,
        forecast_active_worker=1,
        flavor_depth=0,
        large_flavors=False,
        large_flavors_except_hmf=True,
        auto_activate_large_flavors=10,
    ),
    "min": ModeDefaults(
        info="forecast job time, start a single flavor, aggressive worker drain and scale-down",
        limit_worker_starts=10,
        worker_cool_down=0,
        worker_weight=0.1,
        smoothing_coefficient=0.0,
        forecast_by_flavor_history=True,
        forecast_by_job_history=True,
        forecast_active_worker=1,
        flavor_depth=0,
        drain_large_nodes=True,
        drain_only_hmf=False,
        drain_delay=0,
    ),
    "reactive": ModeDefaults(
        info="forecast job time, start multiple flavors, prefer higher flavors (except high memory) + more workers per runtime + without active worker forecast",
        scale_force=0.8,
        smoothing_coefficient=0.02,
        forecast_by_flavor_history=False,
        forecast_by_job_history=True,
        forecast_active_worker=0,
        flavor_depth=-1,
        large_flavors=True,
        large_flavors_except_hmf=True,
    ),
}
