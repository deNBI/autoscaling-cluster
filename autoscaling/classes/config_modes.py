from dataclasses import dataclass, field
from typing import Any


@dataclass
class BasicMode:
    service_frequency: int = 60
    limit_memory: int = 0
    limit_worker_starts: int = 0
    limit_workers: int = 0
    scale_force: float = 0.6
    scale_delay: int = 60
    limit_flavor_usage: list[str] = field(default_factory=list)
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
    flavor_restriction: int = 0
    flavor_default: str = "de.NBI tiny"
    flavor_ephemeral: bool = True
    flavor_gpu: int = 1
    flavor_depth: int = -1
    large_flavors: bool = False
    large_flavors_except_hmf: bool = True
    auto_activate_large_flavors: int = 10
    drain_large_nodes: bool = False
    drain_only_hmf: bool = False
    drain_delay: int = 0
    info: str = (
        "start multiple flavors for 60% pending jobs, no limitations, no time forecast"
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert the mode's attributes to a dictionary."""
        return self.__dict__


@dataclass
class ApproachMode(BasicMode):
    info: str = (
        "start workers for 60% pending jobs, approach by 10 workers per flavor and service frequency"
    )
    limit_worker_starts: int = 10
    scale_force: float = 0.6


@dataclass
class AdaptiveMode(BasicMode):
    info: str = (
        "forecast job time, start worker one flavor ahead in queue, set max 50% hmf worker per service request to drain"
    )
    limit_worker_starts: int = 10
    smoothing_coefficient: float = 0.05
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.5
    flavor_depth: int = 1
    drain_large_nodes: bool = True
    drain_only_hmf: bool = True
    drain_delay: int = -1


@dataclass
class SequenceMode(BasicMode):
    info: str = (
        "forecast job time, start worker for the next jobs in queue with the same flavor"
    )
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.5


@dataclass
class MultiMode(BasicMode):
    info: str = (
        "forecast job time, start multiple flavors, automatic higher (threshold 10)"
    )
    smoothing_coefficient: float = 0.01
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.5
    flavor_depth: int = -1
    large_flavors: bool = False
    large_flavors_except_hmf: bool = True
    auto_activate_large_flavors: int = 10


@dataclass
class MaxMode(BasicMode):
    info: str = "forecast job time, maximum worker - no flavor separation"
    worker_cool_down: int = 60
    worker_weight: float = 0.0
    smoothing_coefficient: float = 0.0
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.5
    flavor_restriction: float = 0.5
    flavor_depth: int = -2


@dataclass
class DefaultMode(BasicMode):
    info: str = (
        "forecast job time, start multiple flavors, prefer higher flavors (except high memory), smooth time"
    )
    scale_delay: int = 100
    worker_cool_down: int = 70
    smoothing_coefficient: float = 0.01
    forecast_by_flavor_history: bool = False
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    forecast_occupied_worker: bool = True
    job_time_threshold: float = 0.5
    flavor_depth: int = -1
    large_flavors: bool = True
    large_flavors_except_hmf: bool = True


@dataclass
class FlavorMode(BasicMode):
    info: str = (
        "forecast job time only on flavor level, start multiple flavors, prefer higher flavors (except high memory)"
    )
    limit_worker_starts: int = 10
    smoothing_coefficient: float = 0.0
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = False
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.4
    flavor_depth: int = 0
    large_flavors: bool = False
    large_flavors_except_hmf: bool = True
    auto_activate_large_flavors: int = 10


@dataclass
class MinMode(BasicMode):
    info: str = (
        "forecast job time, start a single flavor, aggressive worker drain and scale-down"
    )
    limit_worker_starts: int = 10
    worker_cool_down: int = 0
    worker_weight: float = 0.1
    smoothing_coefficient: float = 0.0
    forecast_by_flavor_history: bool = True
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 1
    job_time_threshold: float = 0.6
    flavor_depth: int = 0
    drain_large_nodes: bool = True
    drain_only_hmf: bool = False
    drain_delay: int = 0


@dataclass
class ReactiveMode(BasicMode):
    info: str = (
        "forecast job time, start multiple flavors, prefer higher flavors (except high memory) + more workers per runtime + without active worker forecast"
    )
    scale_force: float = 0.8
    smoothing_coefficient: float = 0.02
    forecast_by_flavor_history: bool = False
    forecast_by_job_history: bool = True
    forecast_active_worker: int = 0
    job_time_threshold: float = 0.2
    flavor_depth: int = -1
    large_flavors: bool = True
    large_flavors_except_hmf: bool = True
