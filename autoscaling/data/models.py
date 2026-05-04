"""
Data models for autoscaling database.
"""

from dataclasses import dataclass, field


@dataclass
class JobHistory:
    """
    History data for a job.
    """

    job_name: str
    flavor_name: str
    elapsed_time: int  # in seconds
    normalized_time: float
    timestamp: int  # Unix timestamp


@dataclass
class FlavorStats:
    """
    Statistics for a flavor.
    """

    flavor_name: str
    time_norm: float  # Normalized time
    time_avg: int  # Average time
    time_sum: int  # Sum of all times
    time_cnt: int  # Count of jobs
    similar_jobs: dict = field(default_factory=dict)  # job_name -> JobHistory


@dataclass
class DatabaseMetadata:
    """
    Metadata for the database.
    """

    version: str
    config_hash: str
    update_time: int  # Unix timestamp of last update


@dataclass
class DatabaseContent:
    """
    Complete database content.
    """

    metadata: DatabaseMetadata
    flavor_stats: dict[str, FlavorStats] = field(default_factory=dict)
