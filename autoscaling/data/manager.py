"""
Database manager for autoscaling.
Handles job history and flavor statistics.
"""

import json
import os
from pathlib import Path
from typing import Optional

from autoscaling.data.models import (
    DatabaseContent,
    DatabaseMetadata,
    FlavorStats,
    JobHistory,
)


class DatabaseManager:
    """
    Manager for the autoscaling database.
    Handles reading, writing, and updating job history.
    """

    def __init__(self, database_file: str):
        """
        Initialize the database manager.

        Args:
            database_file: Path to the database JSON file
        """
        self.database_file = database_file
        self._data: Optional[DatabaseContent] = None

    def load(self) -> Optional[DatabaseContent]:
        """
        Load database from file.

        Returns:
            Database content or None if file not found
        """
        if not os.path.exists(self.database_file):
            return None

        try:
            with open(self.database_file, encoding="utf8") as f:
                data = json.load(f)

            # Parse metadata
            metadata = DatabaseMetadata(
                version=data.get("VERSION", ""),
                config_hash=data.get("config_hash", ""),
                update_time=data.get("update_time", 0),
            )

            # Parse flavor stats
            flavor_stats = {}
            for fv_name, fv_data in data.get("flavor_name", {}).items():
                similar_jobs = {}
                for job_name, job_data in fv_data.get("similar_data", {}).items():
                    similar_jobs[job_name] = JobHistory(
                        job_name=job_name,
                        flavor_name=fv_name,
                        elapsed_time=job_data.get("elapsed", 0),
                        normalized_time=job_data.get("fv_time_norm", 0.0),
                        timestamp=job_data.get("timestamp", 0),
                    )

                flavor_stats[fv_name] = FlavorStats(
                    flavor_name=fv_name,
                    time_norm=fv_data.get("fv_time_norm", 0.0),
                    time_avg=fv_data.get("fv_time_avg", 0),
                    time_sum=fv_data.get("fv_time_sum", 0),
                    time_cnt=fv_data.get("fv_time_cnt", 0),
                    similar_jobs=similar_jobs,
                )

            self._data = DatabaseContent(
                metadata=metadata,
                flavor_stats=flavor_stats,
            )

            return self._data
        except (json.JSONDecodeError, OSError) as e:
            print(f"Error loading database: {e}")
            return None

    def save(self, data: DatabaseContent) -> bool:
        """
        Save database to file.

        Args:
            data: Database content to save

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            db_path = Path(self.database_file)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            # Build output structure
            output = {
                "VERSION": data.metadata.version,
                "config_hash": data.metadata.config_hash,
                "update_time": data.metadata.update_time,
                "flavor_name": {},
            }

            for fv_name, fv_data in data.flavor_stats.items():
                similar_data = {}
                for job_name, job_data in fv_data.similar_jobs.items():
                    similar_data[job_name] = {
                        "elapsed": job_data.elapsed_time,
                        "fv_time_norm": job_data.normalized_time,
                        "timestamp": job_data.timestamp,
                    }

                output["flavor_name"][fv_name] = {  # type: ignore[index]
                    "fv_time_norm": fv_data.time_norm,
                    "fv_time_avg": fv_data.time_avg,
                    "fv_time_sum": fv_data.time_sum,
                    "fv_time_cnt": fv_data.time_cnt,
                    "similar_data": similar_data,
                }

            with open(self.database_file, "w", encoding="utf8") as f:
                json.dump(output, f, indent=2)

            return True
        except OSError as e:
            print(f"Error saving database: {e}")
            return False

    def get_flavor_stats(self, flavor_name: str) -> Optional[FlavorStats]:
        """
        Get statistics for a specific flavor.

        Args:
            flavor_name: Name of the flavor

        Returns:
            Flavor statistics or None if not found
        """
        if self._data is None:
            return None

        return self._data.flavor_stats.get(flavor_name)

    def update_flavor_time(
        self,
        flavor_name: str,
        elapsed_time: int,
        normalized_time: float,
        job_name: str,
    ) -> bool:
        """
        Update flavor statistics with new job data.

        Args:
            flavor_name: Name of the flavor
            elapsed_time: Job elapsed time in seconds
            normalized_time: Normalized job time
            job_name: Name of the job

        Returns:
            True if successful, False otherwise
        """
        if self._data is None:
            return False

        fv_name = self._normalize_flavor_name(flavor_name)

        if fv_name not in self._data.flavor_stats:
            # Initialize new flavor
            self._data.flavor_stats[fv_name] = FlavorStats(
                flavor_name=fv_name,
                time_norm=normalized_time,
                time_avg=elapsed_time,
                time_sum=elapsed_time,
                time_cnt=1,
                similar_jobs={},
            )
        else:
            # Update existing flavor
            fv_stats = self._data.flavor_stats[fv_name]
            fv_stats.time_sum += elapsed_time
            fv_stats.time_cnt += 1
            fv_stats.time_avg = int(fv_stats.time_sum / fv_stats.time_cnt)
            fv_stats.time_norm = normalized_time

        # Add to similar jobs
        self._data.flavor_stats[fv_name].similar_jobs[job_name] = JobHistory(
            job_name=job_name,
            flavor_name=fv_name,
            elapsed_time=elapsed_time,
            normalized_time=normalized_time,
            timestamp=int(utc_timestamp()),
        )

        return self.save(self._data)

    def _normalize_flavor_name(self, flavor_name: str) -> str:
        """
        Normalize flavor name by removing suffixes.

        Args:
            flavor_name: Original flavor name

        Returns:
            Normalized flavor name
        """
        suffix = " ephemeral"
        if flavor_name.endswith(suffix):
            return flavor_name[: -len(suffix)]
        return flavor_name

    def get_job_history(
        self,
        flavor_name: str,
        job_name: str,
        match_threshold: float = 0.95,
    ) -> Optional[JobHistory]:
        """
        Find job history by name with fuzzy matching.

        Args:
            flavor_name: Name of the flavor
            job_name: Name of the job
            match_threshold: Matching threshold (0.0 to 1.0)

        Returns:
            Job history or None if not found
        """
        if self._data is None:
            return None

        fv_name = self._normalize_flavor_name(flavor_name)

        # First, try exact match in specified flavor
        if fv_name in self._data.flavor_stats:
            for name, history in self._data.flavor_stats[fv_name].similar_jobs.items():
                if name == job_name:
                    assert isinstance(history, JobHistory)
                    return history

        # Then, try fuzzy match across all flavors
        for _fv_name_check, fv_stats in self._data.flavor_stats.items():
            for name, history in fv_stats.similar_jobs.items():
                if self._fuzzy_match(job_name, name, match_threshold):
                    assert isinstance(history, JobHistory)
                    return history

        return None

    def _fuzzy_match(self, s1: str, s2: str, threshold: float) -> bool:
        """
        Check if two strings match above threshold.

        Uses simple character comparison for matching.

        Args:
            s1: First string
            s2: Second string
            threshold: Matching threshold

        Returns:
            True if strings match above threshold
        """
        # Simple similarity calculation
        if s1 == s2:
            return True

        if not s1 or not s2:
            return False

        # Character-level similarity
        longer = max(len(s1), len(s2))
        if longer == 0:
            return True

        # Simple character comparison
        matches = sum(1 for c1, c2 in zip(s1, s2) if c1 == c2)
        similarity = matches / longer

        return similarity >= threshold

    def get_version(self) -> str:
        """
        Get database version.

        Returns:
            Version string
        """
        if self._data is None:
            return ""
        return self._data.metadata.version

    def get_update_time(self) -> int:
        """
        Get last update time.

        Returns:
            Unix timestamp of last update
        """
        if self._data is None:
            return 0
        return self._data.metadata.update_time

    def update_metadata(self, version: str, config_hash: str) -> bool:
        """
        Update database metadata.

        Args:
            version: New version string
            config_hash: New config hash

        Returns:
            True if successful, False otherwise
        """
        if self._data is None:
            return False

        self._data.metadata.version = version
        self._data.metadata.config_hash = config_hash
        self._data.metadata.update_time = int(utc_timestamp())

        return self.save(self._data)

    def reset(self) -> bool:
        """
        Reset database to empty state.

        Returns:
            True if successful, False otherwise
        """
        self._data = DatabaseContent(
            metadata=DatabaseMetadata(
                version="",
                config_hash="",
                update_time=0,
            ),
            flavor_stats={},
        )
        return self.save(self._data)


def utc_timestamp() -> int:
    """Get current UTC timestamp."""
    import time

    return int(time.time())
