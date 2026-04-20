"""
Database management for job history and forecasting.
"""

import json
import logging
import os
import time
from typing import Any, Optional

from autoscaling.utils.helpers import AUTOSCALING_VERSION, DATABASE_FILE

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULT_HISTORY_RECALL = 7
DATA_LONG_TIME = 7


class JobDatabase:
    """
    Database for job history and forecasting data.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize database.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.database_file = DATABASE_FILE
        self.update_time: int = 0
        self.config_hash: str = ""
        self.data: dict[str, Any] = {}
        self._smoothing_coefficient = float(config.get("smoothing_coefficient", 0.0))

    def load(self, config_hash: str) -> bool:
        """
        Load database from file.

        Args:
            config_hash: Hash of current config for version check

        Returns:
            True if loaded successfully, False if fresh database needed
        """
        if not os.path.exists(self.database_file):
            logger.info("Database file not found, will create new")
            return False

        try:
            with open(self.database_file, "r", encoding="utf-8") as f:
                self.data = json.load(f)

            # Check version and config hash
            if (
                self.data.get("VERSION") != AUTOSCALING_VERSION
                or self.data.get("config_hash") != config_hash
            ):
                logger.info("Config or version changed, recreating database")
                if self.config.get("database_reset", False):
                    self.data = {}
                    return False
                # Update hash but keep data
                self.data["config_hash"] = config_hash

            self.update_time = self.data.get("update_time", 0)
            self.config_hash = config_hash
            logger.info("Loaded database from %s", self.database_file)
            return True

        except (json.JSONDecodeError, IOError) as exc:
            logger.error("Error loading database: %s", exc)
            return False

    def save(self) -> bool:
        """
        Save database to file.

        Returns:
            True on success
        """
        try:
            self.data["update_time"] = int(time.time())
            self.data["config_hash"] = self.config_hash
            self.data["VERSION"] = AUTOSCALING_VERSION

            with open(self.database_file, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)

            logger.debug("Saved database to %s", self.database_file)
            return True

        except IOError as exc:
            logger.error("Error saving database: %s", exc)
            return False

    def create_new(self, config_hash: str, flavor_names: list) -> None:
        """
        Create a new database.

        Args:
            config_hash: Hash of current config
            flavor_names: List of flavor names to initialize
        """
        self.data = {
            "VERSION": AUTOSCALING_VERSION,
            "config_hash": config_hash,
            "update_time": int(time.time()) - (self._get_history_recall() * 86400),
            "flavor_name": {fv: self._get_empty_flavor_data() for fv in flavor_names},
        }
        self.config_hash = config_hash
        logger.info("Created new database with %d flavors", len(flavor_names))

    def _get_empty_flavor_data(self) -> dict[str, Any]:
        """Get empty flavor data structure."""
        return {
            "fv_time_norm": 0.0,
            "fv_time_avg": 0,
            "fv_time_sum": 0,
            "fv_time_cnt": 0,
            "similar_data": {},
        }

    def _get_history_recall(self) -> int:
        """Get history recall days from config."""
        return self.config.get("history_recall", DEFAULT_HISTORY_RECALL)

    def update_with_jobs(self, jobs: dict[int, Any], flavor_data: list) -> None:
        """
        Update database with new job data.

        Args:
            jobs: Dictionary of completed jobs
            flavor_data: List of available flavors
        """
        if not jobs:
            logger.info("No jobs to update database with")
            return

        current_time = int(time.time())
        last_update = self.data.get("update_time", 0)

        logger.info(
            "Updating database with jobs from %s to %s", last_update, current_time
        )

        for job_id, job_data in jobs.items():
            if not self._is_valid_job(job_data, last_update):
                continue

            job_name = self._clear_job_name(job_data)
            v_tmp = self._translate_to_flavor(job_data, flavor_data)

            if v_tmp is None:
                continue

            fv_name = self._clear_flavor_name(v_tmp["flavor"]["name"])
            job_time = self._calc_job_time_norm(job_data["elapsed"], 1)

            self._update_flavor_data(fv_name, job_data, job_time)
            self._update_job_similarity(fv_name, job_name, job_data)

        self.save()

    def _is_valid_job(self, job_data: dict[str, Any], last_update: int) -> bool:
        """Check if job is valid for database update."""
        return (
            job_data.get("state") == 3  # JOB_FINISHED
            and int(job_data.get("end", 0)) >= last_update
            and int(job_data.get("elapsed", -1)) >= 0
        )

    def _translate_to_flavor(
        self, job_data: dict[str, Any], flavor_data: list
    ) -> Optional[dict[str, Any]]:
        """Translate job requirements to a flavor."""
        from autoscaling.utils.converter import translate_metrics_to_flavor

        return translate_metrics_to_flavor(
            job_data.get("req_cpus", 0),
            job_data.get("req_mem", 0),
            job_data.get("temporary_disk", 0),
            flavor_data,
            False,
            True,
        )

    def _update_flavor_data(
        self, fv_name: str, job_data: dict[str, Any], job_time: float
    ) -> None:
        """Update flavor-specific data in database."""
        if fv_name not in self.data["flavor_name"]:
            self.data["flavor_name"][fv_name] = self._get_empty_flavor_data()
            logger.debug("Initialized flavor %s in database", fv_name)

        flavor_data = self.data["flavor_name"][fv_name]
        flavor_data["fv_time_cnt"] += 1
        flavor_data["fv_time_sum"] += job_data["elapsed"]
        flavor_data["fv_time_avg"] = job_data["elapsed"]

        if self._smoothing_coefficient != 0:
            flavor_data["fv_time_norm"] = self._smooth_time(
                flavor_data["fv_time_norm"],
                job_data["elapsed"],
                self._smoothing_coefficient,
            )

    def _update_job_similarity(
        self, fv_name: str, job_name: str, job_data: dict[str, Any]
    ) -> None:
        """Update job similarity data."""
        job_match_value = float(self.config.get("job_match_value", 0.95))
        similar_data = self.data["flavor_name"][fv_name]["similar_data"]

        for existing_name, existing_data in similar_data.items():
            if self._similarity_match(job_name, existing_name, job_match_value):
                logger.debug("Job %s matches existing %s", job_name, existing_name)
                return

        similar_data[job_name] = {
            "fv_time_norm": job_data.get("norm_time", 0),
            "fv_time_avg": job_data.get("elapsed", 0),
        }

    def _similarity_match(self, name1: str, name2: str, threshold: float) -> bool:
        """Check if job names are similar enough."""
        import difflib

        return difflib.SequenceMatcher(None, name1, name2).ratio() > threshold

    def _clear_job_name(self, job_data: dict[str, Any]) -> str:
        """Clear job name for matching."""
        jobname = job_data.get("jobname", "")
        # Remove numbers
        if self.config.get("job_name_remove_numbers", True):
            import re

            jobname = re.sub(r"\(\d+\)", "", jobname)
            jobname = re.sub(r"\d+", "", jobname)
        return jobname.strip()

    def _clear_flavor_name(self, fv_name: str) -> str:
        """Clear flavor name pattern."""
        pattern = self.config.get("DATABASE_WORKER_PATTERN", " + ephemeral")
        if pattern and fv_name.endswith(pattern):
            return fv_name[: -len(pattern)]
        return fv_name

    def _calc_job_time_norm(self, job_time: int, job_num: int) -> float:
        """Calculate normalized job time."""
        return float(job_time) / float(job_num) if job_num > 0 else 0.0

    def _smooth_time(
        self, f_t: float, current_time: int, smoothing_coefficient: float
    ) -> float:
        """Calculate exponential smoothing."""

        a_t = self._calc_job_time_norm(current_time, 1)
        return smoothing_coefficient * a_t + (1 - smoothing_coefficient) * f_t

    def get_flavor_time(self, flavor_name: str) -> Optional[float]:
        """Get forecasted time for a flavor."""
        fv_name = self._clear_flavor_name(flavor_name)
        if fv_name in self.data.get("flavor_name", {}):
            return self.data["flavor_name"][fv_name].get("fv_time_norm")
        return None

    def get_job_time(
        self, flavor_name: str, job_name: str, multi_flavor: bool = False
    ) -> Optional[dict[str, Any]]:
        """Get job time data from database."""
        fv_name = self._clear_flavor_name(flavor_name)
        job_match_value = float(self.config.get("job_match_value", 0.95))

        similar_data = (
            self.data.get("flavor_name", {}).get(fv_name, {}).get("similar_data", {})
        )

        for current_job, job_data in similar_data.items():
            if self._similarity_match(job_name, current_job, job_match_value):
                logger.debug("Found matching job: %s", current_job)
                return job_data

        if multi_flavor:
            # Search other flavors
            for flavor_tmp in self.data.get("flavor_name", {}):
                if flavor_tmp == fv_name:
                    continue
                similar_data = self.data["flavor_name"][flavor_tmp].get(
                    "similar_data", {}
                )
                for current_job, job_data in similar_data.items():
                    if self._similarity_match(job_name, current_job, job_match_value):
                        logger.debug("Found matching job in flavor %s", flavor_tmp)
                        return job_data

        return None
