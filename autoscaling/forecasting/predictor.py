"""
Job time prediction and forecasting.
"""

import difflib
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class JobPredictor:
    """
    Predicts job execution times using history and smoothing.
    """

    def __init__(self, config: dict[str, Any], database: Any = None):
        """
        Initialize predictor.

        Args:
            config: Configuration dictionary
            database: Job database instance
        """
        self.config = config
        self.database = database
        self.job_match_value = float(config.get("job_match_value", 0.95))

    def get_job_time_from_database(
        self,
        dict_db: dict[str, Any],
        flavor_name: str,
        job_name: str,
        multi_flavor: bool = False,
    ) -> Optional[dict[str, Any]]:
        """
        Get job time data from database.

        Args:
            dict_db: Database dictionary
            flavor_name: Flavor name to search
            job_name: Job name to match
            multi_flavor: Search across all flavors

        Returns:
            Job time data, or None if not found
        """
        fv_name = self._clear_flavor_name(flavor_name)
        logger.debug("Searching in history: job=%s, flavor=%s", job_name, fv_name)

        # Search in specified flavor first
        if fv_name in dict_db.get("flavor_name", {}):
            similar_data = dict_db["flavor_name"][fv_name].get("similar_data", {})
            for current_job, job_data in similar_data.items():
                if self._similarity_match(job_name, current_job):
                    logger.debug(
                        "Found match: diff_match=%.2f, job=%s, current=%s",
                        difflib.SequenceMatcher(None, job_name, current_job).ratio(),
                        job_name,
                        current_job,
                    )
                    return job_data

        # Search other flavors if requested
        if multi_flavor:
            for flavor_tmp in dict_db.get("flavor_name", {}):
                if flavor_tmp == fv_name:
                    continue
                similar_data = dict_db["flavor_name"][flavor_tmp].get(
                    "similar_data", {}
                )
                for current_job, job_data in similar_data.items():
                    if self._similarity_match(job_name, current_job):
                        logger.debug(
                            "Found match in flavor %s: job=%s",
                            flavor_tmp,
                            current_job,
                        )
                        return job_data

        return None

    def get_flavor_time_from_database(
        self, dict_db: dict[str, Any], flavor_name: str
    ) -> Optional[float]:
        """
        Get forecasted time for a flavor from database.

        Args:
            dict_db: Database dictionary
            flavor_name: Flavor name

        Returns:
            Forecasted time, or None if not found
        """
        fv_name = self._clear_flavor_name(flavor_name)
        flavor_data = dict_db.get("flavor_name", {}).get(fv_name)
        if flavor_data:
            return flavor_data.get("fv_time_norm")
        return None

    def smooth_flavor_time(
        self, dict_db: dict[str, Any], current_flavor: str, smoothing_coefficient: float
    ) -> float:
        """
        Calculate forecast with exponential smoothing.

        Formula: f_(t+1) = alpha * a_t + (1-alpha) * f_t

        Args:
            dict_db: Database dictionary
            current_flavor: Flavor name
            smoothing_coefficient: Alpha value (0-1)

        Returns:
            Smoothed forecast value
        """
        flavor_data = dict_db.get("flavor_name", {}).get(current_flavor)
        if not flavor_data:
            return 0.0

        f_t = flavor_data.get("fv_time_norm", 0)
        a_t = flavor_data.get("fv_time_avg", 0)

        f_t1 = smoothing_coefficient * a_t + (1 - smoothing_coefficient) * f_t
        logger.debug("Exponential smoothing %s: f_t1=%.4f", current_flavor, f_t1)
        return f_t1

    def smooth_time(
        self, f_t: float, current_time: int, smoothing_coefficient: float
    ) -> float:
        """
        Calculate exponential smoothing for a single time value.

        Args:
            f_t: Previous forecast
            current_time: Current time value
            smoothing_coefficient: Alpha value

        Returns:
            Smoothed forecast
        """
        a_t = self._calc_job_time_norm(current_time, 1)
        f_t1 = smoothing_coefficient * a_t + (1 - smoothing_coefficient) * f_t
        return f_t1

    def predict_job_time(
        self,
        job: dict[str, Any],
        flavor_name: str,
        dict_db: Optional[dict[str, Any]] = None,
        flavor_data: Optional[dict[str, Any]] = None,
    ) -> float:
        """
        Predict job execution time.

        Args:
            job: Job data
            flavor_name: Flavor name
            dict_db: Database dictionary
            flavor_data: Flavor data for forecasting

        Returns:
            Predicted time in normalized units
        """
        if dict_db:
            # Try to match job from history
            job_name = self._clear_job_name(job)
            job_data = self.get_job_time_from_database(
                dict_db, flavor_name, job_name, False
            )
            if job_data:
                return job_data.get("fv_time_norm", 0)

            # Fallback to flavor history
            if self.config.get("forecast_by_flavor_history", False):
                flavor_time = self.get_flavor_time_from_database(dict_db, flavor_name)
                if flavor_time:
                    return flavor_time

        return 0.0

    def _similarity_match(self, name1: str, name2: str) -> bool:
        """Check if job names are similar enough."""
        return (
            difflib.SequenceMatcher(None, name1, name2).ratio() > self.job_match_value
        )

    def _clear_job_name(self, job: dict[str, Any]) -> str:
        """Clear job name for matching."""
        jobname = job.get("jobname", "")

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


class JobTimeCalculator:
    """
    Calculates job time metrics.
    """

    def __init__(self, config: dict[str, Any]):
        """
        Initialize calculator.

        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.norm_high = None
        self.norm_low = 0.0001

    def calc_job_time_norm(self, job_time_sum: int, job_num: int) -> float:
        """
        Calculate normalized job time.

        Args:
            job_time_sum: Sum of job times
            job_num: Number of jobs

        Returns:
            Normalized time value
        """
        time_range_max = int(self.config.get("time_range_max", 3600))
        time_range_min = int(self.config.get("time_range_min", 60))

        try:
            job_time_sum = int(job_time_sum)
            job_num = int(job_num)

            if job_num > 0 and job_time_sum > 1:
                job_time_avg = self._division_round(job_time_sum, job_num)
                norm = float(job_time_avg - time_range_min) / float(
                    time_range_max - time_range_min
                )

                if norm <= self.norm_low:
                    return self.norm_low
                return norm

        except ZeroDivisionError:
            pass

        return self.norm_low

    def get_time_border(self) -> tuple:
        """Return time classification values."""
        return self.norm_high, self.norm_low

    def _division_round(self, num: float, div: float) -> int:
        """Calculate division with rounding."""
        try:
            return int((float(num) / float(div)) + 0.5)
        except ZeroDivisionError:
            return 0
