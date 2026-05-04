"""
Unit tests for utils/filters.py
"""
import unittest
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from autoscaling.utils.filters import (
    filter_by_flavor,
    filter_by_state,
    filter_idle_workers,
    filter_active_workers,
    filter_draining_workers,
    filter_by_name_pattern,
    exclude_workers,
    get_worker_count_by_state,
)


class TestFilterByFlavor(unittest.TestCase):
    """Tests for filter_by_flavor function."""

    def test_filter_by_flavor_matches(self):
        """Test filtering workers by matching flavor."""
        workers = {
            "worker-1": {"flavor": {"name": "large"}},
            "worker-2": {"flavor": {"name": "small"}},
            "worker-3": {"flavor": {"name": "large"}},
        }

        result = filter_by_flavor(workers, "large")

        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-3", result)

    def test_filter_by_flavor_no_match(self):
        """Test filtering workers with no matching flavor."""
        workers = {
            "worker-1": {"flavor": {"name": "large"}},
            "worker-2": {"flavor": {"name": "small"}},
        }

        result = filter_by_flavor(workers, "medium")

        self.assertEqual(len(result), 0)

    def test_filter_by_flavor_custom_key(self):
        """Test filtering with custom flavor key."""
        workers = {
            "worker-1": {"instance_type": {"name": "large"}},
        }

        result = filter_by_flavor(workers, "large", flavor_key="instance_type")

        self.assertEqual(len(result), 1)
        self.assertIn("worker-1", result)

    def test_filter_by_flavor_empty_workers(self):
        """Test filtering empty workers dict."""
        result = filter_by_flavor({}, "large")

        self.assertEqual(len(result), 0)

    def test_filter_by_flavor_missing_flavor_key(self):
        """Test filtering when flavor key is missing."""
        workers = {
            "worker-1": {},
        }

        result = filter_by_flavor(workers, "large")

        self.assertEqual(len(result), 0)


class TestFilterByState(unittest.TestCase):
    """Tests for filter_by_state function."""

    def test_filter_by_state_exact_match(self):
        """Test filtering workers by exact state match."""
        workers = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "ALLOC"},
            "worker-3": {"state": "IDLE"},
        }

        result = filter_by_state(workers, "IDLE", include_drain=False)

        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-3", result)

    def test_filter_by_state_include_drain(self):
        """Test filtering with include_drain=True."""
        workers = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE,DRAIN"},
            "worker-3": {"state": "ALLOC"},
        }

        result = filter_by_state(workers, "IDLE", include_drain=True)

        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)

    def test_filter_by_state_exclude_drain(self):
        """Test filtering with include_drain=False."""
        workers = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE,DRAIN"},
            "worker-3": {"state": "ALLOC"},
        }

        result = filter_by_state(workers, "IDLE", include_drain=False)

        self.assertEqual(len(result), 1)
        self.assertIn("worker-1", result)

    def test_filter_by_state_empty_workers(self):
        """Test filtering empty workers dict."""
        result = filter_by_state({}, "IDLE")

        self.assertEqual(len(result), 0)

    def test_filter_by_state_missing_state(self):
        """Test filtering when state key is missing."""
        workers = {
            "worker-1": {},
        }

        result = filter_by_state(workers, "IDLE")

        self.assertEqual(len(result), 0)


class TestFilterIdleWorkers(unittest.TestCase):
    """Tests for filter_idle_workers function."""

    def test_filter_idle_workers(self):
        """Test filtering only idle workers."""
        workers = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "ALLOC"},
            "worker-3": {"state": "IDLE,DRAIN"},
            "worker-4": {"state": "IDLE"},
        }

        result = filter_idle_workers(workers)

        # Should only include pure IDLE, not DRAIN variants
        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-4", result)

    def test_filter_idle_workers_empty(self):
        """Test filtering empty workers."""
        result = filter_idle_workers({})

        self.assertEqual(len(result), 0)


class TestFilterActiveWorkers(unittest.TestCase):
    """Tests for filter_active_workers function."""

    def test_filter_active_workers(self):
        """Test filtering active workers (ALLOC or MIX)."""
        workers = {
            "worker-1": {"state": "ALLOC"},
            "worker-2": {"state": "MIX"},
            "worker-3": {"state": "IDLE"},
            "worker-4": {"state": "ALLOC"},
        }

        result = filter_active_workers(workers)

        self.assertEqual(len(result), 3)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)
        self.assertIn("worker-4", result)

    def test_filter_active_workers_empty(self):
        """Test filtering empty workers."""
        result = filter_active_workers({})

        self.assertEqual(len(result), 0)


class TestFilterDrainingWorkers(unittest.TestCase):
    """Tests for filter_draining_workers function."""

    def test_filter_draining_workers(self):
        """Test filtering draining workers."""
        workers = {
            "worker-1": {"state": "IDLE,DRAIN"},
            "worker-2": {"state": "ALLOC,DRAIN"},
            "worker-3": {"state": "IDLE"},
            "worker-4": {"state": "DRAIN"},
        }

        result = filter_draining_workers(workers)

        self.assertEqual(len(result), 3)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)
        self.assertIn("worker-4", result)

    def test_filter_draining_workers_empty(self):
        """Test filtering empty workers."""
        result = filter_draining_workers({})

        self.assertEqual(len(result), 0)


class TestFilterByNamePattern(unittest.TestCase):
    """Tests for filter_by_name_pattern function."""

    def test_filter_by_name_pattern(self):
        """Test filtering by hostname pattern."""
        workers = {
            "worker-1": {},
            "worker-2": {},
            "compute-1": {},
            "node-1": {},
        }

        result = filter_by_name_pattern(workers, "worker")

        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)

    def test_filter_by_name_pattern_no_match(self):
        """Test filtering with no matching pattern."""
        workers = {
            "worker-1": {},
            "worker-2": {},
        }

        result = filter_by_name_pattern(workers, "compute")

        self.assertEqual(len(result), 0)

    def test_filter_by_name_pattern_empty(self):
        """Test filtering empty workers."""
        result = filter_by_name_pattern({}, "worker")

        self.assertEqual(len(result), 0)


class TestExcludeWorkers(unittest.TestCase):
    """Tests for exclude_workers function."""

    def test_exclude_workers(self):
        """Test excluding specific workers."""
        workers = {
            "worker-1": {},
            "worker-2": {},
            "worker-3": {},
            "worker-4": {},
        }

        result = exclude_workers(workers, ["worker-2", "worker-4"])

        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-3", result)

    def test_exclude_workers_empty_list(self):
        """Test excluding with empty list."""
        workers = {
            "worker-1": {},
        }

        result = exclude_workers(workers, [])

        self.assertEqual(len(result), 1)
        self.assertIn("worker-1", result)

    def test_exclude_workers_empty_workers(self):
        """Test excluding from empty workers."""
        result = exclude_workers({}, ["worker-1"])

        self.assertEqual(len(result), 0)

    def test_exclude_workers_all_excluded(self):
        """Test excluding all workers."""
        workers = {
            "worker-1": {},
        }

        result = exclude_workers(workers, ["worker-1"])

        self.assertEqual(len(result), 0)


class TestGetWorkerCountByState(unittest.TestCase):
    """Tests for get_worker_count_by_state function."""

    def test_get_worker_count_by_state(self):
        """Test counting workers by state."""
        workers = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
            "worker-3": {"state": "ALLOC"},
            "worker-4": {"state": "MIX"},
            "worker-5": {"state": "DRAIN"},
            "worker-6": {"state": "DOWN"},
            "worker-7": {"state": "UNKNOWN"},
        }

        result = get_worker_count_by_state(workers)

        self.assertEqual(result["IDLE"], 2)
        self.assertEqual(result["ALLOC"], 1)
        self.assertEqual(result["MIX"], 1)
        self.assertEqual(result["DRAIN"], 1)
        self.assertEqual(result["DOWN"], 1)
        self.assertEqual(result["OTHER"], 1)

    def test_get_worker_count_by_state_empty(self):
        """Test counting empty workers."""
        result = get_worker_count_by_state({})

        for state in result:
            self.assertEqual(result[state], 0)

    def test_get_worker_count_by_state_missing_state(self):
        """Test counting when state is missing."""
        workers = {
            "worker-1": {},
        }

        result = get_worker_count_by_state(workers)

        self.assertEqual(result["OTHER"], 1)


if __name__ == '__main__':
    unittest.main()
