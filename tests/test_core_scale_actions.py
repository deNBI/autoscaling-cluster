"""
Unit tests for the scale_actions module.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock

from autoscaling.core import scale_actions
from autoscaling.core.state import ScaleState, Rescale


class TestClassifyJobsToFlavors(unittest.TestCase):
    """Tests for classify_jobs_to_flavors function."""

    def test_classify_jobs_groups_by_flavor(self):
        """Test that jobs are grouped by flavor."""
        flavor_data = [
            {"flavor": {"name": "small", "vcpus": 2, "ram_gib": 4, "ephemeral_disk": 10, "usable_count": 10}},
            {"flavor": {"name": "medium", "vcpus": 4, "ram_gib": 8, "ephemeral_disk": 20, "usable_count": 10}},
            {"flavor": {"name": "large", "vcpus": 8, "ram_gib": 16, "ephemeral_disk": 40, "usable_count": 10}},
        ]

        job_priority = [
            (1, {"req_cpus": 2, "req_mem": 4096}),
            (2, {"req_cpus": 2, "req_mem": 4096}),
            (3, {"req_cpus": 2, "req_mem": 4096}),
        ]

        result = scale_actions.classify_jobs_to_flavors(job_priority, flavor_data)

        # Should create one group with 3 jobs for a flavor
        self.assertEqual(len(result), 1)
        count, flavor, jobs = result[0]
        self.assertEqual(count, 3)
        self.assertIn("flavor", flavor)
        self.assertEqual(jobs, [1, 2, 3])

    def test_classify_jobs_different_flavors(self):
        """Test that jobs with different flavor needs are separated."""
        flavor_data = [
            {"flavor": {"name": "small", "vcpus": 2, "ram_gib": 4, "ephemeral_disk": 10, "usable_count": 10}},
            {"flavor": {"name": "medium", "vcpus": 4, "ram_gib": 8, "ephemeral_disk": 20, "usable_count": 10}},
            {"flavor": {"name": "large", "vcpus": 8, "ram_gib": 16, "ephemeral_disk": 40, "usable_count": 10}},
        ]

        job_priority = [
            (1, {"req_cpus": 2, "req_mem": 4096}),
            (2, {"req_cpus": 2, "req_mem": 4096}),
            (3, {"req_cpus": 8, "req_mem": 16384}),
            (4, {"req_cpus": 4, "req_mem": 8192}),
        ]

        result = scale_actions.classify_jobs_to_flavors(job_priority, flavor_data)

        # Should create multiple groups
        self.assertGreater(len(result), 0)

    def test_classify_jobs_no_flavor_matches(self):
        """Test when no flavors match job requirements."""
        flavor_data = [
            {"flavor": {"name": "small", "vcpus": 2, "ram_gib": 4, "ephemeral_disk": 10, "usable_count": 10}},
        ]

        job_priority = [
            (1, {"req_cpus": 16, "req_mem": 65536}),
        ]

        result = scale_actions.classify_jobs_to_flavors(job_priority, flavor_data)

        self.assertEqual(len(result), 0)

    def test_classify_jobs_empty_flavor_data(self):
        """Test with empty flavor_data."""
        job_priority = [
            (1, {"req_cpus": 2, "req_mem": 4096}),
        ]

        result = scale_actions.classify_jobs_to_flavors(job_priority, [])

        self.assertEqual(len(result), 0)

    def test_classify_jobs_flavor_without_name(self):
        """Test handling of flavor without name field in flavor_data."""
        flavor_data = [
            {"flavor": {}},  # No name field
            {"flavor": {"name": "small", "vcpus": 2, "ram_gib": 4, "ephemeral_disk": 10, "usable_count": 10}},
        ]

        job_priority = [
            (1, {"req_cpus": 2, "req_mem": 4096}),
        ]

        result = scale_actions.classify_jobs_to_flavors(job_priority, flavor_data)

        # Should skip the flavor without name and match the small one
        self.assertEqual(len(result), 1)


class TestGenerateDownscaleList(unittest.TestCase):
    """Tests for _generate_downscale_list function."""

    def test_generate_list_selects_idle_workers(self):
        """Test that idle workers are selected first."""
        worker_json = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
            "worker-3": {"state": "IDLE"},
            "worker-4": {"state": "ALLOC"},
            "headnode": {"state": "IDLE"},
        }

        result = scale_actions._generate_downscale_list(worker_json, 2, None)

        # Should select 2 idle workers
        self.assertEqual(len(result), 2)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)
        # Should not include headnode or worker-4
        self.assertNotIn("headnode", result)
        self.assertNotIn("worker-4", result)

    def test_generate_list_includes_drain_workers(self):
        """Test that drain workers are included after idle."""
        worker_json = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE+DRAIN"},
            "worker-3": {"state": "ALLOC+DRAIN"},
        }

        result = scale_actions._generate_downscale_list(worker_json, 3, None)

        # Should include all worker candidates (not headnode)
        self.assertEqual(len(result), 3)
        self.assertIn("worker-1", result)
        self.assertIn("worker-2", result)
        self.assertIn("worker-3", result)

    def test_generate_list_limits_to_requested_number(self):
        """Test that result is limited to worker_num."""
        worker_json = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
            "worker-3": {"state": "IDLE"},
            "worker-4": {"state": "IDLE"},
        }

        result = scale_actions._generate_downscale_list(worker_json, 2, None)

        self.assertEqual(len(result), 2)

    def test_generate_list_with_jobs_dict(self):
        """Test that jobs_dict is accepted (not used in current impl)."""
        worker_json = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
        }

        jobs_dict = {1: {"state": "PENDING"}}
        result = scale_actions._generate_downscale_list(worker_json, 2, jobs_dict)

        self.assertEqual(len(result), 2)

    def test_generate_list_empty_worker_json(self):
        """Test with empty worker_json."""
        result = scale_actions._generate_downscale_list({}, 5, None)
        self.assertEqual(len(result), 0)

    def test_generate_list_no_idle_workers(self):
        """Test when no idle workers exist."""
        worker_json = {
            "worker-1": {"state": "ALLOC"},
            "worker-2": {"state": "MIX"},
        }

        result = scale_actions._generate_downscale_list(worker_json, 2, None)
        self.assertEqual(len(result), 0)


class TestGetWorkerMemoryUsage(unittest.TestCase):
    """Tests for _get_worker_memory_usage function."""

    def test_get_memory_usage_sum(self):
        """Test summing memory usage from all workers."""
        worker_json = {
            "worker-1": {"real_memory": 16384},
            "worker-2": {"real_memory": 32768},
            "worker-3": {"real_memory": 8192},
        }

        result = scale_actions._get_worker_memory_usage(worker_json)

        self.assertEqual(result, 16384 + 32768 + 8192)  # 57344

    def test_get_memory_usage_empty(self):
        """Test with empty worker_json."""
        result = scale_actions._get_worker_memory_usage({})
        self.assertEqual(result, 0)

    def test_get_memory_usage_missing_field(self):
        """Test workers without real_memory field."""
        worker_json = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"real_memory": 8192},
        }

        result = scale_actions._get_worker_memory_usage(worker_json)
        self.assertEqual(result, 8192)

    def test_get_memory_usage_zero(self):
        """Test workers with zero memory."""
        worker_json = {
            "worker-1": {"real_memory": 0},
            "worker-2": {"real_memory": 0},
        }

        result = scale_actions._get_worker_memory_usage(worker_json)
        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main()
