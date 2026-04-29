"""
Unit tests for scheduler helper functions.
"""
import unittest

from autoscaling.scheduler.interface import (
    SchedulerInterface,
    SchedulerNodeState,
    SchedulerJobState,
)
from autoscaling.scheduler.node_data import (
    receive_node_data_live,
    receive_node_data_live_uncut,
    node_filter,
)
from autoscaling.scheduler.job_data import (
    receive_job_data,
    sort_job_priority,
    sort_job_by_resources,
)


class TestSchedulerNodeDataHelper(unittest.TestCase):
    """Tests for scheduler node data helper functions."""

    def test_node_filter_removes_workers(self):
        """Test that node_filter removes specified workers."""
        node_dict = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
            "worker-3": {"state": "ALLOC"},
        }

        filtered = node_filter(node_dict, ["worker-1", "worker-3"])

        self.assertNotIn("worker-1", filtered)
        self.assertIn("worker-2", filtered)
        self.assertNotIn("worker-3", filtered)

    def test_node_filter_empty_filter(self):
        """Test node_filter with empty ignore list."""
        node_dict = {"worker-1": {"state": "IDLE"}, "worker-2": {"state": "IDLE"}}

        filtered = node_filter(node_dict, [])

        self.assertEqual(len(filtered), 2)

    def test_node_filter_none_filter(self):
        """Test node_filter with None filter."""
        node_dict = {"worker-1": {"state": "IDLE"}}

        filtered = node_filter(node_dict, None)

        self.assertEqual(len(filtered), 1)

    def test_node_filter_modifies_in_place(self):
        """Test that node_filter modifies the original dict."""
        node_dict = {"worker-1": {"state": "IDLE"}, "worker-2": {"state": "IDLE"}}

        node_filter(node_dict, ["worker-1"])

        # The original dict should be modified
        self.assertEqual(len(node_dict), 1)
        self.assertNotIn("worker-1", node_dict)


class TestSchedulerJobDataHelper(unittest.TestCase):
    """Tests for scheduler job data helper functions."""

    def test_sort_job_priority_sorted(self):
        """Test sort_job_priority returns sorted list."""
        jobs_dict = {
            1: {
                "jobid": 1,
                "priority": 50,
                "req_mem": 4096,
                "req_cpus": 2,
            },
            2: {
                "jobid": 2,
                "priority": 100,
                "req_mem": 2048,
                "req_cpus": 1,
            },
            3: {
                "jobid": 3,
                "priority": 75,
                "req_mem": 8192,
                "req_cpus": 4,
            },
        }

        sorted_jobs = sort_job_priority(jobs_dict)

        # Should be sorted by priority descending: 100, 75, 50
        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 3, 1])

    def test_sort_job_by_resources_sorted(self):
        """Test sort_job_by_resources sorts by memory descending."""
        jobs_dict = {
            1: {
                "jobid": 1,
                "req_mem": 4096,
                "req_cpus": 2,
                "temporary_disk": 0,
            },
            2: {
                "jobid": 2,
                "req_mem": 8192,
                "req_cpus": 1,
                "temporary_disk": 0,
            },
            3: {
                "jobid": 3,
                "req_mem": 2048,
                "req_cpus": 4,
                "temporary_disk": 0,
            },
        }

        sorted_jobs = sort_job_by_resources(jobs_dict)

        # Should be sorted by memory descending: 8192, 4096, 2048
        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 1, 3])

    def test_sort_job_priority_with_job_state(self):
        """Test sort_job_priority works with SchedulerJobState objects."""
        jobs_dict = {
            1: SchedulerJobState(
                jobid=1,
                state=0,
                state_str="PENDING",
                req_cpus=2,
                req_mem=4096,
                temporary_disk=0,
                priority=50,
                jobname="job-1",
                nodes="",
                elapsed=0,
            ),
            2: SchedulerJobState(
                jobid=2,
                state=0,
                state_str="PENDING",
                req_cpus=1,
                req_mem=2048,
                temporary_disk=0,
                priority=100,
                jobname="job-2",
                nodes="",
                elapsed=0,
            ),
        }

        sorted_jobs = sort_job_priority(jobs_dict)

        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 1])

    def test_sort_job_by_resources_with_job_state(self):
        """Test sort_job_by_resources works with SchedulerJobState objects."""
        jobs_dict = {
            1: SchedulerJobState(
                jobid=1,
                state=0,
                state_str="PENDING",
                req_cpus=2,
                req_mem=4096,
                temporary_disk=0,
                priority=100,
                jobname="job-1",
                nodes="",
                elapsed=0,
            ),
            2: SchedulerJobState(
                jobid=2,
                state=0,
                state_str="PENDING",
                req_cpus=1,
                req_mem=8192,
                temporary_disk=0,
                priority=100,
                jobname="job-2",
                nodes="",
                elapsed=0,
            ),
        }

        sorted_jobs = sort_job_by_resources(jobs_dict)

        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 1])


class TestSchedulerInterfaceMock(unittest.TestCase):
    """Tests using a mock scheduler implementation."""

    def test_scheduler_interface_is_abstract(self):
        """Test that SchedulerInterface is abstract."""
        self.assertTrue(issubclass(SchedulerInterface, object))

    def test_scheduler_node_state_dataclass(self):
        """Test SchedulerNodeState is a dataclass."""
        state = SchedulerNodeState(
            hostname="worker-1",
            state="IDLE",
            total_cpus=4,
            free_memory=8192,
            real_memory=16384,
            temporary_disk=20,
            gres=["gpu:0"],
            node_hostname="worker-1",
        )
        self.assertEqual(state.hostname, "worker-1")
        self.assertEqual(state.total_cpus, 4)

    def test_scheduler_job_state_dataclass(self):
        """Test SchedulerJobState is a dataclass."""
        state = SchedulerJobState(
            jobid=1,
            state=0,
            state_str="PENDING",
            req_cpus=2,
            req_mem=4096,
            temporary_disk=0,
            priority=100,
            jobname="test-job",
            nodes="",
            elapsed=0,
        )
        self.assertEqual(state.jobid, 1)
        self.assertEqual(state.state, 0)
        self.assertEqual(state.req_mem, 4096)


if __name__ == "__main__":
    unittest.main()
