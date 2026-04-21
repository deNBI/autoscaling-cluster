"""
Unit tests for scheduler modules.
"""
import unittest


class TestSchedulerNodeData(unittest.TestCase):
    """Tests for scheduler node data functions."""

    def test_node_filter_removes_ignored_workers(self):
        """Test that node_filter removes ignored workers."""
        from autoscaling.scheduler.node_data import node_filter

        node_dict = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
            "worker-3": {"state": "ALLOC"},
            "headnode": {"state": "IDLE"},
        }

        # Filter out worker-1 and worker-2
        filtered = node_filter(node_dict, ["worker-1", "worker-2"])

        self.assertNotIn("worker-1", filtered)
        self.assertNotIn("worker-2", filtered)
        self.assertIn("worker-3", filtered)
        self.assertIn("headnode", filtered)

    def test_node_filter_empty_list(self):
        """Test node_filter with empty ignore list."""
        from autoscaling.scheduler.node_data import node_filter

        node_dict = {
            "worker-1": {"state": "IDLE"},
            "worker-2": {"state": "IDLE"},
        }

        filtered = node_filter(node_dict, [])

        self.assertEqual(len(filtered), 2)

    def test_node_filter_none_ignore(self):
        """Test node_filter with None ignore list."""
        from autoscaling.scheduler.node_data import node_filter

        node_dict = {
            "worker-1": {"state": "IDLE"},
        }

        filtered = node_filter(node_dict, None)

        self.assertEqual(len(filtered), 1)

    def test_receive_node_stats_filters_dummy(self):
        """Test _receive_node_stats filters out dummy node."""
        from autoscaling.scheduler.node_data import _receive_node_stats

        node_dict = {
            "bibigrid-worker-autoscaling-dummy": {"state": "IDLE"},
            "worker-1": {"state": "IDLE", "total_cpus": 4, "real_memory": 16384},
            "worker-2": {"state": "ALLOC", "total_cpus": 4, "real_memory": 16384},
        }

        result = _receive_node_stats(node_dict, quiet=True)

        worker_json, worker_count, worker_in_use, worker_drain, worker_drain_idle = result

        self.assertNotIn("bibigrid-worker-autoscaling-dummy", worker_json)
        self.assertEqual(worker_count, 2)
        self.assertEqual(len(worker_in_use), 1)
        self.assertIn("worker-2", worker_in_use)

    def test_receive_node_stats_categorizes_workers(self):
        """Test _receive_node_stats correctly categorizes workers."""
        from autoscaling.scheduler.node_data import _receive_node_stats

        node_dict = {
            "bibigrid-worker-autoscaling-dummy": {"state": "IDLE", "total_cpus": 4, "real_memory": 16384},
            "worker-1": {"state": "IDLE", "total_cpus": 4, "real_memory": 16384},
            "worker-2": {"state": "ALLOC", "total_cpus": 4, "real_memory": 16384},
            "worker-3": {"state": "ALLOC+DRAIN", "total_cpus": 4, "real_memory": 16384},
            "worker-4": {"state": "IDLE+DRAIN", "total_cpus": 4, "real_memory": 16384},
        }

        result = _receive_node_stats(node_dict, quiet=True)

        worker_json, worker_count, worker_in_use, worker_drain, worker_drain_idle = result

        # Should only count worker nodes
        self.assertEqual(worker_count, 4)

        # worker-2 is ALLOC
        self.assertIn("worker-2", worker_in_use)

        # worker-3 and worker-4 are DRAIN
        self.assertIn("worker-3", worker_drain)
        self.assertIn("worker-4", worker_drain)

        # worker-4 is IDLE+DRAIN
        self.assertIn("worker-4", worker_drain_idle)

        # worker-1 is IDLE (free)
        self.assertNotIn("worker-1", worker_in_use)
        self.assertNotIn("worker-1", worker_drain)

    def test_receive_node_stats_converts_state_dict(self):
        """Test _receive_node_stats converts SchedulerNodeState to dict."""
        from autoscaling.scheduler.node_data import _receive_node_stats
        from autoscaling.scheduler.interface import SchedulerNodeState

        node_dict = {
            "bibigrid-worker-autoscaling-dummy": {"state": "IDLE", "total_cpus": 4, "real_memory": 16384},
            "worker-1": SchedulerNodeState(
                hostname="worker-1",
                state="IDLE",
                total_cpus=4,
                free_memory=8192,
                real_memory=16384,
                temporary_disk=20,
                gres=["gpu:0"],
                node_hostname="worker-1",
            ),
        }

        result = _receive_node_stats(node_dict, quiet=True)

        worker_json, worker_count, _, _, _ = result

        self.assertEqual(worker_count, 1)
        self.assertIn("worker-1", worker_json)
        worker_data = worker_json["worker-1"]
        self.assertEqual(worker_data["state"], "IDLE")
        self.assertEqual(worker_data["total_cpus"], 4)
        self.assertEqual(worker_data["real_memory"], 16384)


class TestSchedulerJobData(unittest.TestCase):
    """Tests for scheduler job data functions."""

    def test_receive_job_data_separates_pending_running(self):
        """Test receive_job_data correctly separates pending and running jobs."""
        from autoscaling.scheduler.job_data import receive_job_data
        from autoscaling.scheduler.interface import SchedulerJobState

        class MockScheduler:
            def get_job_data_live(self):
                return {
                    1: SchedulerJobState(
                        jobid=1,
                        state=0,  # PENDING
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
                        state=1,  # RUNNING
                        state_str="RUNNING",
                        req_cpus=4,
                        req_mem=8192,
                        temporary_disk=0,
                        priority=200,
                        jobname="job-2",
                        nodes="",
                        elapsed=3600,
                    ),
                    3: SchedulerJobState(
                        jobid=3,
                        state=0,  # PENDING
                        state_str="PENDING",
                        req_cpus=1,
                        req_mem=2048,
                        temporary_disk=0,
                        priority=50,
                        jobname="job-3",
                        nodes="",
                        elapsed=0,
                    ),
                }

        scheduler = MockScheduler()
        pending, running = receive_job_data(scheduler)

        self.assertEqual(len(pending), 2)
        self.assertEqual(len(running), 1)
        self.assertIn(1, pending)
        self.assertIn(3, pending)
        self.assertIn(2, running)

    def test_sort_job_priority_sorts_by_priority(self):
        """Test sort_job_priority sorts by priority."""
        from autoscaling.scheduler.job_data import sort_job_priority

        jobs_dict = {
            1: {
                "jobid": 1,
                "state": 0,
                "priority": 100,
                "req_mem": 4096,
                "req_cpus": 2,
            },
            2: {
                "jobid": 2,
                "state": 0,
                "priority": 200,
                "req_mem": 2048,
                "req_cpus": 1,
            },
            3: {
                "jobid": 3,
                "state": 0,
                "priority": 50,
                "req_mem": 8192,
                "req_cpus": 4,
            },
        }

        sorted_jobs = sort_job_priority(jobs_dict)

        # Should be sorted by priority descending
        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 1, 3])

    def test_sort_job_by_resources_sorts_by_memory(self):
        """Test sort_job_by_resources sorts by memory."""
        from autoscaling.scheduler.job_data import sort_job_by_resources

        jobs_dict = {
            1: {
                "jobid": 1,
                "state": 0,
                "priority": 100,
                "req_mem": 4096,
                "req_cpus": 2,
            },
            2: {
                "jobid": 2,
                "state": 0,
                "priority": 100,
                "req_mem": 8192,
                "req_cpus": 1,
            },
            3: {
                "jobid": 3,
                "state": 0,
                "priority": 100,
                "req_mem": 2048,
                "req_cpus": 4,
            },
        }

        sorted_jobs = sort_job_by_resources(jobs_dict)

        # Should be sorted by memory descending
        job_ids = [j[0] for j in sorted_jobs]
        self.assertEqual(job_ids, [2, 1, 3])


if __name__ == "__main__":
    unittest.main()
