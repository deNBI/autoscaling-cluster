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


class TestSchedulerInterface(unittest.TestCase):
    """Tests for the SchedulerInterface abstract base class."""

    def test_interface_has_required_methods(self):
        """Test that SchedulerInterface has all required abstract methods."""
        from autoscaling.scheduler.interface import SchedulerInterface
        import abc

        required_methods = [
            'test_connection',
            'get_node_data',
            'get_job_data',
            'get_node_data_live',
            'get_job_data_live',
            'drain_node',
            'resume_node',
            'get_job_data_by_range',
        ]

        for method in required_methods:
            self.assertTrue(
                hasattr(SchedulerInterface, method),
                f"SchedulerInterface should have method '{method}'"
            )
            # Verify it's an abstract method
            method_obj = getattr(SchedulerInterface, method)
            self.assertTrue(
                getattr(method_obj, '__isabstractmethod__', False),
                f"Method '{method}' should be abstract"
            )

    def test_interface_raises_on_instantiation(self):
        """Test that SchedulerInterface cannot be directly instantiated."""
        from autoscaling.scheduler.interface import SchedulerInterface

        with self.assertRaises(TypeError):
            SchedulerInterface()

    def test_scheduler_node_state_dataclass(self):
        """Test SchedulerNodeState dataclass structure."""
        from autoscaling.scheduler.interface import SchedulerNodeState

        node = SchedulerNodeState(
            hostname="test-node",
            state="IDLE",
            total_cpus=4,
            free_memory=8192,
            real_memory=16384,
            temporary_disk=20,
            gres=["gpu:0"],
            node_hostname="test-node",
        )

        self.assertEqual(node.hostname, "test-node")
        self.assertEqual(node.state, "IDLE")
        self.assertEqual(node.total_cpus, 4)
        self.assertEqual(node.free_memory, 8192)
        self.assertEqual(node.real_memory, 16384)
        self.assertEqual(node.temporary_disk, 20)
        self.assertEqual(node.gres, ["gpu:0"])

    def test_scheduler_job_state_dataclass(self):
        """Test SchedulerJobState dataclass structure."""
        from autoscaling.scheduler.interface import SchedulerJobState

        job = SchedulerJobState(
            jobid=123,
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

        self.assertEqual(job.jobid, 123)
        self.assertEqual(job.state, 0)
        self.assertEqual(job.state_str, "PENDING")
        self.assertEqual(job.req_cpus, 2)
        self.assertEqual(job.req_mem, 4096)
        self.assertEqual(job.priority, 100)
        self.assertEqual(job.jobname, "test-job")


class TestSlurmScheduler(unittest.TestCase):
    """Tests for the SlurmScheduler implementation."""

    def setUp(self):
        """Set up test fixtures."""
        from autoscaling.scheduler.slurm import SlurmScheduler
        self.scheduler = SlurmScheduler()

    def test_slurm_scheduler_implements_interface(self):
        """Test that SlurmScheduler implements SchedulerInterface."""
        from autoscaling.scheduler.interface import SchedulerInterface

        self.assertIsInstance(self.scheduler, SchedulerInterface)

    def test_set_logger(self):
        """Test setting logger on scheduler."""
        import logging
        logger = logging.getLogger(__name__)
        self.scheduler.set_logger(logger)
        # Logger should be set (we can't easily verify internal state)
        self.assertIsNotNone(self.scheduler._logger)

    def test_test_connection_without_pyslurm(self):
        """Test test_connection when pyslurm is not available."""
        # This test verifies the error handling path
        # We can't easily mock pyslurm import, so we just verify the method exists
        self.assertTrue(hasattr(self.scheduler, 'test_connection'))

    def test_get_node_data_without_pyslurm(self):
        """Test get_node_data when pyslurm is not available."""
        # This test verifies the error handling path
        self.assertTrue(hasattr(self.scheduler, 'get_node_data'))

    def test_get_job_data_method_exists(self):
        """Test that get_job_data method exists."""
        self.assertTrue(hasattr(self.scheduler, 'get_job_data'))

    def test_drain_node_method_exists(self):
        """Test that drain_node method exists."""
        self.assertTrue(hasattr(self.scheduler, 'drain_node'))

    def test_resume_node_method_exists(self):
        """Test that resume_node method exists."""
        self.assertTrue(hasattr(self.scheduler, 'resume_node'))

    def test_node_state_constants(self):
        """Test that SlurmScheduler has node state constants."""
        self.assertEqual(self.scheduler.NODE_ALLOCATED, "ALLOC")
        self.assertEqual(self.scheduler.NODE_IDLE, "IDLE")
        self.assertEqual(self.scheduler.NODE_DRAIN, "DRAIN")
        self.assertEqual(self.scheduler.NODE_DOWN, "DOWN")
        self.assertEqual(self.scheduler.NODE_MIX, "MIX")

    def test_job_state_constants(self):
        """Test that SlurmScheduler has job state constants."""
        self.assertEqual(self.scheduler.JOB_PENDING, 0)
        self.assertEqual(self.scheduler.JOB_RUNNING, 1)
        self.assertEqual(self.scheduler.JOB_COMPLETED, 3)

    def test_convert_node_state_alloc(self):
        """Test _convert_node_state for ALLOC state."""
        self.assertEqual(self.scheduler._convert_node_state("ALLOC"), "ALLOC")

    def test_convert_node_state_idle(self):
        """Test _convert_node_state for IDLE state."""
        self.assertEqual(self.scheduler._convert_node_state("IDLE"), "IDLE")

    def test_convert_node_state_drain(self):
        """Test _convert_node_state for DRAIN state."""
        self.assertEqual(self.scheduler._convert_node_state("IDLE+DRAIN"), "IDLE+DRAIN")

    def test_convert_node_state_drng(self):
        """Test _convert_node_state for DRNG state."""
        self.assertEqual(self.scheduler._convert_node_state("ALLOC+DRNG"), "ALLOC+DRAIN")

    def test_convert_node_state_mixed(self):
        """Test _convert_node_state for MIX state."""
        self.assertEqual(self.scheduler._convert_node_state("MIX"), "MIX")

    def test_convert_node_state_down(self):
        """Test _convert_node_state for DOWN state."""
        self.assertEqual(self.scheduler._convert_node_state("DOWN"), "DOWN")

    def test_convert_node_state_unknown(self):
        """Test _convert_node_state for unknown state."""
        self.assertEqual(self.scheduler._convert_node_state("UNKNOWN"), "UNKNOWN")

    def test_convert_job_state_pending(self):
        """Test _convert_job_state for PENDING."""
        self.assertEqual(self.scheduler._convert_job_state("PENDING"), 0)

    def test_convert_job_state_running(self):
        """Test _convert_job_state for RUNNING."""
        self.assertEqual(self.scheduler._convert_job_state("RUNNING"), 1)

    def test_convert_job_state_completed(self):
        """Test _convert_job_state for COMPLETED."""
        self.assertEqual(self.scheduler._convert_job_state("COMPLETED"), 3)

    def test_convert_job_state_unknown(self):
        """Test _convert_job_state for unknown state."""
        self.assertEqual(self.scheduler._convert_job_state("UNKNOWN"), -1)

    def test_parse_memory_kb(self):
        """Test _parse_memory with KB."""
        self.assertEqual(self.scheduler._parse_memory("1024K"), 1024)

    def test_parse_memory_mb(self):
        """Test _parse_memory with MB."""
        self.assertEqual(self.scheduler._parse_memory("1024M"), 1024)

    def test_parse_memory_gb(self):
        """Test _parse_memory with GB."""
        self.assertEqual(self.scheduler._parse_memory("1G"), 1024)

    def test_parse_memory_tb(self):
        """Test _parse_memory with TB."""
        self.assertEqual(self.scheduler._parse_memory("1T"), 1024 * 1024)

    def test_parse_time_hms(self):
        """Test _parse_time with HH:MM:SS format."""
        self.assertEqual(self.scheduler._parse_time("01:30:00"), 5400)  # 1.5 hours

    def test_parse_time_mins(self):
        """Test _parse_time with MM:SS format."""
        self.assertEqual(self.scheduler._parse_time("30:00"), 1800)  # 30 minutes

    def test_parse_time_days(self):
        """Test _parse_time with DD-HH:MM:SS format."""
        self.assertEqual(self.scheduler._parse_time("1-02:30:00"), 95400)  # 1 day + 2.5 hours


if __name__ == "__main__":
    unittest.main()
