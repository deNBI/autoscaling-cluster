"""
Unit tests for the core state module.
"""
import unittest

from autoscaling.core.state import (
    ScalingContext,
    ScalingAction,
    ScaleState,
    Rescale,
    WorkerResource,
)


class TestScaleState(unittest.TestCase):
    """Tests for ScaleState enum."""

    def test_scale_state_values(self):
        """Test ScaleState enum values."""
        self.assertEqual(ScaleState.UP.value, 1)
        self.assertEqual(ScaleState.DOWN.value, 0)
        self.assertEqual(ScaleState.SKIP.value, 2)
        self.assertEqual(ScaleState.DONE.value, 3)
        self.assertEqual(ScaleState.DOWN_UP.value, 4)
        self.assertEqual(ScaleState.FORCE_UP.value, 5)
        self.assertEqual(ScaleState.DELAY.value, -1)


class TestRescale(unittest.TestCase):
    """Tests for Rescale enum."""

    def test_rescale_values(self):
        """Test Rescale enum values."""
        self.assertEqual(Rescale.INIT.value, 0)
        self.assertEqual(Rescale.CHECK.value, 1)
        self.assertEqual(Rescale.NONE.value, 2)


class TestScalingContext(unittest.TestCase):
    """Tests for ScalingContext dataclass."""

    def setUp(self):
        """Set up test fixtures."""
        self.context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=[],
            worker_drain=[],
            worker_free=["worker-1", "worker-2"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )

    def test_context_creation(self):
        """Test creating a ScalingContext."""
        self.assertEqual(self.context.mode, "basic")
        self.assertEqual(self.context.scale_force, 0.6)
        self.assertEqual(self.context.scale_delay, 60)
        self.assertEqual(self.context.worker_cool_down, 60)
        self.assertEqual(self.context.limit_memory, 0)
        self.assertEqual(self.context.limit_worker_starts, 0)
        self.assertEqual(self.context.limit_workers, 0)
        self.assertEqual(self.context.worker_count, 2)
        self.assertEqual(self.context.worker_in_use, [])
        self.assertEqual(self.context.worker_drain, [])
        self.assertEqual(self.context.worker_free, ["worker-1", "worker-2"])
        self.assertEqual(self.context.jobs_pending, [])
        self.assertEqual(self.context.jobs_running, [])
        self.assertEqual(self.context.jobs_pending_count, 0)
        self.assertEqual(self.context.jobs_running_count, 0)
        self.assertEqual(self.context.flavor_data, [])

    def test_context_with_defaults(self):
        """Test ScalingContext with default values for optional fields."""
        context = ScalingContext(
            mode="adaptive",
            scale_force=0.8,
            scale_delay=120,
            worker_cool_down=30,
            limit_memory=16384,
            limit_worker_starts=5,
            limit_workers=20,
            worker_count=5,
            worker_in_use=["worker-1"],
            worker_drain=["worker-2"],
            worker_free=["worker-3", "worker-4"],
            jobs_pending=[{"jobid": 1}],
            jobs_running=[{"jobid": 2}],
            jobs_pending_count=1,
            jobs_running_count=1,
            flavor_data=[],
        )
        self.assertEqual(context.mode, "adaptive")
        self.assertEqual(context.scale_force, 0.8)
        self.assertEqual(context.scale_delay, 120)
        self.assertEqual(context.worker_cool_down, 30)
        self.assertEqual(context.limit_memory, 16384)
        self.assertEqual(context.limit_worker_starts, 5)
        self.assertEqual(context.limit_workers, 20)
        self.assertEqual(context.worker_count, 5)
        self.assertIn("worker-1", context.worker_in_use)
        self.assertIn("worker-2", context.worker_drain)
        self.assertIn("worker-3", context.worker_free)
        self.assertEqual(len(context.jobs_pending), 1)
        self.assertEqual(len(context.jobs_running), 1)

    def test_need_workers_true(self):
        """Test need_workers when workers are needed."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=0,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[{"jobid": 1}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertTrue(context.need_workers)

    def test_need_workers_false_when_no_pending_jobs(self):
        """Test need_workers when no pending jobs."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=0,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertFalse(context.need_workers)

    def test_need_workers_false_when_in_use_workers(self):
        """Test need_workers when there are in-use workers."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=["worker-1"],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[{"jobid": 1}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertFalse(context.need_workers)

    def test_need_workers_false_when_free_workers(self):
        """Test need_workers when there are free workers."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=[],
            worker_drain=[],
            worker_free=["worker-1"],
            jobs_pending=[{"jobid": 1}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertFalse(context.need_workers)

    def test_can_scale_up_true(self):
        """Test can_scale_up when scaling up is possible."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=[],
            worker_drain=[],
            worker_free=["worker-1"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertTrue(context.can_scale_up)

    def test_can_scale_up_false_when_limit_reached(self):
        """Test can_scale_up when limit_workers is reached."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=5,
            worker_count=5,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertFalse(context.can_scale_up)

    def test_can_scale_down_true(self):
        """Test can_scale_down when scaling down is possible."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=5,
            worker_in_use=[],
            worker_drain=[],
            worker_free=["worker-1", "worker-2"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertTrue(context.can_scale_down)

    def test_can_scale_down_false_when_no_workers(self):
        """Test can_scale_down when no workers exist."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=0,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        self.assertFalse(context.can_scale_down)

    def test_context_with_all_optional_fields(self):
        """Test ScalingContext with all optional fields set."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=[],
            worker_drain=[],
            worker_free=["worker-1"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
            flavor_default="small",
            forecast_by_flavor_history=True,
            forecast_by_job_history=True,
            forecast_active_worker=3,
            job_time_threshold=0.75,
            smoothing_coefficient=0.5,
            flavor_depth=2,
            large_flavors=True,
            large_flavors_except_hmf=False,
        )
        self.assertEqual(context.flavor_default, "small")
        self.assertTrue(context.forecast_by_flavor_history)
        self.assertTrue(context.forecast_by_job_history)
        self.assertEqual(context.forecast_active_worker, 3)
        self.assertEqual(context.job_time_threshold, 0.75)
        self.assertEqual(context.smoothing_coefficient, 0.5)
        self.assertEqual(context.flavor_depth, 2)
        self.assertTrue(context.large_flavors)
        self.assertFalse(context.large_flavors_except_hmf)


class TestScalingAction(unittest.TestCase):
    """Tests for ScalingAction dataclass."""

    def test_action_creation_upscale(self):
        """Test creating an upscale action."""
        action = ScalingAction.upscale_action(
            flavor="large", count=3, reason="High pending jobs"
        )
        self.assertTrue(action.upscale)
        self.assertFalse(action.downscale)
        self.assertFalse(action.is_noop)
        self.assertEqual(action.upscale_flavor, "large")
        self.assertEqual(action.upscale_count, 3)
        self.assertEqual(action.reason, "High pending jobs")

    def test_action_creation_downscale(self):
        """Test creating a downscale action."""
        action = ScalingAction.downscale_action(
            workers=["worker-1", "worker-2", "worker-3"],
            reason="Low utilization"
        )
        self.assertFalse(action.upscale)
        self.assertTrue(action.downscale)
        self.assertFalse(action.is_noop)
        self.assertEqual(action.downscale_workers, ["worker-1", "worker-2", "worker-3"])
        self.assertEqual(action.reason, "Low utilization")

    def test_action_creation_noop(self):
        """Test creating a noop action."""
        action = ScalingAction.noop_action("No scaling needed")
        self.assertFalse(action.upscale)
        self.assertFalse(action.downscale)
        self.assertTrue(action.is_noop)
        self.assertEqual(action.reason, "No scaling needed")

    def test_action_default_values(self):
        """Test ScalingAction default values."""
        action = ScalingAction()
        self.assertFalse(action.upscale)
        self.assertFalse(action.downscale)
        self.assertTrue(action.is_noop)
        self.assertEqual(action.upscale_flavor, None)
        self.assertEqual(action.upscale_count, 0)
        self.assertEqual(action.downscale_workers, [])
        self.assertEqual(action.reason, "")

    def test_action_downscale_workers_default_empty_list(self):
        """Test that downscale_workers defaults to empty list."""
        action = ScalingAction(downscale=True)
        self.assertEqual(action.downscale_workers, [])

    def test_action_is_noop_property(self):
        """Test is_noop property for various actions."""
        # Noop action
        noop = ScalingAction.noop_action("test")
        self.assertTrue(noop.is_noop)

        # Upscale action
        upscale = ScalingAction.upscale_action("small", 1, "test")
        self.assertFalse(upscale.is_noop)

        # Downscale action
        downscale = ScalingAction.downscale_action(["w1"], "test")
        self.assertFalse(downscale.is_noop)

        # Manual creation with no flags
        manual = ScalingAction()
        self.assertTrue(manual.is_noop)


class TestWorkerResource(unittest.TestCase):
    """Tests for WorkerResource dataclass."""

    def test_worker_resource_creation(self):
        """Test creating a WorkerResource."""
        resource = WorkerResource(cpu=4, memory=8192, disk=100)
        self.assertEqual(resource.cpu, 4)
        self.assertEqual(resource.memory, 8192)
        self.assertEqual(resource.disk, 100)

    def test_worker_resource_default_values(self):
        """Test WorkerResource with zero values."""
        resource = WorkerResource(cpu=0, memory=0, disk=0)
        self.assertEqual(resource.cpu, 0)
        self.assertEqual(resource.memory, 0)
        self.assertEqual(resource.disk, 0)


if __name__ == "__main__":
    unittest.main()
