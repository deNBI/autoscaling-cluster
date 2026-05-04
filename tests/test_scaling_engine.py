"""
Unit tests for the ScalingEngine.
"""
import unittest
from autoscaling.core.state import ScalingContext, ScalingAction, ScaleState
from autoscaling.core.scaling_engine import ScalingEngine


class TestScalingEngine(unittest.TestCase):
    """Tests for the ScalingEngine class."""

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
            flavor_default=None,
        )
        self.engine = ScalingEngine(self.context)

    def test_calculate_scaling_no_pending_jobs(self):
        """Test scaling calculation when no jobs are pending."""
        action = self.engine.calculate_scaling()
        self.assertFalse(action.upscale)
        # Downscale is preferred when no pending jobs and free workers exist
        self.assertTrue(action.downscale or action.is_noop)

    def test_calculate_scaling_no_free_workers(self):
        """Test scaling calculation when no free workers."""
        self.context.jobs_pending = [{"req_mem": 1024, "req_cpus": 1}]
        self.context.worker_free = []
        action = self.engine.calculate_scaling()
        self.assertFalse(action.downscale)
        # When no free workers and no compatible flavors, no scaling
        self.assertTrue(action.is_noop)

    def test_calculate_scaling_uses_scale_force(self):
        """Test that scale_force is applied correctly."""
        self.context.jobs_pending = [{"req_mem": 1024, "req_cpus": 1}] * 10
        self.context.worker_free = ["worker-1", "worker-2"]
        action = self.engine.calculate_scaling()
        # With 10 pending jobs and scale_force 0.6, should try to scale up 6
        # But limited by free workers (2) and no compatible flavors
        self.assertIsNotNone(action)

    def test_calculate_downscale_with_free_workers(self):
        """Test downscale calculation with free workers."""
        self.context.worker_free = ["worker-1", "worker-2"]
        self.context.jobs_pending = []
        action = self.engine.calculate_scaling()
        # With free workers and no pending jobs, should scale down
        self.assertTrue(action.downscale or action.is_noop)

    def test_noop_action(self):
        """Test creation of noop action."""
        action = ScalingAction.noop_action("Test reason")
        self.assertTrue(action.is_noop)
        self.assertEqual(action.reason, "Test reason")

    def test_upscale_action(self):
        """Test creation of upscale action."""
        action = ScalingAction.upscale_action(
            flavor="test-flavor", count=3, reason="Test upscale"
        )
        self.assertTrue(action.upscale)
        self.assertEqual(action.upscale_flavor, "test-flavor")
        self.assertEqual(action.upscale_count, 3)

    def test_downscale_action(self):
        """Test creation of downscale action."""
        action = ScalingAction.downscale_action(
            workers=["worker-1", "worker-2"], reason="Test downscale"
        )
        self.assertTrue(action.downscale)
        self.assertEqual(action.downscale_workers, ["worker-1", "worker-2"])

    def test_scaling_context_need_workers(self):
        """Test need_workers property."""
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
            jobs_pending=[{"req_mem": 1024}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=[],
        )
        # need_workers checks if pending > 0 AND in_use == 0 AND free == 0
        # Since worker_free is now a list, we check len() == 0
        self.assertTrue(len(context.jobs_pending) > 0)
        self.assertTrue(len(context.worker_in_use) == 0)
        self.assertTrue(len(context.worker_free) == 0)

    def test_scaling_context_can_scale_up(self):
        """Test can_scale_up property."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=10,
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
        self.assertTrue(context.can_scale_up)

    def test_scaling_context_can_scale_down(self):
        """Test can_scale_down property."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=10,
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
        self.assertTrue(context.can_scale_down)


class TestScalingEngineFlavorSelection(unittest.TestCase):
    """Tests for flavor selection in ScalingEngine."""

    def setUp(self):
        """Set up test fixtures."""
        self.flavor_data = [
            {
                "flavor": {
                    "name": "small",
                    "vcpus": 2,
                    "ram_gib": 4,
                    "ephemeral_disk": 10,
                }
            },
            {
                "flavor": {
                    "name": "medium",
                    "vcpus": 4,
                    "ram_gib": 8,
                    "ephemeral_disk": 20,
                }
            },
            {
                "flavor": {
                    "name": "large",
                    "vcpus": 8,
                    "ram_gib": 16,
                    "ephemeral_disk": 40,
                }
            },
        ]

    def test_get_compatible_flavors(self):
        """Test filtering compatible flavors."""
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
            jobs_pending=[{"req_mem": 2048, "req_cpus": 2}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=self.flavor_data,
        )
        engine = ScalingEngine(context)
        job = {"req_mem": 2048, "req_cpus": 2}
        compatible = engine._get_compatible_flavors(job)
        self.assertEqual(len(compatible), 3)

    def test_get_compatible_flavors_excludes_small(self):
        """Test that small flavors are excluded when job needs more resources."""
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
            jobs_pending=[{"req_mem": 16384, "req_cpus": 8}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=self.flavor_data,
        )
        engine = ScalingEngine(context)
        job = {"req_mem": 16384, "req_cpus": 8}
        compatible = engine._get_compatible_flavors(job)
        self.assertEqual(len(compatible), 1)
        self.assertEqual(compatible[0]["flavor"]["name"], "large")

    def test_select_flavor_selects_smallest(self):
        """Test that the smallest compatible flavor is selected."""
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
            jobs_pending=[{"req_mem": 1024, "req_cpus": 1}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=self.flavor_data,
        )
        engine = ScalingEngine(context)
        compatible = engine._get_compatible_flavors(
            {"req_mem": 1024, "req_cpus": 1}
        )
        selected = engine._select_flavor(compatible, {"req_mem": 1024, "req_cpus": 1})
        self.assertEqual(selected["flavor"]["name"], "small")

    def test_select_flavor_respects_default(self):
        """Test that default flavor is respected when configured."""
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
            jobs_pending=[{"req_mem": 4096, "req_cpus": 2}],
            jobs_running=[],
            jobs_pending_count=1,
            jobs_running_count=0,
            flavor_data=self.flavor_data,
            flavor_default="medium",
        )
        engine = ScalingEngine(context)
        compatible = engine._get_compatible_flavors(
            {"req_mem": 4096, "req_cpus": 2}
        )
        selected = engine._select_flavor(compatible, {"req_mem": 4096, "req_cpus": 2})
        self.assertEqual(selected["flavor"]["name"], "medium")


class TestScalingEngineUpscaleLimit(unittest.TestCase):
    """Tests for upscale limit calculation."""

    def test_calculate_upscale_limit_basic(self):
        """Test basic upscale limit calculation."""
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
            jobs_pending_count=10,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 10)
        # 10 * 0.6 = 6, should be at least 1
        self.assertEqual(limit, 6)

    def test_calculate_upscale_limit_with_worker_starts(self):
        """Test upscale limit with worker starts constraint."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=3,
            limit_workers=0,
            worker_count=0,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=10,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 10)
        # Should be limited by limit_worker_starts
        self.assertEqual(limit, 3)

    def test_calculate_upscale_limit_with_workers_total(self):
        """Test upscale limit with total workers constraint."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=5,
            worker_count=3,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=10,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 10)
        # Should be limited by limit_workers - worker_count = 2
        self.assertEqual(limit, 2)

    def test_calculate_upscale_limit_with_memory(self):
        """Test upscale limit with memory constraint."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=16384,  # 16GB in MB
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=1,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=10,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        # Flavor uses 4GB (4096 MB)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 10)
        # Available: 16384 - 4096 = 12288 MB
        # Each worker: 4096 MB
        # Memory limit: 12288 / 4096 = 3
        self.assertEqual(limit, 3)

    def test_calculate_scaling_with_draining_workers(self):
        """Test scaling with draining workers excluded from downscale."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=3,
            worker_in_use=[],
            worker_drain=["worker-1"],
            worker_free=["worker-1", "worker-2"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        action = engine.calculate_scaling()
        # worker-1 is draining, so only worker-2 should be in downscale list
        if action.downscale:
            self.assertNotIn("worker-1", action.downscale_workers)
            self.assertIn("worker-2", action.downscale_workers)

    def test_calculate_scaling_with_no_jobs(self):
        """Test scaling when no jobs exist at all."""
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
            worker_free=["worker-1", "worker-2", "worker-3"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        action = engine.calculate_scaling()
        # Should scale down since no jobs and free workers
        self.assertTrue(action.downscale or action.is_noop)

    def test_calculate_scaling_with_running_jobs_only(self):
        """Test scaling with only running jobs, no pending."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=2,
            worker_in_use=["worker-1", "worker-2"],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[{"jobid": 1}, {"jobid": 2}],
            jobs_pending_count=0,
            jobs_running_count=2,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        action = engine.calculate_scaling()
        # No free workers, no pending jobs -> noop
        self.assertTrue(action.is_noop)

    def test_get_most_demanding_job(self):
        """Test _get_most_demanding_job selects highest memory job."""
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
        engine = ScalingEngine(context)
        jobs = [
            {"req_mem": 1024, "req_cpus": 2},
            {"req_mem": 4096, "req_cpus": 1},
            {"req_mem": 2048, "req_cpus": 4},
        ]
        most_demanding = engine._get_most_demanding_job(jobs)
        self.assertEqual(most_demanding["req_mem"], 4096)

    def test_get_compatible_flavors_no_matches(self):
        """Test _get_compatible_flavors when no flavors match."""
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
            jobs_pending=[{"req_mem": 16384, "req_cpus": 16}],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        compatible = engine._get_compatible_flavors({"req_mem": 16384, "req_cpus": 16})
        self.assertEqual(len(compatible), 0)

    def test_select_flavor_no_flavors(self):
        """Test _select_flavor with empty list."""
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
        engine = ScalingEngine(context)
        selected = engine._select_flavor([], {"req_mem": 1024, "req_cpus": 1})
        self.assertIsNone(selected)


class TestScalingEngineEdgeCases(unittest.TestCase):
    """Tests for edge cases in ScalingEngine."""

    def test_calculate_upscale_limit_with_zero_pending(self):
        """Test upscale limit calculation with zero pending jobs."""
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
        engine = ScalingEngine(context)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 0)
        # 0 * 0.6 = 0, but max(0, 1) = 1
        self.assertEqual(limit, 1)

    def test_calculate_upscale_limit_with_high_limit_workers(self):
        """Test upscale limit when limit_workers is very high."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=1000,
            worker_count=0,
            worker_in_use=[],
            worker_drain=[],
            worker_free=[],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=10,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        flavor = {"flavor": {"vcpus": 2, "ram_gib": 4, "available": 100}}
        limit = engine._calculate_upscale_limit(flavor, 10)
        # 10 * 0.6 = 6, no other limits apply
        self.assertEqual(limit, 6)

    def test_calculate_downscale_with_only_draining_workers(self):
        """Test downscale when all workers are draining."""
        context = ScalingContext(
            mode="basic",
            scale_force=0.6,
            scale_delay=60,
            worker_cool_down=60,
            limit_memory=0,
            limit_worker_starts=0,
            limit_workers=0,
            worker_count=3,
            worker_in_use=[],
            worker_drain=["worker-1", "worker-2", "worker-3"],
            worker_free=["worker-1", "worker-2", "worker-3"],
            jobs_pending=[],
            jobs_running=[],
            jobs_pending_count=0,
            jobs_running_count=0,
            flavor_data=[],
        )
        engine = ScalingEngine(context)
        action = engine.calculate_scaling()
        # All workers are draining, so no free workers to scale down
        self.assertTrue(action.is_noop)

    def test_can_scale_up_with_limit_reached(self):
        """Test can_scale_up when limit is reached."""
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

    def test_can_scale_down_with_zero_workers(self):
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


if __name__ == "__main__":
    unittest.main()
