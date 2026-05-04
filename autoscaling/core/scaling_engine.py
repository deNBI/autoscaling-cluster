"""
Scaling engine for autoscaling.
Contains the core scaling decision logic.
"""

from typing import Optional

from autoscaling.core.state import (
    ScalingAction,
    ScalingContext,
    WorkerResource,
)


class ScalingEngine:
    """
    Engine for making scaling decisions.
    Calculates how many workers to scale up or down.
    """

    def __init__(self, context: ScalingContext):
        """
        Initialize the scaling engine.

        Args:
            context: Scaling context with all necessary information
        """
        self.context = context

    def calculate_scaling(self) -> ScalingAction:
        """
        Calculate the scaling action based on current state.

        Returns:
            ScalingAction with the calculated scaling decision
        """
        # Check if we need to delay
        if self.context.mode == "delay":
            return ScalingAction.noop_action("Waiting for cool-down period")

        # Calculate upscale
        upscale_action = self._calculate_upscale()

        # Calculate downscale
        downscale_action = self._calculate_downscale()

        # Combine actions
        if upscale_action.upscale:
            return upscale_action
        elif downscale_action.downscale:
            return downscale_action
        else:
            return ScalingAction.noop_action("No scaling needed")

    def _calculate_upscale(self) -> ScalingAction:
        """
        Calculate upscale action.

        Returns:
            ScalingAction for upscale (or no-op)
        """
        if not self.context.can_scale_up:
            return ScalingAction.noop_action("Cannot scale up (limits reached)")

        pending_jobs = self.context.jobs_pending
        if not pending_jobs:
            return ScalingAction.noop_action("No pending jobs")

        # Get the most demanding pending job
        job = self._get_most_demanding_job(pending_jobs)
        if not job:
            return ScalingAction.noop_action("No valid pending jobs")

        # Get compatible flavors
        compatible_flavors = self._get_compatible_flavors(job)
        if not compatible_flavors:
            return ScalingAction.noop_action("No compatible flavors")

        # Select the best flavor
        selected_flavor = self._select_flavor(compatible_flavors, job)
        if not selected_flavor:
            return ScalingAction.noop_action("No flavor selected")

        # Calculate how many workers we need
        upscale_limit = self._calculate_upscale_limit(selected_flavor, len(pending_jobs))

        if upscale_limit <= 0:
            return ScalingAction.noop_action("Upscale limit is zero or negative")

        return ScalingAction.upscale_action(
            flavor=selected_flavor["flavor"]["name"],
            count=upscale_limit,
            reason=f"Scale up for {len(pending_jobs)} pending jobs",
        )

    def _calculate_downscale(self) -> ScalingAction:
        """
        Calculate downscale action.

        Returns:
            ScalingAction for downscale (or no-op)
        """
        if not self.context.can_scale_down:
            return ScalingAction.noop_action("Cannot scale down")

        # Only scale down if we have free workers
        free_workers = self.context.worker_free
        if not free_workers:
            return ScalingAction.noop_action("No free workers to scale down")

        # Get workers to scale down
        workers_to_scale = self._get_workers_to_scale_down()

        if not workers_to_scale:
            return ScalingAction.noop_action("No workers to scale down")

        return ScalingAction.downscale_action(
            workers=workers_to_scale,
            reason=f"Scale down {len(workers_to_scale)} free workers",
        )

    def _get_most_demanding_job(self, jobs: list[dict]) -> Optional[dict]:
        """
        Get the most demanding job from the list.

        Args:
            jobs: List of job dictionaries

        Returns:
            Most demanding job or None
        """
        if not jobs:
            return None

        # Sort by memory requirement (descending)
        sorted_jobs = sorted(jobs, key=lambda j: j.get("req_mem", 0), reverse=True)
        return sorted_jobs[0]

    def _get_compatible_flavors(self, job: dict) -> list[dict]:
        """
        Get flavors compatible with a job.

        Args:
            job: Job dictionary

        Returns:
            List of compatible flavors
        """
        compatible = []

        for flavor in self.context.flavor_data:
            fv = flavor.get("flavor", {})

            # Check CPU
            if fv.get("vcpus", 0) < job.get("req_cpus", 0):
                continue

            # Check memory
            if fv.get("ram_gib", 0) * 1024 < job.get("req_mem", 0):
                continue

            # Check disk
            if fv.get("temporary_disk", 0) < job.get("temporary_disk", 0):
                continue

            compatible.append(flavor)

        return compatible

    def _select_flavor(self, flavors: list[dict], job: dict) -> Optional[dict]:
        """
        Select the best flavor for a job.

        Args:
            flavors: List of compatible flavors
            job: Job dictionary

        Returns:
            Selected flavor or None
        """
        if not flavors:
            return None

        # Default to smallest flavor
        smallest = min(
            flavors,
            key=lambda f: f.get("flavor", {}).get("ram_gib", float("inf")),
        )

        # If default flavor is enabled and meets requirements
        if self.context.flavor_default:
            for flavor in flavors:
                if flavor.get("flavor", {}).get("name") == self.context.flavor_default:
                    fv = flavor.get("flavor", {})
                    if (
                        fv.get("ram_gib", 0) * 1024 >= job.get("req_mem", 0)
                        and fv.get("vcpus", 0) >= job.get("req_cpus", 0)
                        and fv.get("temporary_disk", 0) >= job.get("temporary_disk", 0)
                    ):
                        return flavor

        return smallest

    def _calculate_upscale_limit(self, flavor: dict, pending_count: int) -> int:
        """
        Calculate the number of workers to scale up.

        Args:
            flavor: Selected flavor
            pending_count: Number of pending jobs

        Returns:
            Number of workers to scale up
        """
        fv = flavor.get("flavor", {})

        # Base limit from force
        base_limit = int(pending_count * self.context.scale_force)
        base_limit = max(base_limit, 1)  # Always scale up at least 1

        # Apply limits
        limit = base_limit

        # Worker starts limit
        if self.context.limit_worker_starts > 0:
            limit = min(limit, self.context.limit_worker_starts)

        # Total workers limit
        if self.context.limit_workers > 0:
            limit = min(limit, self.context.limit_workers - self.context.worker_count)

        # Memory limit
        if self.context.limit_memory > 0:
            fv_memory = fv.get("ram_gib", 0) * 1024  # GB to MB
            available_memory = self.context.limit_memory - (self.context.worker_count * fv_memory)
            memory_limit = max(int(available_memory / fv_memory), 0)
            limit = min(limit, memory_limit)

        # Flavor availability limit
        flavor_count = fv.get("available", float("inf"))
        limit = min(limit, int(flavor_count))

        assert isinstance(limit, int)
        return limit

    def _get_workers_to_scale_down(self) -> list[str]:
        """
        Get list of workers to scale down.

        Returns:
            List of worker hostnames
        """
        # Get all workers
        self.context.worker_in_use + self.context.worker_free

        # Exclude draining workers
        draining = set(self.context.worker_drain)

        # Get free workers (workers that are not in use and not draining)
        free_workers = [w for w in self.context.worker_free if w not in draining]

        return free_workers

    def calculate_worker_resources(self, flavor: dict) -> WorkerResource:
        """
        Calculate resource requirements for a worker.

        Args:
            flavor: Flavor dictionary

        Returns:
            WorkerResource with CPU, memory, and disk requirements
        """
        fv = flavor.get("flavor", {})

        return WorkerResource(
            cpu=fv.get("vcpus", 1),
            memory=fv.get("ram_gib", 1) * 1024,  # GB to MB
            disk=fv.get("temporary_disk", 0),
        )


def calculate_scaling(context: ScalingContext) -> ScalingAction:
    """
    Convenience function to calculate scaling.

    Args:
        context: Scaling context

    Returns:
        ScalingAction with the calculated scaling decision
    """
    engine = ScalingEngine(context)
    return engine.calculate_scaling()
