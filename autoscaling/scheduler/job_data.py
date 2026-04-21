"""
Job data handling for autoscaling scheduler.
Provides functions to receive and process job data from the scheduler.
"""
from typing import Optional

from autoscaling.scheduler.interface import SchedulerInterface, SchedulerJobState


def receive_job_data(scheduler: SchedulerInterface) -> tuple[dict, dict]:
    """
    Receive current job data from scheduler.

    Args:
        scheduler: Scheduler interface instance

    Returns:
        Tuple of (pending_jobs_dict, running_jobs_dict)
    """
    jobs_pending_dict = {}
    jobs_running_dict = {}

    job_live_dict = scheduler.get_job_data_live()

    if job_live_dict:
        for j_key, j_val in job_live_dict.items():
            if isinstance(j_val, SchedulerJobState):
                j_val = {
                    "jobid": j_val.jobid,
                    "state": j_val.state,
                    "state_str": j_val.state_str,
                    "req_cpus": j_val.req_cpus,
                    "req_mem": j_val.req_mem,
                    "temporary_disk": j_val.temporary_disk,
                    "priority": j_val.priority,
                    "jobname": j_val.jobname,
                    "nodes": j_val.nodes,
                    "elapsed": j_val.elapsed,
                    "comment": j_val.comment,
                }

            if j_val.get("state") == 0:  # PENDING
                jobs_pending_dict.update({j_key: j_val})
            elif j_val.get("state") == 1:  # RUNNING
                jobs_running_dict.update({j_key: j_val})

    return jobs_pending_dict, jobs_running_dict


def receive_completed_job_data(
    scheduler: SchedulerInterface, days: int
) -> dict:
    """
    Return completed jobs from the last x days.

    Args:
        scheduler: Scheduler interface instance
        days: Number of days to look back

    Returns:
        Dictionary of completed jobs
    """
    jobs_dict = scheduler.get_job_data(days)

    if jobs_dict:
        JOB_FINISHED = 3

        # Filter for completed jobs only
        for key in list(jobs_dict.keys()):
            job_data = jobs_dict[key]
            if isinstance(job_data, SchedulerJobState):
                job_state = job_data.state
            else:
                job_state = job_data.get("state", -1)

            if job_state != JOB_FINISHED:
                del jobs_dict[key]

        # Sort by end time
        def get_end_time(job):
            job_data = jobs_dict[job]
            if isinstance(job_data, SchedulerJobState):
                return 0  # SchedulerJobState doesn't have end time
            return job_data.get("end", 0)

        jobs_dict = dict(
            sorted(jobs_dict.items(), key=get_end_time, reverse=False)
        )

    if jobs_dict is None:
        jobs_dict = {}

    return jobs_dict


def print_job_data(job_data: list) -> None:
    """
    Print job data in a readable format.

    Args:
        job_data: List of (job_id, job_data) tuples
    """
    for job_id, job_data in job_data:
        if isinstance(job_data, SchedulerJobState):
            print(f"Job {job_id}: {job_data.state_str} - {job_data.jobname}")
        else:
            print(f"Job {job_id}: {job_data.get('state_str', 'UNKNOWN')} - {job_data.get('jobname', 'unknown')}")


def sort_jobs(jobs_pending_dict: dict, config_mode: dict) -> list:
    """
    Sort jobs by resource metrics or priority.

    Args:
        jobs_pending_dict: Jobs as dictionary
        config_mode: Configuration mode settings

    Returns:
        Sorted job list
    """
    if config_mode.get("resource_sorting", False):
        job_priority = sort_job_by_resources(jobs_pending_dict)
    else:
        job_priority = sort_job_priority(jobs_pending_dict)

    pending_jobs_percent = config_mode.get("pending_jobs_percent", 1.0)
    limit = int(pending_jobs_percent * len(job_priority))

    return job_priority[:limit]


def sort_job_priority(jobs_dict: dict) -> list:
    """
    Sort jobs by priority.

    Args:
        jobs_dict: Jobs as dictionary

    Returns:
        Job list sorted by priority (high priority at the top)
    """
    def sort_key(item):
        job = item[1]
        if isinstance(job, SchedulerJobState):
            return (
                job.priority,
                job.req_mem,
                job.req_cpus,
                job.temporary_disk,
            )
        return (
            job.get("priority", 0),
            job.get("req_mem", 0),
            job.get("req_cpus", 0),
            job.get("temporary_disk", 0),
        )

    return sorted(
        jobs_dict.items(),
        key=sort_key,
        reverse=True,
    )


def sort_job_by_resources(jobs_dict: dict) -> list:
    """
    Sort jobs by resources (memory, cpu, ephemeral).

    Args:
        jobs_dict: Jobs as dictionary

    Returns:
        Job list sorted by resources (high resources at the top)
    """
    def sort_key(item):
        job = item[1]
        if isinstance(job, SchedulerJobState):
            return (
                job.req_mem,
                job.req_cpus,
                job.temporary_disk,
            )
        return (
            job.get("req_mem", 0),
            job.get("req_cpus", 0),
            job.get("temporary_disk", 0),
        )

    return sorted(
        jobs_dict.items(),
        key=sort_key,
        reverse=True,
    )
