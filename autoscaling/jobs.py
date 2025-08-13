from classes.configuration import Configuration
from classes.job_info import JobInfo
from classes.scheduler_interface import SchedulerInterface
from constants import JOB_PENDING, JOB_RUNNING
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def receive_job_data(scheduler_interface: SchedulerInterface):
    """
    Receive current job data.
    :return:
        - dictionary with pending jobs
        - dictionary with running jobs
    """

    jobs_pending_list: list[JobInfo] = []
    jobs_running_list: list[JobInfo] = []
    job_list: list[JobInfo] = scheduler_interface.job_data_live()
    # TODO
    # if LOG_LEVEL == logging.DEBUG and not job_live_dict:
    #   logger.debug("try again, maybe logging problems")
    #  time.sleep(2)
    # job_live_dict = scheduler_interface.job_data_live()
    for job in job_list:
        if job.state == JOB_PENDING:
            jobs_pending_list.append(job)
        elif job.state == JOB_RUNNING:
            jobs_running_list.append(job)

    return jobs_pending_list, jobs_running_list


def receive_completed_job_data(
    from_last_days: int, scheduler_interface: SchedulerInterface
):
    """
    Return completed jobs from the last x days.
    :from_last_days: number of days
    :return: jobs dictionary
    """
    jobs_list: list[JobInfo] = scheduler_interface.fetch_scheduler_job_data(
        num_days=from_last_days
    )
    jobs_list_done: list[JobInfo] = []
    for job in jobs_list:
        if job.is_finished():
            jobs_list_done.append(job)
        jobs_list_done = sorted(
            jobs_list_done, key=lambda k: job.end_time, reverse=False
        )
        for job in jobs_list_done:
            logger.debug(job.end_time)
    return jobs_list_done


def sort_jobs(jobs_pending: list[JobInfo], config: Configuration):
    """
    Sort jobs by resource metrics or priority.
    :param jobs_pending_dict: jobs as json dictionary object
    :return: sorted job list
    """
    if config.resource_sorting:
        job_priority = sort_job_by_resources(jobs_pending)
    else:
        job_priority = sort_job_by_priority(jobs_pending)
    pending_jobs_percent = config.pending_jobs_percent
    limit = int(pending_jobs_percent * len(job_priority))
    logger.debug(
        f"Using {pending_jobs_percent} percent of the jobs for calculating --> {limit} Jobs"
    )
    return job_priority[:limit]


def sort_job_by_priority(job_list: list[JobInfo]) -> list[JobInfo]:
    """
    Sort jobs by priority.
    :param job_list: list of JobInfo
    :return: sorted list of JobInfo, high priority at the top
    """
    sorted_jobs = sorted(
        job_list,
        key=lambda job: (
            job.priority,
            job.req_mem,
            job.req_cpus,
            job.temporary_disk,
        ),
        reverse=True,
    )
    return sorted_jobs


def sort_job_by_resources(job_list: list[JobInfo]) -> list[JobInfo]:
    """
    Sort jobs by resource usage: memory, cpu, ephemeral.
    :param job_list: list of JobInfo
    :return: list of JobInfo sorted by req_mem, req_cpus, temporary_disk (high to low)
    """
    sorted_job_list = sorted(
        job_list,
        key=lambda job: (job.req_mem, job.req_cpus, job.temporary_disk),
        reverse=True,
    )
    return sorted_job_list


def print_job_history(job_history: list[JobInfo]):
    for job in job_history:
        job.print_job_data()
