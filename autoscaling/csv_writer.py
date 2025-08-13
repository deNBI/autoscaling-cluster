import csv

from constants import (
    JOB_PENDING,
    JOB_RUNNING,
    LOG_CSV,
    NODE_ALLOCATED,
    NODE_DRAIN,
    NODE_IDLE,
    NODE_MIX,
)
from logger import setup_custom_logger

logger = setup_custom_logger(__name__)


def __csv_log_entry(scale_ud, worker_change, reason):
    """
    DEBUG
    Log current job and worker data
        - refresh node and worker data
        - write data to csv log
        - if required create csv file with header.
    :param scale_ud: marker
    :param worker_change: scale count
    :param reason: information
    :return:
    """
    logger.debug("---- __csv_log_entry %s ----", scale_ud)
    header = [
        "time",
        "scale",
        "worker_cnt",
        "worker_cnt_use",
        "worker_cnt_free",
        "cpu_alloc",
        "cpu_idle",
        "mem_alloc",
        "mem_idle",
        "jobs_cnt_running",
        "job_cnt_pending",
        "jobs_cpu_alloc",
        "jobs_cpu_pending",
        "jobs_mem_alloc",
        "jobs_mem_pending",
        "worker_mem_alloc_drain",
        "worker_mem_alloc",
        "worker_mem_idle_drain",
        "worker_mem_idle",
        "worker_mem_mix_drain",
        "worker_mem_mix",
        "worker_drain_cnt",
        "w_change",
        "reason",
        "credit",
        "cluster_worker",
        "worker_credit_data",
    ]
    log_entry = {}
    try:
        if not os.path.exists(LOG_CSV) or os.path.getsize(LOG_CSV) == 0:
            logger.debug("missing log file, create file and header")
            __csv_writer(LOG_CSV, header)
        if scale_ud == "C" and LOG_LEVEL == logging.DEBUG:
            pass
            # TODO when credits defined
        # credit_, cluster_worker_list, worker_credit_data = cluster_credit_usage()
        else:
            credit_ = 0
            cluster_worker_list = []
            worker_credit_data = {}

        jobs_pending_dict, jobs_running_dict = receive_job_data()
        (
            worker_json,
            worker_count,
            worker_in_use,
            worker_drain,
            _,
        ) = receive_node_data_db(True)
        if worker_json is None:
            return

        log_entry.update({"time": __get_time()})
        log_entry.update({"scale": scale_ud})
        log_entry.update({"job_cnt_pending": len(jobs_pending_dict)})
        log_entry.update({"jobs_cnt_running": len(jobs_running_dict)})
        log_entry = {
            **log_entry,
            **__current_usage(worker_json, {**jobs_pending_dict, **jobs_running_dict}),
        }
        log_entry.update({"worker_cnt": worker_count})
        log_entry.update({"worker_cnt_use": len(worker_in_use)})
        log_entry.update({"worker_cnt_free": (worker_count - len(worker_in_use))})
        log_entry.update({"worker_drain_cnt": len(worker_drain)})
        log_entry.update({"worker_drain_cnt": len(worker_drain)})
        log_entry.update({"w_change": worker_change})
        log_entry.update({"reason": reason})
        log_entry.update({"credit": credit_})
        log_entry.update({"cluster_worker": len(cluster_worker_list)})
        log_entry.update({"worker_credit_data": worker_credit_data})
    except NameError as e:
        logger.debug("job data from interface not usable, %s", e)

    w_data = []
    for data in header:
        if data not in log_entry:
            logger.debug("missing ", data)
            continue
        w_data.append(log_entry[data])
    __csv_writer(LOG_CSV, w_data)


def __csv_writer(csv_file, log_data):
    """
    Update log file.
    :param csv_file: write to file
    :param log_data: write this new line
    :return:
    """
    with open(csv_file, "a", newline="", encoding="utf8") as csvfile:
        f_writer = csv.writer(csvfile, delimiter=",")
        f_writer.writerow(log_data)


def __current_usage(worker_json, jobs_json):
    """
    DEBUG
    For csv log only, temporary logging.
    :param worker_json: worker information as json dictionary object
    :param jobs_json: job information as json dictionary object
    :return: usage dict
    """
    cpu_alloc, mem_alloc, mem_alloc_drain, mem_alloc_no_drain = 0, 0, 0, 0
    mem_mix_drain, mem_mix_no_drain = 0, 0

    cpu_idle, mem_idle, mem_idle_drain, mem_idle_no_drain = 0, 0, 0, 0
    jobs_cpu_alloc, jobs_mem_alloc = 0, 0
    jobs_cpu_pending, jobs_mem_pending = 0, 0
    if jobs_json:
        for key, value in jobs_json.items():
            if value["state"] == JOB_PENDING:
                jobs_cpu_pending += value["req_cpus"]
                jobs_mem_pending += value["req_mem"]
            elif value["state"] == JOB_RUNNING:
                jobs_cpu_alloc += value["req_cpus"]
                jobs_mem_alloc += value["req_mem"]
    if worker_json:
        for key, value in worker_json.items():
            if "worker" not in key:
                continue
            if (NODE_ALLOCATED in value["state"]) or (NODE_MIX in value["state"]):
                cpu_alloc += value["total_cpus"]
                mem_alloc += value["real_memory"]
                if NODE_DRAIN in value["state"]:
                    if NODE_ALLOCATED in value["state"]:
                        mem_alloc_drain += value["real_memory"]
                    elif NODE_MIX in value["state"]:
                        mem_mix_drain += value["real_memory"]
                else:
                    if NODE_ALLOCATED in value["state"]:
                        mem_alloc_no_drain += value["real_memory"]
                    elif NODE_MIX in value["state"]:
                        mem_mix_no_drain += value["real_memory"]
            elif NODE_IDLE in value["state"]:
                cpu_idle += value["total_cpus"]
                mem_idle += value["real_memory"]
                if NODE_DRAIN in value["state"]:
                    mem_idle_drain += value["real_memory"]
                else:
                    mem_idle_no_drain += value["real_memory"]

    usage_entry = {}
    usage_entry.update({"cpu_alloc": cpu_alloc})
    usage_entry.update({"mem_alloc": mem_alloc})
    usage_entry.update({"cpu_idle": cpu_idle})
    usage_entry.update({"mem_idle": mem_idle})
    usage_entry.update({"jobs_cpu_alloc": jobs_cpu_alloc})
    usage_entry.update({"jobs_mem_alloc": jobs_mem_alloc})
    usage_entry.update({"jobs_cpu_pending": jobs_cpu_pending})
    usage_entry.update({"jobs_mem_pending": jobs_mem_pending})
    usage_entry.update({"worker_mem_alloc_drain": mem_alloc_drain})
    usage_entry.update({"worker_mem_alloc": mem_alloc_no_drain})
    usage_entry.update({"worker_mem_idle_drain": mem_idle_drain})
    usage_entry.update({"worker_mem_idle": mem_idle_no_drain})
    usage_entry.update({"worker_mem_mix_drain": mem_mix_drain})
    usage_entry.update({"worker_mem_mix": mem_mix_no_drain})
    return usage_entry
