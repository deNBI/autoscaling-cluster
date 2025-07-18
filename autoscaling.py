# !/usr/bin/python3

import abc
import calendar
import csv
import datetime
import difflib
import enum
import hashlib
import inspect
import json
import logging
import math
import os
import re
import signal
import subprocess
import sys
import textwrap
import time
import traceback
from functools import total_ordering
from logging.handlers import RotatingFileHandler
from multiprocessing import Process
from pathlib import Path
from pprint import pformat
from subprocess import PIPE, Popen

import matplotlib.backends.backend_pdf
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import yaml

LOG_LEVEL = logging.DEBUG
OUTDATED_SCRIPT_MSG = (
    "Your script is outdated [VERSION: {SCRIPT_VERSION} - latest is {LATEST_VERSION}] "
    "-  please download the current version and run it again!"
)

PORTAL_LINK = "https://cloud.denbi.de"
AUTOSCALING_VERSION_KEY = "AUTOSCALING_VERSION"
AUTOSCALING_VERSION = "2.1.0"
SCALING_TYPE = "autoscaling"

REPO_LINK = "https://github.com/deNBI/autoscaling-cluster/"
REPO_API_LINK = "https://api.github.com/repos/deNBI/autoscaling-cluster/"

RAW_REPO_LINK = "https://raw.githubusercontent.com/deNBI/autoscaling-cluster/"
HTTP_CODE_OK = 200
HTTP_CODE_UNAUTHORIZED = 401
HTTP_CODE_OUTDATED = 400
AUTOSCALING_FOLDER = os.path.dirname(os.path.realpath(__file__)) + "/"
SCALING_SCRIPT_FILE = AUTOSCALING_FOLDER + "scaling.py"

IDENTIFIER = "autoscaling"

FILE_CONFIG = IDENTIFIER + "_config.yaml"
FILE_CONFIG_YAML = AUTOSCALING_FOLDER + FILE_CONFIG
FILE_ID = IDENTIFIER + ".py"
FILE_PROG = AUTOSCALING_FOLDER + FILE_ID
FILE_PID = IDENTIFIER + ".pid"
SOURCE_LINK_CONFIG = REPO_LINK + FILE_CONFIG

CLUSTER_PASSWORD_FILE = AUTOSCALING_FOLDER + "cluster_pw.json"

LOG_FILE = AUTOSCALING_FOLDER + IDENTIFIER + ".log"
LOG_CSV = AUTOSCALING_FOLDER + IDENTIFIER + ".csv"

NORM_HIGH = None
NORM_LOW = 0.0001
FORCE_LOW = 0.0001

FLAVOR_GPU_ONLY = -1
FLAVOR_GPU_REMOVE = 1
# ----- NODE STATES -----
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_IDLE = "IDLE"
NODE_DRAIN = "DRAIN"
NODE_DOWN = "DOWN"
NODE_DUMMY = "bibigrid-worker-autoscaling_dummy"
NODE_DUMMY_REQ = True
WORKER_SCHEDULING = "SCHEDULING"
WORKER_PLANNED = "PLANNED"
WORKER_ERROR = "ERROR"
WORKER_CREATION_FAILED = "CREATION_FAILED"
WORKER_FAILED = "FAILED"
WORKER_PORT_CLOSED = "PORT_CLOSED"
WORKER_ACTIVE = "ACTIVE"

# flavor depth options
DEPTH_MAX_WORKER = -2
DEPTH_MULTI_SINGLE = -3
DEPTH_MULTI = -1

# ----- JOB STATE IDs -----
JOB_FINISHED = 3
JOB_PENDING = 0
JOB_RUNNING = 1

DOWNSCALE_LIMIT = 0
WAIT_CLUSTER_SCALING = 10
FLAVOR_HIGH_MEM = "hmf"

DATABASE_FILE = AUTOSCALING_FOLDER + IDENTIFIER + "_database.json"
DATA_LONG_TIME = 7
DATABASE_WORKER_PATTERN = " + ephemeral"

HOME = str(Path.home())
PLAYBOOK_DIR = HOME + "/playbook"
PLAYBOOK_VARS_DIR = HOME + "/playbook/vars"
COMMON_CONFIGURATION_YML = PLAYBOOK_VARS_DIR + "/common_configuration.yml"
ANSIBLE_HOSTS_FILE = PLAYBOOK_DIR + "/ansible_hosts"
INSTANCES_YML = PLAYBOOK_VARS_DIR + "/instances.yml"
SCHEDULER_YML = PLAYBOOK_VARS_DIR + "/scheduler_config.yml"


@total_ordering
class ScaleState(enum.Enum):
    """
    Possible program scaling states.
    """

    UP = 1
    DOWN = 0
    SKIP = 2
    DONE = 3
    DOWN_UP = 4
    FORCE_UP = 5
    DELAY = -1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class Rescale(enum.Enum):
    """
    Rescaling types.
    """

    INIT = 0
    CHECK = 1
    NONE = 2


class TerminateProtected:
    """
    Protect a section of code from being killed by SIGINT or SIGTERM.
    Example:
        with TerminateProtected():
            function_1()
            function_2()
    """

    killed = False

    def _handler(self, signum, frame):
        logging.error(
            "Received SIGINT or SIGTERM! Finishing this code block, then exiting."
        )
        self.killed = True

    def __enter__(self):
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, type, value, traceback):
        if self.killed:
            sys.exit(0)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)


class ProcessFilter(logging.Filter):
    """
    Only accept log records from a specific pid.
    """

    def __init__(self, pid):
        self._pid = pid

    def filter(self, record):
        return record.process == self._pid


class SchedulerInterface(abc.ABC):
    """
    Scheduler interface template.
    """

    @abc.abstractmethod
    def scheduler_function_test(self):
        """
        Test if scheduler can retrieve job and node data.
        :return: boolean, success
        """
        return False

    @abc.abstractmethod
    def fetch_scheduler_node_data(self):
        """
        Read scheduler data from database and return a json object with node data.
        Verify database data.
        Example:
        {
        'bibigrid-worker-2-849-mhz6y1u0tnesizi':
            {'cpus': 28,
               'free_mem': 226280,
               'gres': ['gpu:0'],
               'node_hostname': 'bibigrid-worker-2-849-mhz6y1u0tnesizi',
               'real_memory': 236000,
               'state': 'MIX',
               'tmp_disk': 1000000},
         'bibigrid-worker-2-850-mhz6y1u0tnesizi':
            {'cpus': 28,
               'free_mem': 202908,
               'gres': ['gpu:0'],
               'node_hostname': 'bibigrid-worker-2-850-mhz6y1u0tnesizi',
               'real_memory': 236000,
               'state': 'ALLOC',
               'tmp_disk': 1000000}
         }

         Node states:
          * `NODE_ALLOCATED` = `'ALLOC'`
          * `NODE_MIX` = `'MIX'`
          * `NODE_IDLE` = `'IDLE'`
          * `NODE_DRAIN` = `'DRAIN'`
          * `NODE_DOWN` = `'DOWN'`
        :return
            - json object with node data,
            - on error, return None
        """
        return None

    @abc.abstractmethod
    def node_data_live(self):
        """
        Receive node data from scheduler, without database usage. Provide more recent data.
        :return: json object with node data
        """
        return None

    @abc.abstractmethod
    def job_data_live(self):
        """
        Receive job data from scheduler, without job history and database usage. Provide more recent data.
        :return: json object with job data
        """
        return None

    @abc.abstractmethod
    def fetch_scheduler_job_data(self, num_days):
        """
        Read scheduler data from database and return a json object with job data.
        Define the prerequisites for the json data structure and the required data.

        Job states:
            - 0: PENDING
            - 1: RUNNING
            - 3: COMPLETED

        Example:
        {20:
            {
            ...
            },
        {33: {'cluster': 'bibigrid',
              'elapsed': 0,
              'end': 1668258570,
              'jobid': 33,
              'jobname': 'nf-sayHello_(6)',
              'nodes': 'bibigrid-worker-3-1-gpwapcgoqhgkctt',
              'partition': 'debug',
              'priority': 71946,
              'req_cpus': 1,
              'req_mem': 5,
              'start': 1668258570,
              'state': 0,
              'state_str': 'PENDING',
              'tmp_disk': 0,
              'comment': None}
        }
        :param num_days:
        :return: json object with job data, return None on error
        """
        return None

    @abc.abstractmethod
    def set_node_to_drain(self, w_key):
        """
        Set scheduler option, node to drain.
            - currently running jobs will keep running
            - no further job will be scheduled on that node
        :param w_key: node name
        :return:
        """

    @abc.abstractmethod
    def set_node_to_resume(self, w_key):
        """
        Set scheduler option, remove drain from required node
            - further jobs will be scheduled on that node
        :param w_key: node name
        :return:
        """


class SlurmInterface(SchedulerInterface):
    """
    Interface for the slurm scheduler.
    """

    def scheduler_function_test(self):
        """
        Test if scheduler can retrieve job and node data.
        :return: boolean, success
        """
        # try node data
        try:
            pyslurm.Nodes.load()
        except ValueError as e:
            logger.error("Error: unable to receive node data \n%s", e)
            return False
        # try job data
        try:
            pyslurm.db.Jobs.load()
        except ValueError as e:
            logger.error("Error: unable to receive job data \n%s", e)
            return False
        return True

    def fetch_scheduler_node_data(self):
        """
        Read scheduler data from database and return a json object with node data.
        Verify database data.
        :return:
            - json object with node data
            - on error, return None
        """
        try:
            nodes = pyslurm.Nodes.load()
            node_dict = {}
            for key, value in nodes.items():
                node_dict[key] = value.to_dict()
            logger.info(node_dict)
            return node_dict
        except ValueError as e:
            logger.error("Error: unable to receive node data \n%s", e)
            return None

    @staticmethod
    def get_json(slurm_command):
        """
        Get all slurm data as string from squeue or sinfo
        :param slurm_command:
        :return: json data from slurm output
        """
        process = Popen(
            [slurm_command, "-o", "%all"],
            stdout=PIPE,
            stderr=PIPE,
            shell=False,
            universal_newlines=True,
        )
        proc_stdout, proc_stderr = process.communicate()
        lines = proc_stdout.split("\n")
        header_line = lines.pop(0)
        header_cols = header_line.split("|")
        entries = []
        for line in lines:
            parts = line.split("|")
            val_dict = {}
            if len(parts) == len(header_cols):
                for i, key in enumerate(header_cols):
                    val_dict[key] = parts[i]
                entries.append(val_dict)
        return entries

    @staticmethod
    def __convert_node_state(state):
        """
        Convert sinfo state to detailed database version.
            - not possible to identify MIX+DRAIN
        :param state: node state to convert
        :return:
        """
        state = state.upper()
        if "DRNG" in state:
            state = NODE_ALLOCATED + "+" + NODE_DRAIN
        elif "DRAIN" in state:
            state = NODE_IDLE + "+" + NODE_DRAIN
        elif "ALLOC" in state:
            state = NODE_ALLOCATED
        elif "IDLE" in state:
            state = NODE_IDLE
        elif "MIX" in state:
            state = NODE_MIX
        elif "DOWN" in state:
            state = NODE_DOWN
        return state

    @staticmethod
    def __convert_job_state(state):
        """
        Convert job state string to id.
        :param state: job state to convert
        :return:
        """
        state = state.upper()
        if state == "PENDING":
            state_id = JOB_PENDING
        elif state == "RUNNING":
            state_id = JOB_RUNNING
        else:
            state_id = -1
        return state_id

    def node_data_live(self):
        """
        Receive node data from sinfo.
        :return: json object
        """

        node_dict = self.get_json("sinfo")
        node_dict_format = {}
        for i in node_dict:
            gres = i["GRES "].replace(" ", "")
            free_mem = None
            if "null" in gres:
                gpu = ["gpu:0"]
            else:
                gpu = [gres]
            if i["FREE_MEM"].isnumeric():
                free_mem = int(i["FREE_MEM"])
            node_dict_format.update(
                {
                    i["HOSTNAMES"]: {
                        "total_cpus": int(i["CPUS"]),
                        "real_memory": int(i["MEMORY"]),
                        "state": self.__convert_node_state(i["STATE"]),
                        "temporary_disk": int(i["TMP_DISK"]),
                        "node_hostname": i["HOSTNAMES"],
                        "gres": gpu,
                        "free_mem": free_mem,
                    }
                }
            )
        return node_dict_format

    @staticmethod
    def __time_to_seconds(time_str):
        """
        Convert a time string to seconds.
        :param time_str: example "1-04:27:12"
        :return: seconds
        """

        value = 0
        ftr = [60, 1]
        try:
            time_split = time_str.split("-")
            if len(time_split) > 1:
                value += int(time_split[0]) * 3600 * 24
                time_split = time_split[1]
            else:
                time_split = time_split[0]

            time_split = time_split.split(":")
            if len(time_split) == 3:
                ftr = [3600, 60, 1]
            value += sum([a * b for a, b in zip(ftr, map(int, time_split))])
        except (ValueError, IndexError) as e:
            logger.error("Error: time data %s", e)
        return value

    def job_data_live(self):
        """
        Fetch live data from scheduler without database usage.
        Use squeue output from slurm, should be faster up-to-date
        :return: return current job data (no history) as dict
        """
        squeue_json = self.get_json("squeue")
        job_live_dict = {}

        for i in squeue_json:
            mem = re.split("(\\d+)", i["MIN_MEMORY"])
            disk = re.split("(\\d+)", i["MIN_TMP_DISK"])
            if i["COMMENT"] == "(null)":
                comment = None
            else:
                comment = i["COMMENT"]

            state_id = self.__convert_job_state(i["STATE"])

            if state_id >= 0:
                job_live_dict.update(
                    {
                        i["JOBID"]: {
                            "jobid": int(i["JOBID"]),
                            "req_cpus": int(i["MIN_CPUS"]),
                            "req_mem": self.memory_size_ib(mem[1], mem[2]),
                            "state": state_id,
                            "state_str": i["STATE"],
                            "temporary_disk": self.memory_size(disk[1], disk[2]),
                            "priority": int(i["PRIORITY"]),
                            "jobname": i["NAME"],
                            "req_gres": i["TRES_PER_NODE"],
                            "nodes": i["NODELIST"],
                            "elapsed": self.__time_to_seconds(i["TIME"]),
                            "comment": comment,
                        }
                    }
                )
        return job_live_dict

    @staticmethod
    def memory_size_ib(num, ending):
        if ending == "G":
            tmp_disk = convert_gb_to_mib(num)
        elif ending == "M":
            tmp_disk = int(num)
        elif ending == "T":
            tmp_disk = convert_tb_to_mib(num)
        else:
            tmp_disk = int(num)
        return tmp_disk

    @staticmethod
    def memory_size(num, ending):
        if ending == "G":
            tmp_disk = convert_gb_to_mb(num)
        elif ending == "M":
            tmp_disk = int(num)
        elif ending == "T":
            tmp_disk = convert_tb_to_mb(num)
        else:
            tmp_disk = int(num)
        return tmp_disk

    def add_jobs_tmp_disk(self, jobs_dict):
        """
        Add tmp disc data value to job dictionary.
        :param jobs_dict: modified jobs_dict
        :return:
        """
        for key, entry in jobs_dict.items():
            if "temporary_disk" not in entry:
                jobs_dict[key]["temporary_disk"] = 0
        return dict(jobs_dict)

    def fetch_scheduler_job_data_by_range(self, start, end):
        """
        Read scheduler data from database and return a json object with job data.
        Define the prerequisites for the json data structure and the required data.

        :param start: start time range
        :param end: end time range
        :return json object with job data, return None on error
        """
        try:
            db_filter = pyslurm.db.JobFilter(
                start_time=start.encode("utf-8"), end_time=end.encode("utf-8")
            )
            jobs_dict = {
                job.id: job.to_dict()
                for job in pyslurm.db.Jobs.load(db_filter).values()
            }
            jobs_dict = self.add_jobs_tmp_disk(jobs_dict)
            return jobs_dict
        except ValueError as e:
            logger.debug("Error: unable to receive job data \n%s", e)
            return None

    def fetch_scheduler_job_data(self, num_days):
        """
        Read job data from database and return a json object with job data.
        :param num_days: number of past days
        :return: jobs_dict
        """
        start = (
            datetime.datetime.utcnow() - datetime.timedelta(days=num_days)
        ).strftime("%Y-%m-%dT00:00:00")
        end = (datetime.datetime.utcnow() + datetime.timedelta(days=num_days)).strftime(
            "%Y-%m-%dT00:00:00"
        )
        return self.fetch_scheduler_job_data_by_range(start, end)

    def set_node_to_drain(self, w_key):
        """
        Set scheduler option, node to drain.
            - currently running jobs will keep running
            - no further job will be scheduled on that node
        :param w_key: node name
        :return:
        """
        logger.debug("drain node %s  ", w_key)
        os.system(
            "sudo scontrol update nodename="
            + str(w_key)
            + " state=drain reason=REPLACE"
        )

    def set_node_to_resume(self, w_key):
        """
        Set scheduler option, remove drain from required node
            - further jobs will be scheduled on that node
        :param w_key: node name
        :return:
        """
        logger.debug("undrain node %s  ", w_key)
        os.system("sudo scontrol update nodename=" + str(w_key) + " state=resume")


def run_ansible_playbook():
    """
    Run ansible playbook with system call.
        - ansible-playbook -v -i ansible_hosts site.yml
    :return: boolean, success
    """
    logger.debug("--- run playbook ---")
    os.chdir(PLAYBOOK_DIR)
    forks_num = str(os.cpu_count() * 4)
    result_ = False

    try:
        # Run the subprocess and capture its output
        process = subprocess.Popen(
            [
                "ansible-playbook",
                "--forks",
                forks_num,
                "-v",
                "-i",
                "ansible_hosts",
                "site.yml",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,  # To work with text output
        )

        # Log the output with the [ANSIBLE] prefix
        for line in process.stdout:
            logger.debug("[ANSIBLE] " + line.strip())

        # Wait for the subprocess to complete and get the return code
        return_code = process.wait()

        if return_code == 0:
            logger.debug("ansible playbook success")
            result_ = True
        else:
            logger.error(
                "ansible playbook failed with return code: " + str(return_code)
            )
    except Exception as e:
        logger.error("Error running ansible-playbook: " + str(e))

    return result_


def rescale_init():
    """
    Combine rescaling steps
        - start scale function to generate and update the ansible playbook
        - run the ansible playbook
    :return: boolean, success    :param cluster_data: cluster data from api
    """

    return update_all_yml_files_and_run_playbook()


def get_version():
    """
    Print the current autoscaling version.
    :return:
    """
    logger.debug(f"Version: {AUTOSCALING_VERSION}")


def __update_playbook_scheduler_config(set_config):
    """
    Update playbook scheduler configuration, skip with error message when missing.
    :return:
    """
    scheduler_config = __read_yaml_file(SCHEDULER_YML)
    if scheduler_config is None:
        logger.info("ignore individual scheduler settings, not supported")
    elif scheduler_config["scheduler_config"]["priority_settings"]:
        scheduler_config["scheduler_config"]["priority_settings"] = set_config
        ___write_yaml_file(SCHEDULER_YML, scheduler_config)
        logger.debug("scheduler config updated")
    else:
        logger.error("scheduler_config, missing: scheduler_config, scheduler_settings")


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


def cluster_credit_usage():
    """
    DEBUG
    Summarize the current credit values of the running workers on the cluster.
    Stand-alone function for regular csv log creation including API data query.
    :return: cluster_credit_usage
    """
    flavors_data = get_usable_flavors(True, False)
    cluster_workers = get_cluster_workers_from_api()
    credit_sum = 0
    worker_credit_data = {}
    if cluster_workers is not None:
        for c_worker in cluster_workers:
            if c_worker["status"].upper() == WORKER_ACTIVE:
                credit_sum += cluster_worker_to_credit(flavors_data, c_worker)
                if c_worker["flavor"] in worker_credit_data:
                    worker_credit_data[c_worker["flavor"]]["cnt"] += 1
                else:
                    worker_credit_data.update(
                        {
                            c_worker["flavor"]: {
                                "cnt": 0,
                                "credit": cluster_worker_to_credit(
                                    flavors_data, c_worker
                                ),
                            }
                        }
                    )
    else:
        cluster_workers = []
    logger.info("current worker credit %s", credit_sum)
    return credit_sum, cluster_workers, worker_credit_data


def cluster_worker_to_credit(flavors_data, cluster_worker):
    """
    Return credit costs per hour
    :param flavors_data: flavor data
    :param cluster_worker: cluster data from worker
    """

    fv_data = cluster_worker_to_flavor(flavors_data, cluster_worker)
    if fv_data:
        try:
            credit = float(fv_data["flavor"].get("credits_costs_per_hour", 0))
            return credit
        except KeyError:
            logger.error("KeyError, missing credit in flavor data")
    return 0


def worker_to_cluster_data(key, cluster_workers):
    if cluster_workers:
        for c_worker in cluster_workers:
            if c_worker["hostname"] == key:
                return c_worker
        logger.error("worker %s not in cluster data", key)
    return None


def cluster_worker_to_flavor(flavors_data, cluster_worker):
    """
    Returns a corresponding flavor to a worker
    :param flavors_data: flavor data
    :param cluster_worker: cluster data from worker
    :return: flavor
    """

    if cluster_worker:
        for fv_data in flavors_data:
            if cluster_worker["flavor"]["name"] == fv_data["flavor"]["name"]:
                return fv_data
        logger.error("flavor not found for worker %s", cluster_worker["flavor"])
    return None


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
            credit_, cluster_worker_list, worker_credit_data = cluster_credit_usage()
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


def __read_yaml_file(file_path):
    """
    Read yaml data from file.
    :param file_path:
    :return:
    """

    try:
        with open(file_path, "r", encoding="utf8") as stream:
            yaml_data = yaml.safe_load(stream)
        return yaml_data
    except yaml.YAMLError as exc:
        logger.error(exc)
        return None
    except EnvironmentError:
        return None


def ___write_yaml_file(yaml_file_target, yaml_data):
    """
    Write yaml data to file.
    :param yaml_file_target:
    :param yaml_data:
    :return:
    """
    with open(yaml_file_target, "w+", encoding="utf8") as target:
        try:
            yaml.dump(yaml_data, target)
        except yaml.YAMLError as exc:
            logger.debug(exc)
            sys.exit(1)


def read_config_yaml():
    """
    Read config from yaml file.
    :return:
    """
    yaml_config = __read_yaml_file(FILE_CONFIG_YAML)
    logger.info("Load config from yaml file %s", FILE_CONFIG_YAML)
    if not yaml_config:
        logger.error("can not read configuration file")
        return None
    return yaml_config["scaling"]


def read_cluster_id():
    """
    Read cluster id from hostname
        - example hostname: bibigrid-master-clusterID
    :return: cluster id
    """

    with open("/etc/hostname", "r", encoding="utf8") as file:
        try:
            cluster_id_ = file.read().rstrip().split("-")[2]
        except (ValueError, IndexError):
            logger.error(
                "error: wrong cluster name \nexample: bibigrid-master-clusterID"
            )
            sys.exit(1)
    return cluster_id_


def __get_scaling_script_url():
    return config_data["scaling_script_url"]


def __get_portal_url_webapp():
    return config_data["portal_webapp_link"]


def get_wrong_password_msg():
    logger.error(
        f"The password seems to be wrong. Please generate a new one "
        f"for the cluster on the Cluster Overview ({__get_portal_url_webapp()})"
        f" and register the password with 'autoscaling -p'."
    )


def __get_portal_url_scaling():
    scaling_link = config_data["portal_scaling_link"]
    return scaling_link if scaling_link.endswith("/") else scaling_link + "/"


def get_url_scale_up():
    """
    :return: return portal api scale up url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-up/"


def get_url_scale_down_specific():
    """
    :return: return portal api scale down specific url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-down/specific/"


def get_url_info_cluster():
    """
    :return: return portal api info url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-data/"


def get_url_info_flavors():
    """
    :return: return portal api flavor info url
    """
    return __get_portal_url_scaling() + cluster_id + "/usable_flavors/"


def reduce_flavor_memory(mem_gb):
    """
    Receive raw memory in GB and reduce this to a usable value,
    memory reduces to os requirements and other circumstances. Calculation according to BiBiGrid setup.
    :param mem_gb: memory
    :return: memory reduced value (mb)
    """
    mem = convert_gb_to_mb(mem_gb)
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    sub = int(mem / 16)
    if mem <= mem_border:
        sub = max(sub, mem_min)
    else:
        sub = min(sub, mem_max)
    return int(mem - sub)


def translate_metrics_to_flavor(
    cpu, mem, tmp_disk, flavors_data, available_check, quiet
):
    """
    Select the flavor by cpu and memory.
    In the future, the server could select the flavor itself based on the required CPU and memory data.
        - "de.NBI tiny"     1 Core  , 2  GB Ram
        - "de.NBI mini"     4 Cores , 7  GB Ram
        - "de.NBI small"    8 Cores , 16 GB Ram
        - "de.NBI medium"  14 Cores , 32 GB Ram
        - "de.NBI large"   28 Cores , 64 GB Ram
        - "de.NBI large + ephemeral" 28 Cores , 64 GB Ram
    If ephemeral required, only ephemeral flavor possible.
    Check if a flavor is available, if not, test the next higher (option).
    :param cpu: required cpu value for job
    :param mem: required memory value for job (mb)
    :param tmp_disk: required ephemeral value (mb)
    :param flavors_data: flavor data (json) from cluster api
    :param available_check: test if flavor is available
    :param quiet: print messages
    :return: matched and available flavor
    """
    cpu = int(cpu)
    mem = int(mem)
    found_flavor = False
    if not flavors_data:
        logger.debug("flavors_data is empty")
        return None
    for fv_data in reversed(flavors_data):
        if fv_data["flavor"]["ephemeral_disk"] == 0 and config_mode["flavor_ephemeral"]:
            continue

        if (
            cpu <= int(fv_data["flavor"]["vcpus"])
            and mem <= int(fv_data["available_memory"])
            and (
                int(tmp_disk) < int(fv_data["flavor"]["temporary_disk"])
                or (
                    int(tmp_disk) == 0 and int(fv_data["flavor"]["temporary_disk"] >= 0)
                )
            )
        ):
            # job tmp disk must be lower than flavor tmp disk, exception if both zero
            # ex. flavor with 1TB tmp disk, jobs with 1TB are not scheduled, only jobs with 999M tmp disk
            found_flavor = True

            if not available_check:
                return fv_data
            if fv_data["usable_count"] > 0:
                logger.debug(
                    "-> match found %s for cpu %d mem %d",
                    fv_data["flavor"]["name"],
                    cpu,
                    mem,
                )
                return fv_data
            if not quiet:
                logger.info(
                    "flavor %s found, but not available - searching for cpu %s memory %s tmp_disk %s",
                    fv_data["flavor"]["name"],
                    cpu,
                    mem,
                    tmp_disk,
                )
            break
    if not quiet and not found_flavor:
        logger.warning(
            "unable to find a suitable flavor - searching for cpu %s mem %s tmp_disk %s",
            cpu,
            mem,
            tmp_disk,
        )

    return None


def __sort_jobs(jobs_pending_dict):
    """
    Sort jobs by resource metrics or priority.
    :param jobs_pending_dict: jobs as json dictionary object
    :return: sorted job list
    """
    if config_data["resource_sorting"]:
        job_priority = __sort_job_by_resources(jobs_pending_dict)
    else:
        job_priority = __sort_job_priority(jobs_pending_dict)
    pending_jobs_percent = config_data["pending_jobs_percent"]
    limit = int(pending_jobs_percent * len(job_priority))
    logger.debug(
        f"Using {pending_jobs_percent} percent of the jobs for calculating --> {limit} Jobs"
    )
    return job_priority[:limit]


def __sort_job_priority(jobs_dict):
    """
    Sort jobs by priority
        - priority should be calculated by scheduler
        - sort memory as secondary condition
        - sort cpu and ephemeral (priority may not consider tmp disk)
    :param jobs_dict: jobs as json dictionary object
    :return: job list sorted by priority, jobs with high priority at the top
    """

    # sort jobs by memory as secondary condition, first priority (scheduler)
    priority_job_list = sorted(
        jobs_dict.items(),
        key=lambda k: (
            k[1]["priority"],
            k[1]["req_mem"],
            k[1]["req_cpus"],
            k[1]["temporary_disk"],
        ),
        reverse=True,
    )
    return priority_job_list


def __sort_job_by_resources(jobs_dict):
    """
    Sort jobs by resources
        - memory
        - cpu
        - ephemeral

    :param jobs_dict: jobs as json dictionary object
    :return: job list sorted, jobs with high resources at the top
    """
    sorted_job_list = sorted(
        jobs_dict.items(),
        key=lambda k: (k[1]["req_mem"], k[1]["req_cpus"], k[1]["temporary_disk"]),
        reverse=True,
    )
    return sorted_job_list


def __receive_node_stats(node_dict, quiet):
    """
    Return stats from job data from scheduler and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    worker_in_use = []
    worker_count = 0
    worker_drain = []
    worker_drain_idle = []

    logger.debug("node dict: %s ", pformat(node_dict))
    node_filter(node_dict)
    if node_dict:
        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            logger.error("%s found, but dummy mode is not active", NODE_DUMMY)
            sys.exit(1)
        else:
            logger.error("%s not found, but dummy mode is active", NODE_DUMMY)
        for key, value in list(node_dict.items()):
            if "temporary_disk" in value:
                tmp_disk = value["temporary_disk"]
            else:
                tmp_disk = 0
            if not quiet:
                logger.info(
                    "key: %s - state: %s cpus %s real_memory %s tmp_disk %s",
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                    tmp_disk,
                )
                # logger.debug(pformat(key))
                # logger.debug(pformat(value))
            if key is not None and "worker" in key:
                worker_count += 1
                if NODE_DRAIN in value["state"]:
                    worker_drain.append(key)
                if (NODE_ALLOCATED in value["state"]) or (NODE_MIX in value["state"]):
                    worker_in_use.append(key)
                elif NODE_DRAIN in value["state"] and NODE_IDLE in value["state"]:
                    worker_drain_idle.append(key)
                elif (NODE_DOWN in value["state"]) and NODE_IDLE not in value["state"]:
                    logger.error("workers are in DOWN state")
            else:
                del node_dict[key]
        if not quiet:
            logger.info(
                "nodes: I found %d worker - %d allocated, %d drain, drain+idle %s",
                worker_count,
                len(worker_in_use),
                len(worker_drain),
                worker_drain_idle,
            )
    else:
        if not quiet:
            logger.info("no nodes found, possible broken configuration!")
    return node_dict, worker_count, worker_in_use, worker_drain, worker_drain_idle


def receive_node_data_live():
    """
    Query the node data from scheduler (no database call) and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    node_dict = receive_node_data_live_uncut()
    return __receive_node_stats(node_dict, False)


def receive_node_data_live_uncut():
    """
    Query the node data from scheduler (no database call).
    :return:  node dictionary
    """
    node_dict = scheduler_interface.node_data_live()
    if node_dict is None:
        node_dict = {}
    return node_dict


def receive_node_data_db(quiet):
    """
    Query the node data from scheduler and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: workers in use
        - worker_drain: workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    node_dict = scheduler_interface.fetch_scheduler_node_data()
    return __receive_node_stats(node_dict, quiet)


def node_filter(node_dict):
    ignore_workers = config_data["ignore_workers"]
    if ignore_workers:
        for key in node_dict.copy():
            if key in ignore_workers:
                del node_dict[key]
        logger.debug("ignore_nodes: %s, node_dict %s", ignore_workers, node_dict.keys())


def f(cluster_workers):
    ignore_workers = config_data["ignore_workers"]

    if ignore_workers:
        index = 0
        for key in cluster_workers:
            if key["hostname"] in ignore_workers:
                cluster_workers.pop(index)
                logger.debug("remove %s", key)
            index += 1
        logger.warning("ignore worker: %s", ignore_workers)


def receive_job_data():
    """
    Receive current job data.
    :return:
        - dictionary with pending jobs
        - dictionary with running jobs
    """

    jobs_pending_dict = {}
    jobs_running_dict = {}
    job_live_dict = scheduler_interface.job_data_live()
    if LOG_LEVEL == logging.DEBUG and not job_live_dict:
        logger.debug("try again, maybe logging problems")
        time.sleep(2)
        job_live_dict = scheduler_interface.job_data_live()
    if job_live_dict:
        for j_key, j_val in job_live_dict.items():
            if j_val["state"] == JOB_PENDING:
                jobs_pending_dict.update({j_key: j_val})
            elif j_val["state"] == JOB_RUNNING:
                jobs_running_dict.update({j_key: j_val})
    return jobs_pending_dict, jobs_running_dict


def receive_completed_job_data(days):
    """
    Return completed jobs from the last x days.
    :param days: number of days
    :return: jobs dictionary
    """
    jobs_dict = scheduler_interface.fetch_scheduler_job_data(days)
    if jobs_dict:
        for key, value in list(jobs_dict.items()):
            if value["state"] != JOB_FINISHED:
                del jobs_dict[key]
        jobs_dict = dict(
            sorted(jobs_dict.items(), key=lambda k: (k[1]["end"]), reverse=False)
        )
        for key, value in list(jobs_dict.items()):
            logger.debug(value["end"])
    if jobs_dict is None:
        jobs_dict = {}
    return jobs_dict


def print_job_history(job_history):
    if job_history:
        print_job_data(list(job_history.items()))


def print_job_data(job_data):
    if job_data:
        for key, value in list(job_data):
            logger.info(
                "id: %s - jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s - elapsed %s",
                key,
                value["jobname"],
                value["req_mem"],
                value["req_cpus"],
                value["priority"],
                value["temporary_disk"],
                value["elapsed"],
            )
    else:
        logger.info("No job found")


def get_cluster_data():
    """
    Receive worker information from portal.
    request example:
    :return:
        cluster data dictionary
        if api error: None
    """
    try:
        json_data = {
            "password": __get_cluster_password(),
            "scaling_type": SCALING_TYPE,
            "version": AUTOSCALING_VERSION,
        }
        response = requests.post(url=get_url_info_cluster(), json=json_data)
        # logger.debug("response code %s, send json_data %s", response.status_code, json_data)

        if response.status_code == HTTP_CODE_OK:
            res = response.json()
            version_check_scale_data(res["VERSION"])
            logger.debug(pformat(res))
            return res
        else:
            handle_code_unauthorized(res=response)

    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        logger.error("unable to receive cluster data")
        if res.status_code == HTTP_CODE_UNAUTHORIZED:
            handle_code_unauthorized(res=res)
        else:
            __sleep_on_server_error()
    except OSError as error:
        logger.error(error)
    except Exception as e:
        logger.error("error by accessing cluster data %s", e)
    return None


def worker_filter(cluster_workers):
    ignore_workers = config_data["ignore_workers"]

    if ignore_workers:
        index = 0
        for key in cluster_workers:
            if key["hostname"] in ignore_workers:
                cluster_workers.pop(index)
                logger.debug("remove %s", key)
            index += 1
        logger.warning("ignore worker: %s", ignore_workers)


def get_cluster_workers(cluster_data):
    """
    Modify cluster worker data.
    :param cluster_data:
    :return:
    """
    cluster_workers = None
    if cluster_data:
        cluster_workers = cluster_data.get("workers", [])
        logger.info(cluster_workers)

        for w_data in cluster_workers:
            logger.info(f"{w_data} ")
            flavor_data = w_data["flavor"]
            w_data.update(
                {
                    "memory_usable": reduce_flavor_memory(
                        convert_mib_to_gb(flavor_data["ram"])
                    )
                }
            )

            w_data.update({"temporary_disk": flavor_data["ephemeral"]})
        worker_filter(cluster_workers)
    return cluster_workers


def get_cluster_workers_from_api():
    return get_cluster_workers(get_cluster_data())


def __get_flavor_available_count(flavor_data, flavor_name):
    """
    Return the number of available workers with this flavor.
    :param flavor_data:
    :param flavor_name:
    :return: maximum of new possible workers with this flavor
    """
    flavor_tmp = __get_flavor_by_name(flavor_data, flavor_name)
    if flavor_tmp:
        if flavor_tmp["usable_count"]:
            return int(flavor_tmp["usable_count"])
    return 0


def __get_flavor_by_name(flavor_data, flavor_name):
    """
    Return flavor object by flavor name.
    :param flavor_data:
    :param flavor_name:
    :return: flavor data
    """
    for flavor_tmp in flavor_data:
        if flavor_tmp["flavor"]["name"] == flavor_name:
            return flavor_tmp
    return None


def convert_gb_to_mb(value):
    """
    Convert GB value to MB.
    :param value:
    :return:
    """
    return int(value) * 1000


def convert_gb_to_mib(value):
    """
    Convert GB value to MiB.
    :param value:
    :return:
    """
    return int(value) * 1024


def convert_mib_to_gb(value):
    """
    Convert GB value to MiB.
    :param value:
    :return:
    """
    return int(int(value) / 1024)


def convert_tb_to_mb(value):
    """
    Convert TB value to MB.
    :param value:
    :return:
    """
    return int(value * 1000000)


def convert_tb_to_mib(value):
    """
    Convert TB value to MiB.
    :param value:
    :return:
    """
    return int(value) * 1000 * 1024


def usable_flavor_data(flavor_data):
    """
    Calculate the number of usable flavors, based on configuration.
    :param flavor_data:
    :return: number of usable flavors
    """
    fv_cut = int(config_mode["flavor_restriction"])
    # modify flavor count by current version
    flavors_available = len(flavor_data)
    logger.debug("flavors_available %s fv_cut %s", flavors_available, fv_cut)
    if 0 < fv_cut < 1:
        fv_cut = __multiply(flavors_available, fv_cut)
    if 1 <= fv_cut < flavors_available:
        flavors_usable = fv_cut
    else:
        flavors_usable = flavors_available
        if fv_cut != 0:
            logger.error("wrong flavor cut value")
    logger.debug("found %s flavors %s usable ", flavors_available, flavors_usable)
    if flavors_usable == 0 and flavors_available > 0:
        flavors_usable = 1
    return flavors_usable


def flavor_mod_gpu(flavors_data):
    """
    Modify flavor data according to gpu option.
    :param flavors_data: flavor data
    :return: changed flavor data
    """
    flavors_data_mod = []
    removed_flavors = []
    flavor_gpu = int(config_mode["flavor_gpu"])
    # modify flavors by gpu
    for fv_data in flavors_data:
        # use only gpu flavors
        if flavor_gpu == FLAVOR_GPU_ONLY and fv_data["flavor"]["gpu"] == 0:
            removed_flavors.append(fv_data["flavor"]["name"])
        # remove all gpu flavors
        elif flavor_gpu == FLAVOR_GPU_REMOVE and fv_data["flavor"]["gpu"] != 0:
            removed_flavors.append(fv_data["flavor"]["name"])
        else:
            flavors_data_mod.append(fv_data)

    if removed_flavors:
        logger.debug("unlisted flavors by GPU config option: %s", removed_flavors)
    return flavors_data_mod


def get_usable_flavors(quiet, cut):
    """
    Receive flavor information from portal.
        - the largest flavor on top
        - memory in GB (real_memory/1024)
    :param quiet: boolean - print flavor data
    :param cut: boolean - cut flavor information according to configuration
    :return: available flavors as json
    """
    try:
        res = requests.post(
            url=get_url_info_flavors(),
            json={
                "password": __get_cluster_password(),
                "version": AUTOSCALING_VERSION,
                "scaling_type": SCALING_TYPE,
            },
        )

        if res.status_code == HTTP_CODE_OK:
            flavors_data = res.json()
            flavors_data = sorted(
                flavors_data,
                key=lambda k: (
                    k["flavor"].get("credits_costs_per_hour", 0),
                    k["flavor"]["ram_gib"],
                    k["flavor"]["vcpus"],
                    k["flavor"]["ephemeral_disk"],
                ),
                reverse=True,
            )
            # logger.debug("flavorData %s", pformat(flavors_data))

            counter = 0
            flavors_data = flavor_mod_gpu(flavors_data)
            flavors_data_mod = []
            if cut:
                flavors_usable = usable_flavor_data(flavors_data)
            else:
                flavors_usable = len(flavors_data)

            for fd in flavors_data:
                available_memory = reduce_flavor_memory(int(fd["flavor"]["ram_gib"]))

                if "ephemeral_disk" in fd["flavor"]:
                    fd["flavor"]["temporary_disk"] = convert_gb_to_mib(
                        fd["flavor"]["ephemeral_disk"]
                    )
                else:
                    fd["flavor"]["temporary_disk"] = 0
                val_tmp = []
                for fd_item in fd.items():
                    val_tmp.append(fd_item)
                val_tmp.append(("cnt", counter))
                val_tmp.append(("available_memory", available_memory))
                # self selected high memory limit
                if (
                    config_mode["limit_flavor_usage"]
                    and fd["flavor"]["type"]["shortcut"] == FLAVOR_HIGH_MEM
                    and fd["flavor"]["name"] in config_mode["limit_flavor_usage"]
                ):
                    user_fv_limit = int(
                        config_mode["limit_flavor_usage"][fd["flavor"]["name"]]
                    )
                    available_to_use = min(
                        user_fv_limit - fd["used_count"],
                        fd["real_available_count_openstack"],
                        fd["available_count"],
                    )
                else:
                    available_to_use = min(
                        fd["real_available_count_openstack"], fd["available_count"]
                    )
                val_tmp.append(("usable_count", available_to_use))
                # --------------
                if counter <= flavors_usable:
                    tmp_id = str(counter)
                    flavors_data_mod.append(dict(val_tmp))
                else:
                    tmp_id = "X"
                if not quiet:
                    logger.info(
                        "fv: %s, %s,"
                        " available: %sx"
                        " ram: %s GB,"
                        " cpu: %s,"
                        " ephemeral: %sGB,"
                        " credit: %s",
                        tmp_id,
                        fd["flavor"]["name"],
                        available_to_use,
                        fd["flavor"]["ram_gib"],
                        fd["flavor"]["vcpus"],
                        fd["flavor"]["ephemeral_disk"],
                        fd["flavor"].get("credits_costs_per_hour", 0),
                    )
                    logger.debug(
                        " project available %sx,"
                        " real available: %sx,"
                        " real memory: %s,"
                        " tmpDisk %s,"
                        " id %s"
                        " type: %s",
                        fd["available_count"],
                        fd["real_available_count_openstack"],
                        available_memory,
                        fd["flavor"]["temporary_disk"],
                        fd["flavor"]["id"],
                        fd["flavor"]["type"]["shortcut"],
                    )
                counter += 1
            return list(flavors_data_mod)

        if res.status_code == HTTP_CODE_UNAUTHORIZED:
            handle_code_unauthorized(res=res)

        else:
            logger.error(
                "server error - unable to receive flavor data, code %s", res.status_code
            )
            __csv_log_entry("E", 0, "15")

    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        __csv_log_entry("E", 0, "14")

        if e.response.status_code == HTTP_CODE_UNAUTHORIZED:
            handle_code_unauthorized(res=res)
        else:
            __sleep_on_server_error()
    except OSError as error:
        logger.error(error)
    except Exception as e:
        logger.error("error by accessing flavor data: %s %s", e, traceback.format_exc())
    return None


def __get_file(read_file):
    """
    Read json content from file and return it.
    :param read_file: file to read
    :return: file content as json
    """
    try:
        with open(read_file, "r", encoding="utf-8") as f:
            file_json = json.load(f)
        return file_json
    except IOError:
        logger.error("Error: file does not exist %s", read_file)
        return None
    except ValueError:
        logger.error("Decoding JSON data failed from file %s", read_file)
        return None


def __save_file(save_file, content):
    """
    Save json content to file.
    :param save_file: file to write
    :return: file content as json
    """
    try:
        with open(save_file, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
            f.flush()
    except IOError:
        logger.error("Error writing file %s", save_file)
        sys.exit(1)


def __get_cluster_password():
    """
    Read cluster password from CLUSTER_PASSWORD_FILE, the file should contain: {"password":"CLUSTER_PASSWORD"}

    :return: "CLUSTER_PASSWORD"
    """
    pw_json = __get_file(CLUSTER_PASSWORD_FILE)
    cluster_pw = pw_json.get("password", None)
    if not cluster_pw:
        logger.error(
            "No Cluster Password. Please set the password via:\n autoscaling -p \n and restart the service!"
        )
        sys.exit(1)
    logger.debug("pw_json: %s", cluster_pw)
    return cluster_pw


def __set_cluster_password():
    """
    Save cluster password to CLUSTER_PASSWORD_FILE, the password can be entered when prompted.
    :return:
    """
    password_ = input("enter cluster password (copy&paste): ")
    tmp_pw = {"password": password_}
    __save_file(CLUSTER_PASSWORD_FILE, tmp_pw)


def __worker_states():
    """
    Recall cluster data from server and return worker statistics.
    :return:
        - worker_active: list of workers with state active
        - worker_error: list of workers with state error
        - worker_down: list of workers with state down
        - worker_unknown: list of workers with state unknown
        - worker_error: list of error workers
        - worker_drain_list: list of idle with drain workers
        - cluster_workers: cluster data from portal
    """
    worker_active = []
    worker_down = []
    worker_unknown = []
    worker_error = []
    cluster_data = get_cluster_data()
    logger.info(cluster_data)
    cluster_workers = get_cluster_workers(cluster_data)

    if cluster_workers is not None:
        for c_worker in cluster_workers:
            logger.debug(
                "hostname: '%s' status: '%s'", c_worker["hostname"], c_worker["status"]
            )
            if NODE_DOWN in c_worker["status"]:
                worker_down.append(c_worker["hostname"])
            elif WORKER_ERROR == c_worker["status"].upper():
                logger.error("ERROR %s", c_worker["hostname"])
                worker_error.append(c_worker["hostname"])
            elif WORKER_CREATION_FAILED == c_worker["status"].upper():
                logger.error("CREATION FAILED %s", c_worker["hostname"])
                worker_error.append(c_worker["hostname"])
            elif WORKER_FAILED in c_worker["status"].upper():
                logger.error("FAILED workers, not recoverable %s", c_worker["hostname"])
                sys.exit(1)
            elif WORKER_ACTIVE == c_worker["status"].upper():
                worker_active.append(c_worker["hostname"])
            else:
                worker_unknown.append(c_worker["hostname"])
    return (
        worker_active,
        worker_unknown,
        worker_error,
        worker_down,
        cluster_workers,
        cluster_data,
    )


def check_workers(rescale, worker_count):
    """
    Fetch cluster data from server and check
        - all workers are active
        - remove broken workers
    :param rescale: rescale decision
    :param worker_count: expected number of workers
    :return: no_error, cluster_data
    """
    worker_ready = False
    start_time = time.time()
    max_time = 1200
    no_error = True
    max_wait_cnt = 0
    service_frequency = int(config_mode["service_frequency"])
    while not worker_ready:
        (
            worker_active,
            worker_unknown,
            worker_error,
            worker_down,
            cluster_workers,
            cluster_data,
        ) = __worker_states()

        if cluster_data is None:
            logger.error(
                "unable to receive cluster data ... try again in %s seconds",
                service_frequency * WAIT_CLUSTER_SCALING,
            )
            time.sleep(service_frequency * WAIT_CLUSTER_SCALING)
            continue
        cluster_count_live = len(cluster_workers)
        logger.info(
            "workers: active %s, error %s, down %s, not ready %s, error list: %s.",
            len(worker_active),
            len(worker_error),
            len(worker_down),
            len(worker_unknown),
            worker_error,
        )
        logger.debug("workers expected %s", worker_count)
        elapsed_time = time.time() - start_time
        if cluster_count_live < worker_count and WAIT_CLUSTER_SCALING > max_wait_cnt:
            logger.debug(
                "increase wait time for cloud api %s, wait until %s workers are listed, current %s",
                max_wait_cnt,
                worker_count,
                cluster_count_live,
            )
            time.sleep(WAIT_CLUSTER_SCALING / 2)
            max_wait_cnt += 1
        elif elapsed_time > max_time and worker_unknown:
            logger.error("workers are stuck: %s", worker_unknown)
            cluster_scale_down_specific_self_check(
                worker_unknown + worker_error, rescale
            )
            worker_count -= len(worker_unknown + worker_error)
            no_error = False
            __csv_log_entry("E", len(worker_unknown + worker_error), "11")
        elif worker_error and not worker_unknown:
            logger.error("scale down error workers: %s", worker_error)
            cluster_scale_down_specific_self_check(worker_error, rescale)
            worker_count -= len(worker_error)
            no_error = False
            __csv_log_entry("E", len(worker_error), "12")
        elif elapsed_time > max_time and worker_down:
            logger.error("scale down workers are down: %s", worker_down)
            cluster_scale_down_specific_self_check(worker_down, rescale)
            worker_count -= len(worker_down)
            no_error = False
            __csv_log_entry("E", len(worker_down), "17")
        elif not worker_unknown and not worker_error:
            logger.info("ALL WORKERS ACTIVE!")
            if cluster_count_live < worker_count:
                logger.warning("higher number of workers expected!")
            return no_error, cluster_data
        else:
            logger.info(
                "at least one worker is not 'ACTIVE', wait ... %d seconds",
                WAIT_CLUSTER_SCALING,
            )
            time.sleep(WAIT_CLUSTER_SCALING)


def __generate_downscale_list(worker_data, count, jobs_dict):
    """
    Scale down unused idle workers
            - set workers in drain state
                - it may help with overlaps if the scheduler tries to reassign the node
            - check if workers are still capable to process pending jobs
        :param worker_data: worker data as json object
        :param count: max number of workers to scale down
        :return: scale down list with workers by name
    """
    worker_remove = []
    worker_idle = []
    worker_cool_down = int(config_mode["worker_cool_down"])
    if count == 0:
        return worker_remove
    # sort workers by resources, memory 'real_memory' False low mem first - True high first
    # workaround - required, if the most capable worker should remain active
    worker_mem_sort = sorted(
        worker_data.items(), key=lambda k: (k[1]["real_memory"]), reverse=False
    )
    for key, value in worker_mem_sort:
        if "worker" in key and count > 0:
            # check for broken workers first
            if (
                (NODE_ALLOCATED not in value["state"])
                and (NODE_MIX not in value["state"])
                and (NODE_IDLE not in value["state"])
            ):
                logger.error(
                    "worker is in unknown state: key %s  value %s", key, value["state"]
                )
                worker_remove.append(key)
            elif (
                (NODE_ALLOCATED not in value["state"])
                and (NODE_MIX not in value["state"])
                and count > 0
            ):
                if NODE_DRAIN in value["state"] and NODE_IDLE in value["state"]:
                    worker_remove.append(key)
                    count -= 1
                elif NODE_IDLE in value["state"]:
                    worker_idle.append(key)
                    count -= 1
                else:
                    logger.debug("unable to include %s %s", key, value["state"])

    logger.debug("worker_idle %s", worker_idle)
    logger.debug("worker_remove %s", worker_remove)

    # remove recent used idle (not drain) workers from list
    if worker_cool_down > 0 and worker_idle:
        worker_idle = __scale_frequency_worker_check(worker_idle)
        logger.debug("frequency worker check %s", worker_idle)
    if worker_idle and jobs_dict:
        # test if idle workers are capable for pending jobs
        _, _, _, worker_useless = __current_workers_capable(
            jobs_dict.items(), worker_data, worker_idle
        )
        worker_idle = [x for x in worker_idle if x in worker_useless]
        logger.debug("worker_idle not capable %s", worker_idle)
    worker_remove = worker_remove + worker_idle
    logger.debug("worker_remove merged: %s", worker_remove)

    if worker_remove:
        [scheduler_interface.set_node_to_drain(key) for key in worker_remove]
    else:
        logger.debug("unable to generate downscale list, worker may still be required")
    return worker_remove


def __drain_worker_check(
    worker_high,
    w_value,
    w_fv,
    flavor_data,
    jobs_history_dict_rev,
    jobs_running_dict,
    cluster_worker,
):
    """
    Check the elapsed time since the last worker usage with the required flavor according to job history.
    Delay drain active workers with higher flavors as required.
    :param worker_high: worker to check
    :param w_value: worker dictionary
    :param flavor_data: flavor data
    :return: worker list
    """

    wait_time = int(config_mode["drain_delay"])  # config_mode['drain_frequency']
    if wait_time > 0:
        if not jobs_history_dict_rev:
            jobs_history_dict_rev = sorted(
                list(receive_completed_job_data(1).items()),
                key=lambda k: (k[1]["end"], k[1]["req_mem"]),
                reverse=True,
            )
        time_now = __get_time()
        if not jobs_history_dict_rev:
            return
        for _, value in jobs_running_dict.items():
            if "nodes" in value and worker_high in value["nodes"]:
                fv_tmp = translate_metrics_to_flavor(
                    value["req_cpus"],
                    value["req_mem"],
                    value["temporary_disk"],
                    flavor_data,
                    False,
                    False,
                )
                if not __compare_worker_high_vs_flavor(fv_tmp, w_value):
                    logger.debug(
                        "worker still in use with required flavor:  %s time_now %s, key %s",
                        wait_time,
                        time_now,
                        worker_high,
                    )
                    return False
        logger.debug("worker %s not in use with suitable job", worker_high)
        for _, value in jobs_history_dict_rev:
            elapsed_time = float(time_now) - float(value["end"])
            if "nodes" in value and worker_high in value["nodes"]:
                if elapsed_time < wait_time:
                    fv_tmp = translate_metrics_to_flavor(
                        value["req_cpus"],
                        value["req_mem"],
                        value["temporary_disk"],
                        flavor_data,
                        False,
                        False,
                    )
                    if not __compare_worker_high_vs_flavor(fv_tmp, w_value):
                        logger.debug(
                            "block drain, last high flavor usage was: %s wait_time %s time_now %s job_end %s, key %s",
                            elapsed_time,
                            wait_time,
                            time_now,
                            value["end"],
                            worker_high,
                        )
                        return False
                logger.debug(
                    "worker %s elapsed %s name %s, nodes %s",
                    worker_high,
                    elapsed_time,
                    value["jobname"],
                    value["nodes"],
                )
                break
    if wait_time < 0 and w_fv:
        # negative number, skip drain for worker
        workers_with_flavor = []
        worker_json, _, _, worker_drain_list, _ = receive_node_data_live()

        for w_key, _ in worker_json.items():
            w_cd_tmp = worker_to_cluster_data(w_key, cluster_worker)
            if w_cd_tmp and w_cd_tmp["flavor"] == w_fv["flavor"]["name"]:
                workers_with_flavor.append(w_key)
                logger.debug(
                    "active worker %s with flavor %s", w_key, w_cd_tmp["flavor"]
                )
        workers_with_flavor_drain = [
            x for x in workers_with_flavor if x in worker_drain_list
        ]
        workers_remaining = __division_round(
            len(workers_with_flavor) - abs(wait_time), 2
        ) - (len(workers_with_flavor_drain))
        logger.debug(
            "workers_remaining: %s, workers_with_flavor_drain: %s, worker_drain_list %s",
            workers_remaining,
            workers_with_flavor_drain,
            worker_drain_list,
        )
        if workers_remaining <= 0:
            logger.debug(
                "skip drain for worker: %s, workers_with_flavor %s: %s, worker_drain_list %s",
                worker_high,
                len(workers_with_flavor),
                workers_with_flavor,
                worker_drain_list,
            )
            return False
    return True


def __scale_frequency_worker_check(worker_useless):
    """
    Check the elapsed time since the last worker usage according to job history.
    Remove workers from the list if the elapsed time since last usage is lower than the 'worker_cool_down'.
    :param worker_useless: worker list
    :return: worker list
    """
    jobs_history_dict_rev = sorted(
        list(receive_completed_job_data(1).items()),
        key=lambda k: (k[1]["end"], k[1]["req_mem"]),
        reverse=True,
    )
    time_now = __get_time()
    wait_time = int(config_mode["worker_cool_down"])
    worker_prevent_frequency = []
    for i in worker_useless:
        for _, value in jobs_history_dict_rev:
            elapsed_time = float(time_now) - float(value["end"])
            if "nodes" in value and i in value["nodes"]:
                if elapsed_time < wait_time:
                    worker_prevent_frequency.append(i)
                break
    logger.debug("prevent %s", worker_prevent_frequency)
    result = [item for item in worker_useless if item not in worker_prevent_frequency]
    logger.debug("result %s", result)
    return result


def __calculate_scale_down_value(worker_count, worker_free, state):
    """
    Retain number of free workers to delete and respect boundaries like DOWNSCALE_LIMIT and SCALE_FORCE.
    :param worker_count: number of total workers
    :param worker_free: number of idle workers
    :return: number of workers to scale down
    """

    if worker_count > DOWNSCALE_LIMIT:
        if worker_free < worker_count and not state == ScaleState.DOWN_UP:
            return round(worker_free * config_mode["scale_force"], 0)

        max_scale_down = worker_free - DOWNSCALE_LIMIT
        if max_scale_down > 0:
            return max_scale_down
    return 0


def __worker_match_to_job(j_value, w_value):
    """
    Test if a worker matches to a job.
    :return: boolean, job data match to worker data
    """
    found_match = False

    try:
        w_mem_tmp = int(w_value["real_memory"])
        w_cpu = int(w_value["total_cpus"])
        w_tmp_disk = int(w_value["temporary_disk"] or 0)

        j_cpu = int(j_value["req_cpus"])
        j_mem = int(j_value["req_mem"])
        j_tmp_disk = int(j_value["temporary_disk"])
        if j_mem <= w_mem_tmp and j_cpu <= w_cpu:
            if j_tmp_disk < w_tmp_disk or j_tmp_disk == 0:
                found_match = True
    except ValueError:
        logger.error("value was not an integer %s w_value %s", j_value, w_value)

    return found_match


def __current_workers_capable(jobs_pending_dict, worker_json, worker_to_check):
    """
    Check if workers are capable for next job in queue (resource based).
    :param jobs_pending_dict: job dictionary with pending jobs
    :param worker_json: worker node data as json dictionary
    :param worker_to_check: check only a limited list of workers, all workers dict with "None"
    :return:
        - boolean: if workers are capable
        - worker_all: all checked workers as list
        - worker_useful: list with useful workers
        - worker_useless: list with useless workers
    """

    found_pending_jobs = False
    worker_not_capable_counter = 0
    if not worker_to_check:
        worker_all = list(worker_json.keys())
    else:
        worker_all = worker_to_check
        logger.debug("limit worker_to_check %s", worker_to_check)
    logger.debug("worker_all %s", worker_all)
    worker_useless = worker_all.copy()
    for _, j_value in jobs_pending_dict:
        if j_value["state"] == JOB_PENDING:
            found_pending_jobs = True
            found_match = False
            for w_key, w_value in worker_json.items():
                if w_key not in worker_all:
                    logger.debug("skip %s", w_key)
                    continue
                if "worker" not in w_key:
                    continue
                if __worker_match_to_job(j_value, w_value):
                    current_time = int(time.time())
                    last_busy_time = w_value["last_busy_time"]
                    if not last_busy_time:
                        last_busy_longer_than_one_hour = False
                    else:
                        time_difference = current_time - last_busy_time

                        last_busy_longer_than_one_hour = time_difference < 3600
                    if w_key in worker_useless and last_busy_longer_than_one_hour:
                        worker_useless.remove(w_key)
                    else:
                        logger.debug("last_busy_time longer than 1 hour")
                    found_match = True
            if not found_match:
                worker_not_capable_counter += 1
    worker_useful = [x for x in worker_all if x not in worker_useless]
    logger.debug("worker_all %s", worker_all)
    logger.debug("worker_useful %s", worker_useful)
    logger.debug("worker_useless %s", worker_useless)

    if found_pending_jobs and worker_not_capable_counter > 0:
        return False, worker_all, worker_useful, worker_useless
    return True, worker_all, worker_useful, worker_useless


def classify_jobs_to_flavors(job_priority, flavor_data):
    """
    Return the expected number of workers with the same flavor.
    Sort pending jobs by required flavor and add them to sub-lists.
    The depth setting specifies how far forward to look (in flavors).

    :param job_priority:
    :param flavor_data:
    :return: returns a collection of job lists divided into flavors.
        one job list contains:
        - number of pending jobs with flavor,
        - next flavor,
        - job list matching the flavor
    """

    flavor_depth = []
    flavor_max_depth = int(config_mode["flavor_depth"])
    depth_limit = len(flavor_data)
    if flavor_max_depth > 0 and flavor_max_depth > depth_limit:
        flavor_max_depth = depth_limit
    if flavor_max_depth in (DEPTH_MULTI, DEPTH_MULTI_SINGLE):
        depth_limit = len(flavor_data)
        logger.debug("flavor_depth is set to all %s flavors", depth_limit)
    elif flavor_max_depth == DEPTH_MAX_WORKER or config_mode["flavor_default"]:
        logger.debug("flavor_depth is set to single flavor")
        flavor_depth.append(__worker_no_flavor_separation(job_priority, flavor_data))
        return flavor_depth
    else:
        depth_limit = flavor_max_depth

    flavor_job_list = []
    flavor_next = None
    flavor_cnt = 0
    counter = 0
    available_check = True
    missing_flavors = False

    if LOG_LEVEL == logging.DEBUG:
        available_check = False

    for key, value in job_priority:
        if value["state"] == JOB_PENDING:
            flavor_tmp = translate_metrics_to_flavor(
                value["req_cpus"],
                value["req_mem"],
                value["temporary_disk"],
                flavor_data,
                available_check,
                False,
            )

            if flavor_tmp is None:
                logger.debug(
                    "unavailable flavor for job %s cpu %s mem %s disk %s",
                    value["jobname"],
                    value["req_cpus"],
                    value["req_mem"],
                    value["temporary_disk"],
                )
                missing_flavors = True
                # jump to the next job with available flavors
            elif flavor_next is None:
                flavor_next = flavor_tmp
                logger.debug(
                    "flavor_tmp_current is None: %s counter: %s", flavor_tmp, counter
                )
                counter += 1
                flavor_job_list.append((key, value))
            elif flavor_next["flavor"]["name"] == flavor_tmp["flavor"]["name"]:
                counter += 1
                flavor_job_list.append((key, value))
            elif (
                flavor_next["flavor"]["name"] != flavor_tmp["flavor"]["name"]
                and flavor_cnt < depth_limit
            ):
                flavor_cnt += 1
                if counter > 0 and flavor_next:
                    flavor_depth.append((counter, flavor_next, flavor_job_list))
                logger.debug(
                    "switch to next flavor, previous flavor %s",
                    flavor_next["flavor"]["name"],
                )
                counter = 1
                flavor_next = flavor_tmp
                flavor_job_list = [(key, value)]
            else:
                logger.debug("reached depth limit")
                break
    if flavor_job_list and flavor_next:
        flavor_depth.append((counter, flavor_next, flavor_job_list))
    if missing_flavors:
        logger.info("insufficient resources")
        __csv_log_entry("L", None, 0)

    if LOG_LEVEL == logging.DEBUG:
        # print result snapshot for debug
        count_ = 0
        for counter, flavor, j_list in flavor_depth:
            logger.debug("\n---------------------------\n")
            if len(j_list) > 0:
                logger.debug(
                    "flavor_tmp_current %s counter: %s priority first: %s last: %s",
                    flavor["flavor"]["name"],
                    counter,
                    pformat(j_list[0][1]["priority"]),
                    pformat(j_list[-1][1]["priority"]),
                )
                for key, value in j_list:
                    logger.debug(
                        "JOB level %s: jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s",
                        count_,
                        value["jobname"],
                        value["req_mem"],
                        value["req_cpus"],
                        value["priority"],
                        value["temporary_disk"],
                    )
                logger.debug("\n---------------------------\n")
            count_ += 1
    return flavor_depth


def __worker_no_flavor_separation(job_priority, flavor_data):
    """
    Return job list with the maximum required flavor.
    :param job_priority:
    :param flavor_data:
    :return: job list with flavor with
        - next flavor,
        - job list matching the flavor
    """

    job_list = []
    flavor_next = None
    for key, value in job_priority:
        if value["state"] == JOB_PENDING:
            flavor_tmp = translate_metrics_to_flavor(
                value["req_cpus"],
                value["req_mem"],
                value["temporary_disk"],
                flavor_data,
                True,
                False,
            )

            if flavor_tmp is None:
                logger.debug(
                    "unavailable flavor for job %s cpu %s mem %s disk %s",
                    value["jobname"],
                    value["req_cpus"],
                    value["req_mem"],
                    value["temporary_disk"],
                )
                continue
            if flavor_next is None:
                flavor_next = flavor_tmp
            elif flavor_next["flavor"]["name"] != flavor_tmp["flavor"]["name"]:
                logger.debug(
                    "flavor_next %s %s, flavor_tmp %s %s",
                    flavor_next["flavor"]["name"],
                    flavor_next["cnt"],
                    flavor_tmp["flavor"]["name"],
                    flavor_tmp["cnt"],
                )
                if flavor_tmp["cnt"] < flavor_next["cnt"]:
                    flavor_next = flavor_tmp
            job_list.append((key, value))
    return len(job_list), flavor_next, job_list


def __compare_flavor_max(fv_max, fv_min):
    """
    Compare flavor vs flavor values and, test if the given tmp flavor is higher or equal than the minimal flavor.
    :param fv_max: maximal flavor as json object
    :param fv_min: minimal flavor as json object
    :return: boolean, fv_max fits to minimum requirement
        - if fv_max is None, return False
        - if fv_min is None, return True
    """
    if not fv_max:
        return False
    if not fv_min and fv_max:
        return True

    if (
        fv_max["flavor"]["ram_gib"] >= fv_min["flavor"]["ram_gib"]
        and fv_max["flavor"]["vcpus"] >= fv_min["flavor"]["vcpus"]
    ):
        if config_mode["flavor_ephemeral"]:
            return True
        if fv_max["flavor"]["ephemeral_disk"] >= fv_min["flavor"]["ephemeral_disk"]:
            return True
    return False


def __compare_worker_high_vs_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, test if the worker is too large compared to given flavor.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp["available_memory"])
    w_mem_tmp = int(w_value["real_memory"])

    if fv_mem < w_mem_tmp and fv_tmp["flavor"]["vcpus"] <= w_value["total_cpus"]:
        if config_mode["flavor_ephemeral"]:
            return True
        if int(fv_tmp["flavor"]["temporary_disk"]) <= int(
            w_value["temporary_disk"] or 0
        ):
            return True
    return False


def __compare_worker_meet_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, if the worker meets the minimum flavor data.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp["available_memory"])
    w_mem_tmp = int(w_value["real_memory"])

    if fv_mem <= w_mem_tmp and fv_tmp["flavor"]["vcpus"] <= w_value["total_cpus"]:
        if config_mode["flavor_ephemeral"]:
            return True
        if int(fv_tmp["flavor"]["temporary_disk"]) <= int(w_value["temporary_disk"]):
            return True
    return False


def __compare_worker_match_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, if the worker match to flavor data with exact memory match.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp["available_memory"])
    w_mem_tmp = int(w_value["real_memory"])
    if (fv_mem == w_mem_tmp) and int(fv_tmp["flavor"]["vcpus"]) >= int(
        w_value["total_cpus"]
    ):
        if config_mode["flavor_ephemeral"]:
            return True
        if int(fv_tmp["flavor"]["temporary_disk"]) >= int(w_value["temporary_disk"]):
            return True
    return False


def set_nodes_to_drain(
    jobs_pending_dict,
    worker_json,
    flavor_data,
    cluster_worker,
    jobs_running_dict,
):
    """
    Calculate the largest flavor/worker for pending jobs.
    Check the workers:
        - if they are over the required resources, set them to drain state
        - if they have required resources and are in drain state, remove drain state
        - tmp disk value must be available on worker and flavor side
    :param jobs_pending_dict: job dictionary with pending jobs
    :param worker_json: worker node data as json dictionary
    :param flavor_data: current flavor data
    :param cluster_worker: worker data from cluster api
    :param jobs_running_dict: job dictionary with running jobs
    :return: changed worker data
    """
    worker_data_changed = False
    missing_flavors = False
    workers_drain = []
    fv_max = None
    if not flavor_data:
        return worker_data_changed

    nodes_resume = 0
    fv_min = None
    jobs_history_dict_rev = None
    # maximum required flavor for pending jobs
    for _, value in jobs_pending_dict.items():

        if value["state"] == JOB_PENDING:
            logger.debug(
                "set_nodes_to_drain: job id %s, cpu %s, mem %s, disk %s, state %s",
                value["jobname"],
                value["req_cpus"],
                value["req_mem"],
                value["temporary_disk"],
                value["state"],
            )
            fv_tmp = translate_metrics_to_flavor(
                value["req_cpus"],
                value["req_mem"],
                value["temporary_disk"],
                flavor_data,
                False,
                False,
            )
            if fv_tmp:
                if __compare_flavor_max(fv_tmp, fv_max):
                    fv_max = fv_tmp
                if not fv_min:
                    fv_min = fv_tmp
                elif __compare_flavor_max(fv_min, fv_tmp):
                    fv_min = fv_tmp
            else:
                logger.debug(
                    "no suitable flavor available for job %s - maybe after downscale",
                    int(value["req_mem"]),
                )
                missing_flavors = True

    logger.debug(
        "set_nodes_to_drain max: %s, ram %s, cpu %s, disk %s",
        fv_max["flavor"]["name"],
        fv_max["flavor"]["ram_gib"],
        fv_max["flavor"]["vcpus"],
        fv_max["flavor"]["temporary_disk"],
    )
    if not fv_max:
        return worker_data_changed
    for w_key, w_value in worker_json.items():
        if "worker" in w_key:
            w_cd = worker_to_cluster_data(w_key, cluster_worker)
            w_fv = cluster_worker_to_flavor(flavor_data, w_cd)
            if config_mode["drain_only_hmf"]:
                if w_fv:
                    if w_fv["flavor"]["type"]["shortcut"] != FLAVOR_HIGH_MEM:
                        logger.debug(
                            "no drain for %s flavor %s, worker %s, continue ... drain only %s",
                            w_fv["flavor"]["type"]["shortcut"],
                            w_fv["flavor"]["name"],
                            w_key,
                            FLAVOR_HIGH_MEM,
                        )
                        continue
            logger.debug(
                "worker disk %s, flavor disk %s",
                int(w_value["temporary_disk"]),
                int(fv_max["flavor"]["temporary_disk"]),
            )
            logger.debug(
                "w_cpus %s, cpus %s",
                int(w_value["total_cpus"]),
                int(fv_max["flavor"]["vcpus"]),
            )
            if missing_flavors and (NODE_DRAIN in w_value["state"]):
                logger.debug("missing_flavors: resume %s - keep worker active?", w_key)
                # scheduler_interface.set_nodfe_to_resume(w_key)
            # worker resources are over required resources
            elif (
                __compare_worker_high_vs_flavor(fv_max, w_value)
                and not missing_flavors
                and (NODE_DRAIN not in w_value["state"])
            ):
                logger.debug("large worker here: set to drain %s", w_key)
                # drain delay
                if __drain_worker_check(
                    w_key,
                    w_value,
                    w_fv,
                    flavor_data,
                    jobs_history_dict_rev,
                    jobs_running_dict,
                    cluster_worker,
                ):
                    scheduler_interface.set_node_to_drain(w_key)
                    worker_data_changed = True
            # if worker in drain and jobs are in queue which require this worker flavor, undrain
            # first test if drained worker are capable
            elif (NODE_DRAIN in w_value["state"]) and __compare_worker_meet_flavor(
                fv_max, w_value
            ):
                logger.debug(
                    "high worker: %s in drain, fit for max required flavor", w_key
                )
                # undrain if worker with flavor still required for upcoming jobs in queue
                worker_data_changed = True
                if __compare_worker_match_flavor(fv_max, w_value):
                    logger.debug("high worker still required: undrain %s", w_key)
                    scheduler_interface.set_node_to_resume(w_key)
                    nodes_resume += 1
                else:
                    logger.debug("high worker: %s in drain, too large", w_key)
                    if (
                        NODE_IDLE in w_value["state"]
                    ):  # if worker already idle, add for removal
                        workers_drain.append(w_value["node_hostname"])
            elif (NODE_DRAIN in w_value["state"]) and (NODE_IDLE in w_value["state"]):
                logger.debug("worker %s is drain and idle - scale down", w_key)
                workers_drain.append(w_value["node_hostname"])
                worker_data_changed = True

    if nodes_resume > 0:
        __csv_log_entry("X", nodes_resume, "1")
    if workers_drain:
        cluster_scale_down_specific_self_check(workers_drain, Rescale.INIT)
        __csv_log_entry("Y", len(workers_drain), "0")
    return worker_data_changed


def generate_hash(file_path):
    """
    Generate a sha256 hash from a file.
    :param file_path: file to process
    :return: hash value
    """
    if os.path.isfile(file_path):
        hash_v = hashlib.sha256()
        with open(file_path, "rb") as file:
            while True:
                chunk = file.read(hash_v.block_size)
                if not chunk:
                    break
                hash_v.update(chunk)

        logger.debug("%s with hash %s", file_path, hash_v.hexdigest())
        return hash_v.hexdigest()
    return None


def check_config():
    """
    Print message to log if configuration file changed since program start.
    :return: sha256 hash from current configuration
    """
    new_hash = generate_hash(FILE_CONFIG_YAML)
    if new_hash != config_hash:
        logger.warning(
            "configuration file changed, to apply the changes a manual restart is required \n"
            "current hash: %s \n"
            "started hash: %s ",
            new_hash,
            config_hash,
        )
    return new_hash


def convert_to_large_flavor(job_priority, flavors_data, pending_cnt, flavor_min):
    """
    Calculate large flavor for next jobs in squeue.
        - jobs must be sorted by priority
        - only working with scheduler mode high priority to high resources
        - consider how many jobs with the same minimal flavor can run on average on this worker
    :param job_priority: jobs, sorted by resources, high resources first
    :param flavors_data: usable flavors
    :param pending_cnt: number of jobs with flavor
    :param flavor_min:
    :return: calculated flavor
        - flavor_max: calculated flavor
        - counter: average jobs per flavor
        - sum_cpu: average cpu usage
        - sum_mem: average memory usage
        - sum_disk: average tmp disk usage
    """

    sum_cpu = 0
    sum_mem = 0
    sum_disk = 0

    flavor_max = None
    counter = 0
    counter_index = pending_cnt
    for _, value in job_priority:
        if value["state"] == JOB_PENDING and counter_index > 0:
            tmp_cpu = int(value["req_cpus"])
            tmp_mem = int(value["req_mem"])
            tmp_disk = int(value["temporary_disk"])
            counter += 1
            sum_cpu += tmp_cpu
            sum_mem += tmp_mem
            sum_disk += tmp_disk

    sum_cpu = __division_round(sum_cpu, counter)
    sum_mem = __division_round(sum_mem, counter)
    sum_disk = __division_round(sum_disk, counter)
    counter_index = pending_cnt
    counter = 0

    tmp_cpu = 0
    tmp_mem = 0
    tmp_disk = 0
    while counter_index > 0:
        tmp_cpu += sum_cpu
        tmp_mem += sum_mem
        tmp_disk += sum_disk
        tmp_fv = translate_metrics_to_flavor(
            tmp_cpu, tmp_mem, tmp_disk, flavors_data, True, True
        )
        if tmp_fv is None:
            break
        if (
            config_mode["large_flavors_except_hmf"]
            and tmp_fv["flavor"]["type"]["shortcut"] == FLAVOR_HIGH_MEM
        ):
            logger.debug("skip, except hmf is active")
            break
        counter += 1
        flavor_max = tmp_fv
        counter_index -= 1
    if flavor_max:
        logger.debug(
            "convert_to_large_flavor: max %s jobs with flavor %s",
            counter,
            flavor_max["flavor"]["name"],
        )
        logger.debug(
            "high flavor found max %s - min %s",
            flavor_max["flavor"]["name"],
            flavor_min["flavor"]["name"],
        )

        # test if flavor meet the specified minimal flavor
        if __compare_flavor_max(flavor_max, flavor_min):
            return flavor_max, counter, sum_cpu, sum_mem, sum_disk
    return None, counter, sum_cpu, sum_mem, sum_disk


def __get_worker_memory_usage(worker_json):
    """
    Calculate memory usage by all running workers.
    :param worker_json: worker node data as json dictionary
    :return: memory usage
    """
    w_memory_sum = 0
    for _, w_value in worker_json.items():
        w_memory_sum += int(w_value["real_memory"])
    return w_memory_sum


def __generate_upscale_limit(current_force, jobs_pending_flavor, worker_count, level):
    """
    Generate an up-scale limit from current force, based on current workers, job time and pending jobs on flavor.
    :param current_force:
    :param jobs_pending_flavor: number of pending jobs with current flavor
    :param worker_count: number of running workers
    :param level: current level, flavor depth
    :return: up-scale limit
    """
    upscale_limit = 0
    worker_weight = float(config_mode["worker_weight"])
    if jobs_pending_flavor == 0:
        return upscale_limit
    # reduce starting new workers based on the number of currently existing workers
    if worker_weight != 0:
        force = current_force - (worker_weight * worker_count)
        logger.debug(
            "WORKER_WEIGHT: %s - worker_useful %s - force %s",
            worker_weight,
            worker_count,
            force,
        )
        if force < FORCE_LOW:  # prevent too low scale force
            logger.debug("scale force too low: %s - reset", force)
            force = FORCE_LOW
    else:
        force = current_force

    upscale_limit = int(jobs_pending_flavor * force)

    logger.info("calculated  up-scale limit: %d, level %s", upscale_limit, level)
    return upscale_limit


def __division_round(num, div):
    """
    Calculate a division, if divided by zero, return zero.
    Round up/down decimal numbers.
    :param num: number
    :param div: divisor
    :return: result as integer
    """
    try:
        return int((float(num) / float(div)) + 0.5)
    except ZeroDivisionError:
        return 0


def __division_round_up(num, div):
    """
    Calculate a division, if divided by zero, return zero.
    Round up/down decimal numbers.
    :param num: number
    :param div: divisor
    :return: result as integer
    """
    try:
        return math.ceil((float(num) / float(div)))
    except ZeroDivisionError:
        return 0


def __division_round_down(num, div):
    """
    Calculate a division, if divided by zero, return zero.
    :param num: number
    :param div: divisor
    :return: result as integer
    """
    try:
        return int(num / div)
    except ZeroDivisionError:
        return 0


def __division_float(num, div):
    """
    Calculate a division, if divided by zero, return zero.
    :param num: number
    :param div: divisor
    :return: result as float
    """
    try:
        return float(num / div)
    except ZeroDivisionError:
        return 0


def __multiply(x_val, y_val):
    """
    Multiply and round the output value
    :param x_val: number x
    :param y_val: number y
    :return: result
    """

    return round((float(x_val) * float(y_val)) + 0.5)


def __multiple_jobs_per_flavor(
    flavor_tmp,
    average_job_resources_cpu,
    average_job_resources_memory,
    average_job_resources_tmp_disk,
):
    """
    Test if the current flavor can process multiple jobs.
    Check how many pending jobs can run on average on this flavor by cpu, memory and tmp disk.

    :param flavor_tmp: current flavor data
    :param average_job_resources_cpu: average cpu requirement
    :param average_job_resources_memory: average memory requirement
    :param average_job_resources_tmp_disk: average tmp disk requirement
    :return: integer value, average jobs per flavor
    """
    jobs_average_cpu = __division_float(
        flavor_tmp["flavor"]["vcpus"], average_job_resources_cpu
    )
    jobs_average_memory = __division_float(
        flavor_tmp["available_memory"], average_job_resources_memory
    )
    logger.debug(
        "fv_cpu %s, av_cpu %s, fv_mem %s, av_mem %s",
        flavor_tmp["flavor"]["vcpus"],
        average_job_resources_cpu,
        flavor_tmp["available_memory"],
        average_job_resources_memory,
    )
    logger.debug(
        "jobs_average_cpu %s, jobs_average_memory %s",
        jobs_average_cpu,
        jobs_average_memory,
    )
    if (
        int(flavor_tmp["flavor"]["temporary_disk"]) != 0
        and average_job_resources_tmp_disk != 0
    ):
        logger.debug(
            "flavor_next['flavor']['tmp_disk'] %s",
            flavor_tmp["flavor"]["temporary_disk"],
        )
        jobs_average_tmp_disk = __division_float(
            flavor_tmp["flavor"]["temporary_disk"], average_job_resources_tmp_disk
        )
        # The lowest value over all types of resources is the maximum possible value
        average_jobs_per_flavor = min(
            jobs_average_cpu, jobs_average_memory, jobs_average_tmp_disk
        )
    else:
        logger.debug("check average job resources, without disk")
        average_jobs_per_flavor = min(jobs_average_cpu, jobs_average_memory)
    if 0.3 < average_jobs_per_flavor % 1 < 0.7:
        return average_jobs_per_flavor
    return int(average_jobs_per_flavor + 0.5)


def __read_job_time(job_data_dict):
    """
    Read range value from job data (dictionary).
    :return:
    """
    try:
        return float(job_data_dict["job_norm"])
    except IndexError:
        logger.error("IndexError: job_data_dict")
        return None


def __current_job_lifetime(
    jobs_running_dict,
    worker_useful,
    average_job_resources_cpu,
    average_job_resources_memory,
    average_job_resources_tmp_disk,
    dict_db,
    flavor_next,
    worker_json,
    cluster_worker,
    average_job_time_norm,
    worker_claimed,
    job_time_sum,
    jobs_pending_flavor,
):
    """
    Check the probable remaining time of the current jobs to expect free resources.
    If the remaining normalized time of a currently running job below the specification border,
    the resources can probably be used again soon. Test how many resources are released by the running jobs.
    Merge resources of a worker that will be released shortly and compare it with the required job resources.
    :param jobs_running_dict: current running jobs
    :param worker_useful: capable and usable workers
    :param average_job_resources_cpu: average cpu requirement
    :param average_job_resources_memory: average memory requirement
    :param average_job_resources_tmp_disk: average tmp disk requirement
    :param average_job_time_norm: average normalized job time
    :param dict_db: job history data as dictionary
    :param flavor_next: selected scale-up favor
    :param worker_json: worker data from scheduler
    :param cluster_worker: worker data from cluster api
    :param worker_claimed: claimed active workers for pending jobs
    :param job_time_sum: merged job times from known jobs
    :param jobs_pending_flavor: number of pending jobs with time data
    :return: expected number of jobs which can be scheduled shortly on active workers
    """
    norm_workers_free = 0
    jobs_workers_can_process = 0
    worker_useful_tmp = []
    worker_useful_reclaim = []
    worker_claimed_keys = worker_claimed.keys()
    for key in worker_useful:
        if key in worker_claimed_keys:
            # already claimed and useful
            worker_useful_reclaim.append(key)
        else:
            # unclaimed and useful
            worker_useful_tmp.append(key)
    logger.debug(
        "worker_useful_tmp %s, worker_useful_reclaim %s",
        worker_useful_tmp,
        worker_useful_reclaim,
    )
    match_val = float(config_mode["job_time_threshold"])
    if dict_db and cluster_worker:
        logger.debug(
            "worker_useful: %s, worker_claimed: %s, usable %s, av job norm %s",
            worker_useful,
            worker_claimed,
            worker_useful_tmp,
            average_job_time_norm,
        )

        # check unclaimed workers
        for worker in worker_useful_tmp:
            worker_data = worker_json.get(worker)
            if not worker_data:
                logger.debug("missing worker_values %s ", worker_data)
                continue
            if NODE_DRAIN in worker_data["state"]:
                continue
            logger.debug("check jobs on worker %s", worker)
            logger.debug("worker_values %s ", worker_data)
            w_free_cpu = worker_data["total_cpus"]
            w_free_mem = int(worker_data["real_memory"])
            w_free_disk = worker_data["temporary_disk"] or 0
            tmp_cpu = 0
            tmp_mem = 0
            tmp_disk = 0
            running_jobs_on_worker = 0
            expected_time_left_sum = 0
            w_cd = worker_to_cluster_data(worker, cluster_worker)
            if not w_cd:
                continue
            for j_key, j_value in jobs_running_dict.items():
                if worker == j_value["nodes"]:
                    job_time_norm = None
                    # collect worker usage
                    w_free_cpu -= max(j_value["req_cpus"], 0)
                    w_free_mem -= max(j_value["req_mem"], 0)
                    w_free_disk -= max(j_value["temporary_disk"] or 0, 0)

                    logger.debug(
                        "running job %s on compatible worker %s",
                        j_value["jobname"],
                        worker,
                    )
                    tmp_job_data = job_data_from_database(
                        dict_db,
                        flavor_next["flavor"]["name"],
                        __clear_job_name(j_value),
                        True,
                    )
                    # jobs with history data
                    if tmp_job_data:
                        # expected normalized runtime on this node
                        job_time_norm = __read_job_time(tmp_job_data)
                    # include flavor times
                    else:
                        if config_mode["forecast_by_flavor_history"]:
                            job_time_norm = flavor_data_from_database(
                                dict_db, flavor_next["flavor"]["name"]
                            )

                    if job_time_norm:
                        current_norm_runtime = __calc_job_time_norm(
                            j_value["elapsed"], 1
                        )
                        # expected runtime < 0, under specified configuration
                        expected_time_left = max(
                            job_time_norm - current_norm_runtime, 0
                        )
                        logger.debug(
                            "jobname %s, tmp_job_data %s, job_time_norm %s, current_norm_runtime %s, norm left %s",
                            j_value["jobname"],
                            tmp_job_data,
                            job_time_norm,
                            current_norm_runtime,
                            expected_time_left,
                        )

                        if expected_time_left < match_val:
                            running_jobs_on_worker += 1
                            expected_time_left_sum += expected_time_left
                            tmp_cpu += j_value["req_cpus"]
                            tmp_mem += j_value["req_mem"]
                            tmp_disk += j_value["temporary_disk"]
                        logger.debug(
                            "worker %s, norm %s, job %s",
                            worker,
                            expected_time_left_sum,
                            j_key,
                        )
            logger.debug(
                "remaining worker %s resources cpu: %s, mem: %s, disk: %s",
                worker,
                w_free_cpu,
                w_free_mem,
                w_free_disk,
            )
            logger.debug(
                "expected free worker %s resources cpu: %s, mem: %s, disk: %s",
                worker,
                tmp_cpu,
                tmp_mem,
                tmp_disk,
            )
            tmp_cpu += w_free_cpu
            tmp_mem += w_free_mem
            tmp_disk += w_free_disk
            # consider available free memory
            if isinstance(worker_data["free_memory"], int):
                logger.debug(
                    "free_mem %s, tmp_mem %s", worker_data["free_memory"], tmp_mem
                )
                tmp_mem = max(worker_data["free_memory"], tmp_mem)
            # check how many jobs we expect to run on available nodes with upcoming worker resources
            if average_job_resources_tmp_disk != 0:
                simultaneous_jobs_per_worker = min(
                    __division_round_down(tmp_cpu, average_job_resources_cpu),
                    __division_round_down(tmp_mem, average_job_resources_memory),
                    __division_round_down(tmp_disk, average_job_resources_tmp_disk),
                )
            else:
                simultaneous_jobs_per_worker = min(
                    __division_round_down(tmp_cpu, average_job_resources_cpu),
                    __division_round_down(tmp_mem, average_job_resources_memory),
                )
            # Calculate how many jobs are possible, according to pending normalized runtime

            # average time until upcoming worker resources are free
            norm_left_tmp = __division_float(
                expected_time_left_sum, running_jobs_on_worker
            )
            logger.debug(
                "norm_left_tmp %s, expected_time_left_sum %s, running_jobs_on_worker %s",
                norm_left_tmp,
                expected_time_left_sum,
                running_jobs_on_worker,
            )
            norm_free_tmp = max((match_val - norm_left_tmp), 0)
            # how many jobs we can expect to process on a single worker
            jobs_per_timeslot_tmp = __division_round_up(
                norm_free_tmp, average_job_time_norm
            )
            logger.debug(
                "average_job_time_norm %s, jobs_per_timeslot_tmp %s",
                average_job_time_norm,
                jobs_per_timeslot_tmp,
            )
            jobs_worker_can_process = (
                jobs_per_timeslot_tmp * simultaneous_jobs_per_worker
            )

            logger.debug(
                "worker %s can process %s jobs "
                "with the normalized free time %s, todo %s "
                "simultaneous jobs per worker possible: %s",
                worker,
                jobs_worker_can_process,
                norm_free_tmp,
                norm_left_tmp,
                simultaneous_jobs_per_worker,
            )

            if jobs_worker_can_process > 0:
                logger.debug(worker_claimed)
                logger.debug(type(worker_claimed))
                worker_claimed.update(
                    {
                        worker: {
                            "cpu": tmp_cpu
                            - (
                                average_job_resources_cpu * simultaneous_jobs_per_worker
                            ),
                            "mem": tmp_mem
                            - (
                                average_job_resources_memory
                                * simultaneous_jobs_per_worker
                            ),
                            "disk": tmp_disk
                            - (
                                average_job_resources_tmp_disk
                                * simultaneous_jobs_per_worker
                            ),
                            "norm": norm_free_tmp,
                        }
                    }
                )
                jobs_workers_can_process += jobs_worker_can_process
                norm_workers_free += norm_free_tmp
                logger.debug("claim worker %s", worker)
                if (
                    job_time_sum - norm_workers_free
                ) < 0 and jobs_pending_flavor < jobs_workers_can_process:
                    break
                if jobs_pending_flavor < jobs_workers_can_process:
                    break
        logger.debug(
            "jobs_workers_can_process %s ==== %s ====",
            jobs_workers_can_process,
            worker_claimed,
        )
        logger.debug("worker_useful_reclaim %s", worker_useful_reclaim)

        for worker in worker_useful_reclaim:
            if jobs_pending_flavor < jobs_workers_can_process:
                break
            values = worker_claimed[worker]
            tmp_cpu = values["cpu"]
            tmp_mem = values["mem"]
            tmp_disk = values["disk"]
            norm_free_tmp = values["norm"]
            if average_job_resources_tmp_disk != 0:
                simultaneous_jobs_per_worker = min(
                    __division_round_down(tmp_cpu, average_job_resources_cpu),
                    __division_round_down(tmp_mem, average_job_resources_memory),
                    __division_round_down(tmp_disk, average_job_resources_tmp_disk),
                )
            else:
                simultaneous_jobs_per_worker = min(
                    __division_round_down(tmp_cpu, average_job_resources_cpu),
                    __division_round_down(tmp_mem, average_job_resources_memory),
                )

            jobs_per_timeslot_tmp = max(
                __division_round(norm_free_tmp, average_job_time_norm), 1
            )
            jobs_worker_can_process = (
                jobs_per_timeslot_tmp * simultaneous_jobs_per_worker
            )
            if jobs_worker_can_process > 0:
                worker_claimed.update(
                    {
                        worker: {
                            "cpu": tmp_cpu
                            - (
                                average_job_resources_cpu * simultaneous_jobs_per_worker
                            ),
                            "mem": tmp_mem
                            - (
                                average_job_resources_memory
                                * simultaneous_jobs_per_worker
                            ),
                            "disk": tmp_disk
                            - (
                                average_job_resources_tmp_disk
                                * simultaneous_jobs_per_worker
                            ),
                            "norm": norm_free_tmp,
                        }
                    }
                )
                jobs_workers_can_process += jobs_worker_can_process
                logger.debug(worker_claimed)
    logger.debug("we can expect to run %s pending jobs", jobs_workers_can_process)
    if jobs_pending_flavor < jobs_workers_can_process:
        jobs_workers_can_process_tmp = jobs_pending_flavor
    else:
        jobs_workers_can_process_tmp = jobs_workers_can_process
    logger.info(
        "we can expect to run %s pending jobs on current allocated workers %s in time",
        jobs_workers_can_process_tmp,
        worker_claimed,
    )
    logger.debug(
        "free normalized time %s, workers claimed %s, job process %s, known jobs %s",
        norm_workers_free,
        worker_claimed,
        jobs_workers_can_process,
        jobs_pending_flavor,
    )
    return jobs_workers_can_process_tmp


def __calculate_scale_up_data(
    flavor_job_list,
    jobs_pending_flavor,
    worker_count,
    worker_json,
    worker_drain,
    state,
    flavors_data,
    flavor_next,
    level,
    worker_memory_usage,
    jobs_running_dict,
    cluster_worker,
    flavors_started_cnt,
    level_pending,
    worker_claimed,
):
    """
    Create scale-up data for pending jobs with a specific flavor.
    Generates the scaling variable step by step according to user settings and available data from the job dictionary.
    A worker variable is generated in the process based on the job data, configuration and the scale force.
    :param flavor_job_list: workers should be generated with this job list as json dictionary object
    :param jobs_pending_flavor: number of pending jobs
    :param worker_count: number of current workers
    :param worker_json: current workers as json dictionary object
    :param worker_drain: worker list, only worker in state drain
    :param state: current multiscale state
    :param flavors_data: flavor data as json dictionary object
    :param flavor_next: next target flavor
    :param level: current level, flavor depth
    :param worker_memory_usage: current worker memory usage (including previous changes)
    :param jobs_running_dict: dictionary with running jobs
    :param cluster_worker: worker data from cluster api
    :param flavors_started_cnt: successful started flavors in previous levels
    :param level_pending: number of pending levels
    :param worker_claimed: active workers with possible free resources, each level forecast can claim
    :return: scale_up_data: scale up data for server
    """
    logger.debug(
        "--------------- %s : %s -----------------",
        inspect.stack()[0][3],
        flavor_next["flavor"]["name"],
    )
    workers_blocked = False
    need_workers = False
    large_flavors = config_mode["large_flavors"]
    worker_capable, _, worker_useful, _ = __current_workers_capable(
        flavor_job_list, worker_json, None
    )
    worker_usable = [x for x in worker_useful if x not in worker_drain]
    worker_usable_cnt = len(worker_usable)
    time_threshold = __get_time_threshold(flavor_next["flavor"]["name"])
    fv_mem = 0
    dict_db = None
    job_time_fv = None
    scale_up_data = {}
    flavor_tmp = flavor_next
    logger.debug(
        "worker_useful %s, worker_drain %s, worker_usable %s",
        worker_useful,
        worker_drain,
        worker_usable,
    )
    if level != 0 and flavors_started_cnt > 0:
        # possible wrong capable data by starting multiple flavors on higher levels
        worker_capable = True

    current_force = float(config_mode["scale_force"])
    forecast_by_flavor_history = config_mode["forecast_by_flavor_history"]
    forecast_by_job_history = config_mode["forecast_by_job_history"]
    flavor_depth = int(config_mode["flavor_depth"])
    limit_memory = int(config_mode["limit_memory"])
    forecast_active_worker = int(config_mode["forecast_active_worker"])
    flavor_default = config_mode["flavor_default"]
    auto_activate_large_flavors = int(config_mode["auto_activate_large_flavors"])
    limit_worker_starts = int(config_mode["limit_worker_starts"])

    if flavor_next is None or not flavors_data:
        return None, worker_memory_usage
    logger.info(
        "calc worker for %s; pending %s jobs; worker active %s, capable %s %s, memory %s, need %s; state %s",
        flavor_next["flavor"]["name"],
        jobs_pending_flavor,
        worker_count,
        worker_capable,
        worker_usable_cnt,
        worker_memory_usage,
        need_workers,
        state.name,
    )

    if forecast_by_flavor_history or forecast_by_job_history:
        dict_db = __get_file(DATABASE_FILE)
        if dict_db is None:
            logger.error("job database not available!")

    if dict_db and forecast_by_flavor_history and flavor_depth != DEPTH_MAX_WORKER:
        # required for jobs without history
        fv_name = __clear_flavor_name(flavor_next["flavor"]["name"])
        if fv_name in dict_db["flavor_name"]:
            job_time_fv = min(
                float(dict_db["flavor_name"][fv_name]["fv_time_norm"]), time_threshold
            )

    upscale_limit = __generate_upscale_limit(
        current_force, jobs_pending_flavor, worker_usable_cnt, level
    )
    logger.debug(
        "track upscale limit level %s: %s, flavor time: %s",
        level,
        upscale_limit,
        job_time_fv,
    )
    # memory limit pre-check
    if limit_memory != 0:
        if worker_memory_usage > convert_tb_to_mb(limit_memory):
            upscale_limit = 0
    logger.debug(
        "track upscale limit level %s: %s, worker_memory_usage: %s - memory limit check",
        level,
        upscale_limit,
        worker_memory_usage,
    )

    if not worker_capable or worker_count == 0:
        need_workers = True
    if need_workers and upscale_limit == 0:
        upscale_limit = 1
        logger.debug(
            "track upscale limit level %s: %s, worker capable: %s - force up",
            level,
            upscale_limit,
            worker_capable,
        )
    if upscale_limit > 0 or level_pending > 0:
        job_time_sum = 0
        job_time_cnt = 0
        jobs_pending_similar_data_missing = 0

        for _, value in flavor_job_list:
            if value["state"] == JOB_PENDING:
                job_name = __clear_job_name(value)
                # search for known jobs
                if forecast_by_job_history:
                    tmp_job_data = job_data_from_database(
                        dict_db, flavor_next["flavor"]["name"], job_name, True
                    )
                    if tmp_job_data:
                        job_time_norm = min(
                            __read_job_time(tmp_job_data), time_threshold
                        )
                    else:
                        # new job
                        job_time_norm = None
                    if job_time_norm:
                        # collect time values from pending jobs
                        logger.debug(
                            "job_time_norm: sum %s, norm %s, mode %s",
                            job_time_sum,
                            job_time_norm,
                            time_threshold,
                        )
                        job_time_sum += job_time_norm
                        job_time_cnt += 1
                    else:
                        jobs_pending_similar_data_missing += 1

        if not forecast_by_job_history:
            jobs_pending_similar_data_missing = jobs_pending_flavor
        # flavor based job time
        if job_time_fv:
            job_time_unk = jobs_pending_similar_data_missing * job_time_fv
        else:
            job_time_unk = jobs_pending_similar_data_missing * time_threshold

        # use only known jobs at lifetime check
        average_job_time_norm = __division_float(job_time_sum, job_time_cnt)
        # convert to a larger flavor when useful
        (
            fv_high,
            jobs_per_fv_high,
            average_job_resources_cpu,
            average_job_resources_memory,
            average_job_resources_tmp_disk,
        ) = convert_to_large_flavor(
            flavor_job_list, flavors_data, jobs_pending_flavor, flavor_next
        )

        # calculate worker with summarized job time norm
        if forecast_by_job_history or forecast_by_flavor_history:
            job_time_sum = max(float(job_time_sum), 0.1)
            # generate time based limit
            similar_job_worker_cnt = __division_round_down(job_time_sum, time_threshold)
            # generate upscale limit for jobs without job or flavor history data
            similar_job_worker_cnt_unk = __generate_upscale_limit(
                current_force,
                jobs_pending_similar_data_missing,
                worker_usable_cnt,
                level,
            )
            if job_time_fv:
                job_time_sum += job_time_unk
                job_time_cnt += jobs_pending_similar_data_missing
                similar_job_worker_cnt = __division_round_down(
                    job_time_sum, time_threshold
                )
                average_job_time_norm = __division_float(job_time_sum, job_time_cnt)
            else:
                similar_job_worker_cnt += similar_job_worker_cnt_unk

            # reduce previous generated upscale limit
            if upscale_limit > similar_job_worker_cnt >= 0:
                upscale_limit = similar_job_worker_cnt
            logger.debug(
                "track upscale limit level %s: %s, sum: %s - job time similar search",
                level,
                upscale_limit,
                job_time_sum,
            )
            # check active workers
            if forecast_active_worker > 0:
                worker_active_weight = __current_job_lifetime(
                    jobs_running_dict,
                    worker_usable,
                    average_job_resources_cpu,
                    average_job_resources_memory,
                    average_job_resources_tmp_disk,
                    dict_db,
                    flavor_next,
                    worker_json,
                    cluster_worker,
                    average_job_time_norm,
                    worker_claimed,
                    job_time_sum,
                    job_time_cnt,
                )

                if (
                    config_mode["forecast_occupied_worker"]
                    and worker_active_weight < 1
                    and flavors_started_cnt == 0
                    and worker_count != 0
                ):
                    # workers are blocked with current jobs
                    logger.debug("workers blocked with active jobs")
                    workers_blocked = True

                # reduce worker time count by active workers
                upscale_limit_active = jobs_pending_flavor - worker_active_weight

                # reduce upscale_limit by lifetime result
                if upscale_limit_active < upscale_limit:
                    upscale_limit = jobs_pending_flavor - worker_active_weight
                    logger.debug(
                        "track upscale limit level %s: %s, lifetime: - %s",
                        level,
                        upscale_limit,
                        jobs_pending_flavor,
                    )

                # reduce job stats by lifetime result
                if worker_active_weight > 0:
                    job_time_cnt_tmp = upscale_limit_active * __division_float(
                        job_time_cnt, jobs_pending_flavor
                    )
                    job_time_sum_tmp = job_time_cnt_tmp * __division_float(
                        job_time_sum, job_time_cnt
                    )
                    job_time_sum = max(job_time_sum_tmp, 0.1)
                    # generate time based limit update
                    similar_job_worker_cnt = __division_round(
                        job_time_sum, time_threshold
                    )

                    # include if flavor data not available
                    if not job_time_fv:
                        similar_job_worker_cnt += similar_job_worker_cnt_unk

                    # limit to previous generated upscale limit
                    if upscale_limit > similar_job_worker_cnt >= 0:
                        upscale_limit = similar_job_worker_cnt
                    logger.debug(
                        "merge jobs: norm_sum %s , worker_cnt %s , active_weight %s upscale %s",
                        job_time_sum,
                        similar_job_worker_cnt,
                        worker_active_weight,
                        upscale_limit,
                    )
                    logger.debug(
                        "upscale_limit %s similar_job_worker_cnt %s",
                        upscale_limit,
                        similar_job_worker_cnt,
                    )

        # set default flavor if default flavor meet requirements
        if flavor_default:
            fv_fix = __get_flavor_by_name(flavors_data, flavor_default)
            if fv_fix:
                if (
                    fv_fix["flavor"]["ram_gib"] >= flavor_tmp["flavor"]["ram_gib"]
                    and fv_fix["flavor"]["temporary_disk"]
                    >= flavor_tmp["flavor"]["temporary_disk"]
                    and fv_fix["flavor"]["vcpus"] >= flavor_tmp["flavor"]["vcpus"]
                ):
                    flavor_tmp = fv_fix
                else:
                    logger.error(
                        "flavor_default is active, but selected flavor not meet the requirements for minimal flavor"
                    )
                    return None, worker_memory_usage
            else:
                logger.error(
                    "flavor_default is active, selected flavor is not available"
                )
                return None, worker_memory_usage

        average_jobs_per_flavor = __multiple_jobs_per_flavor(
            flavor_tmp,
            average_job_resources_cpu,
            average_job_resources_memory,
            average_job_resources_tmp_disk,
        )
        # auto activate large flavors
        if (
            auto_activate_large_flavors != 0
            and __division_round(upscale_limit, average_jobs_per_flavor)
            >= auto_activate_large_flavors
        ):
            large_flavors = True
            logger.debug("auto activate high worker")
        # switch to higher flavor
        if large_flavors and fv_high:
            logger.debug(
                "high_flavor found %s cnt %s with %s jobs per flavor, minimum flavor was %s cnt %s",
                fv_high["flavor"]["name"],
                fv_high["cnt"],
                jobs_per_fv_high,
                flavor_tmp["flavor"]["name"],
                flavor_tmp["cnt"],
            )
            if fv_high["cnt"] < flavor_tmp["cnt"]:  # lower - higher flavor
                flavor_tmp = fv_high
                logger.debug("found high flavor for jobs in queue")
                average_jobs_per_flavor = __multiple_jobs_per_flavor(
                    flavor_tmp,
                    average_job_resources_cpu,
                    average_job_resources_memory,
                    average_job_resources_tmp_disk,
                )
        # more than one job per flavor
        if average_jobs_per_flavor > 1:
            upscale_limit_tmp = __division_round(upscale_limit, average_jobs_per_flavor)
            if upscale_limit_tmp < upscale_limit:
                logger.debug(
                    "check average job resources against flavor resources, average max %s, limit old %s new %s",
                    average_jobs_per_flavor,
                    upscale_limit,
                    upscale_limit_tmp,
                )
                upscale_limit = upscale_limit_tmp
        logger.debug(
            "track upscale limit level %s: %s, jobs per flavor: %s",
            level,
            upscale_limit,
            average_jobs_per_flavor,
        )

        if workers_blocked and upscale_limit < 1:
            logger.info("workers occupied, need one")
            upscale_limit = 1
        # memory limit check for scale up workers
        fv_mem = convert_gb_to_mb(flavor_tmp["flavor"]["ram_gib"])
        if limit_memory != 0:
            memory_buffer = convert_tb_to_mb(limit_memory) - worker_memory_usage
            flavor_possible = __division_round_down(memory_buffer, fv_mem)
            logger.debug(
                "memory limit: flavor_possible %s, upscale_limit %s,"
                " free %s, fv_mem %s, mem_sum %s, level %s",
                flavor_possible,
                upscale_limit,
                memory_buffer,
                fv_mem,
                worker_memory_usage,
                level,
            )
            if flavor_possible < upscale_limit:
                upscale_limit = flavor_possible

        logger.debug(
            "track upscale limit level %s: %s,  memory limit second check",
            level,
            upscale_limit,
        )

        # worker count limit check
        if limit_memory != 0:
            if worker_count >= limit_memory:
                upscale_limit = 0
            elif (worker_count + upscale_limit) > limit_memory:
                logger.debug(
                    "limit workers: up %s, count %s, limit %s, reduced %s",
                    upscale_limit,
                    worker_count,
                    limit_memory,
                    limit_memory - worker_count,
                )
                upscale_limit = limit_memory - worker_count
        logger.debug(
            "track upscale limit level %s: %s  - limit workers", level, upscale_limit
        )

        # force scale up, if required
        if need_workers and upscale_limit < 1:
            logger.info("scale force up %s, worker_count %s", state, worker_count)
            upscale_limit = 1
        logger.debug(
            "track upscale limit level %s: %s  - need workers", level, upscale_limit
        )
        # check available flavors on cluster
        flavor_count_available = __get_flavor_available_count(
            flavors_data, flavor_tmp["flavor"]["name"]
        )
        if upscale_limit > flavor_count_available:
            logger.debug(
                "track upscale limit level %s: %s  - reduce worker, available flavor %s ",
                level,
                upscale_limit,
                flavor_count_available,
            )
            upscale_limit = flavor_count_available
            logger.info("insufficient resources")
            __csv_log_entry("L", flavor_next["flavor"]["name"], 0)
        # limit worker starts
        if limit_worker_starts != 0 and upscale_limit > limit_worker_starts:
            upscale_limit = limit_worker_starts
        scale_up_data = {
            "password": __get_cluster_password(),
            "worker_flavor_name": flavor_tmp["flavor"]["name"],
            "upscale_count": upscale_limit,
            "version": AUTOSCALING_VERSION,
        }
        logger.debug(
            "track upscale limit level %s: %s  - start limit", level, upscale_limit
        )
        logger.debug("scale-up-data: \n%s", pformat(scale_up_data))
    logger.debug("claimed worker %s", worker_claimed)
    # log upper limit + changes by time + average jobs per flavor
    if upscale_limit <= 0 or not flavor_tmp:
        logger.debug("calculated upscale_limit is %d", upscale_limit)
        logger.info(
            "calculated upscale_limit is 0 or missing flavor '%s', skip",
            flavor_tmp["flavor"]["name"],
        )
        return None, worker_memory_usage
    logger.debug("track upscale limit level %s: %s  - final", level, upscale_limit)
    worker_memory_usage_tmp = worker_memory_usage + fv_mem * upscale_limit
    return scale_up_data, worker_memory_usage_tmp


def __get_time_border():
    """
    Return time classification values.
    :return:
    """
    return NORM_HIGH, NORM_LOW


def __calc_job_time_norm(job_time_sum, job_num):
    """
    Generate normalized value between 0 and 1 in relation to time classification range for scale up decision.
    :param job_time_sum: sum of job times
    :param job_num: job count
    :return: normalized value
    """
    _, border_min = __get_time_border()

    try:
        job_time_sum = int(job_time_sum)
        job_num = int(job_num)
        time_range_max = int(config_data["time_range_max"])
        time_range_min = int(config_data["time_range_min"])

        if job_num > 0 and job_time_sum > 1:
            job_time_average = __division_round(job_time_sum, job_num)
            norm = float(job_time_average - time_range_min) / float(
                time_range_max - time_range_min
            )

            if norm <= border_min:
                return float(border_min)
            return norm

    except ZeroDivisionError:
        logger.debug("cannot divide by zero, unusable data")
    return float(border_min)


def __multiscale_scale_down(
    scale_state, worker_json, worker_count, worker_free, jobs_pending_dict
):
    """
    Scale down part from multiscale.
    :param scale_state: current scale state
    :param worker_json: worker information as json dictionary object
    :param worker_count: number of current workers
    :param worker_free: number of unused workers
    :param jobs_pending_dict: pending jobs as dictionary
    :return: new scale state
    """
    worker_down_cnt = __calculate_scale_down_value(
        worker_count, worker_free, scale_state
    )
    # maximum worker count to scale down

    if scale_state == ScaleState.DELAY:
        scale_state = ScaleState.DOWN
        time.sleep(int(config_mode["scale_delay"]))
    elif scale_state == ScaleState.DOWN:
        cluster_scale_down_specific(
            worker_json, worker_down_cnt, Rescale.CHECK, jobs_pending_dict
        )
        scale_state = ScaleState.DONE
    else:
        scale_state = ScaleState.SKIP
        logger.info("---- SCALE DOWN - condition changed - skip ----")
    return scale_state


def __check_cluster_node_workers(cluster_workers, worker_json):
    """
    Compare cluster nodes with scheduler. If required, try a second reconfiguration.
    :param cluster_workers: cluster data as json
    :param worker_json: worker data as json
    :return: worker missing
    """
    worker_missing = __compare_custer_node_workers(cluster_workers, worker_json)

    if worker_missing:
        logger.debug("1. check, missing worker list: %s", worker_missing)
        rescale_init()
        worker_json, _, _, _, _ = receive_node_data_live()
        worker_missing = __compare_custer_node_workers(cluster_workers, worker_json)
        logger.debug("2. check, missing worker list: %s", worker_missing)
    return worker_missing


def __compare_custer_node_workers(cluster_workers, worker_json):
    """
    Compare cluster nodes with scheduler nodes
    :param cluster_workers: cluster worker data as json
    :param worker_json: worker node data as json dictionary
    :return: worker missing
    """
    # verify cluster workers with node data
    cluster_worker_list = []
    worker_missing = []
    try:
        if cluster_workers is not None:
            for c_worker in cluster_workers:
                cluster_worker_list.append(c_worker["hostname"])
            logger.debug(
                "cluster_workers %s, worker_json %s",
                cluster_worker_list,
                worker_json.keys(),
            )
            worker_missing = list(
                set(worker_json.keys()).difference(cluster_worker_list)
            )
            if worker_missing:
                logger.debug("found missing workers in node data %s", worker_missing)
    except AttributeError:
        logger.debug(
            "received broken data, cluster_workers: %s, worker_json: %s",
            cluster_workers,
            worker_json,
        )
    return worker_missing


def __scale_down_error_workers(worker_err_list):
    """
    Scale down workers by a given error worker list and print error message.
    Skip, if the error worker list is empty.
    :param worker_err_list: workers in state error as list
    :return:
    """
    if worker_err_list:
        logger.error(
            "found %s broken workers on cluster, no state change %s in %s",
            len(worker_err_list),
            worker_err_list,
            config_mode["scale_delay"],
        )
        time.sleep(WAIT_CLUSTER_SCALING)
        cluster_scale_down_specific_self_check(worker_err_list, Rescale.CHECK)
        __csv_log_entry("E", "0", "0")


def __verify_cluster_workers(cluster_workers, worker_json):
    """
    Check via cluster api if workers are broken and compare workers from first and second call.
        - delete workers if states on cluster are stuck at error, down, planned or drain
        - server side (check worker)
    :param cluster_workers: cluster data from first call
    :param worker_json: worker information as json dictionary object
    :return: cluster_worker_no_change, cluster_worker_complete
    """
    if cluster_workers is None:
        return None
    if not cluster_workers:
        logger.debug("cluster worker data is empty")
    worker_missing = __check_cluster_node_workers(cluster_workers, worker_json)

    error_states = [WORKER_ERROR, WORKER_PLANNED, WORKER_CREATION_FAILED]
    worker_cluster_err_list = []
    for clt in cluster_workers:
        if clt["hostname"] != NODE_DUMMY:
            logger.debug(
                "cluster worker: %s, state %s", clt["hostname"], clt["status"].upper()
            )
            if any(ele in clt["status"].upper() for ele in error_states):
                worker_cluster_err_list.append(clt["hostname"])
    logger.debug("cluster workers, error list: %s", worker_cluster_err_list)
    logger.debug("cluster workers, missing list %s", worker_missing)
    error_worker_list = worker_missing + worker_cluster_err_list
    if error_worker_list:
        __scale_down_error_workers(error_worker_list)
    return cluster_workers


def __worker_same_states(worker_json, worker_copy):
    """
    Create worker copy from scheduler
        - compare two worker objects, return only workers with the same state
        - remove most capable and active worker from list, due scheduler limitations
        - include worker marked for removal
    :param worker_json: worker information as json dictionary object
    :param worker_copy: worker copy for comparison
    :return: worker object with workers stuck in the same state
    """
    if worker_copy is None:
        worker_copy = {}
        for key, value in worker_json.items():
            if "worker" in key:
                worker_copy[key] = value
        return worker_copy

    worker_copy_tmp = {}
    for key, value in worker_json.items():
        if key in worker_copy:
            if value["state"] == worker_copy[key]["state"]:
                worker_copy_tmp[key] = value
                logger.debug(
                    "%s state: %s vs %s - state no change",
                    key,
                    value["state"],
                    worker_copy[key]["state"],
                )
            elif NODE_IDLE in value["state"] and NODE_DRAIN in value["state"]:
                worker_copy_tmp[key] = value
                logger.debug(
                    "%s state: %s vs %s - state recently changed, exception worker idle & drain!",
                    key,
                    value["state"],
                    worker_copy[key]["state"],
                )
            else:
                logger.debug(
                    "%s state: %s vs %s - state recently changed or keep alive",
                    key,
                    value["state"],
                    worker_copy[key]["state"],
                )
    return worker_copy_tmp


def multiscale(flavor_data):
    """
    Make a scaling decision in advance by scaling state classification.
    :param flavor_data: current flavor data
    :return:
    """
    state = ScaleState.DELAY
    update_all_yml_files_and_run_playbook()

    # create worker copy, only use delete workers after a delay, give scheduler and network time to activate
    worker_copy = None
    cluster_workers = None
    changed_data = False
    scale_down_value = 0
    drained = False
    drain_large_nodes = config_mode["drain_large_nodes"]
    scale_delay = int(config_mode["scale_delay"])
    if flavor_data is None:
        return
    cluster_data = get_cluster_data()
    while state not in (ScaleState.SKIP, ScaleState.DONE):
        jobs_pending_dict, jobs_running_dict = receive_job_data()
        jobs_pending = len(jobs_pending_dict)
        (
            worker_json,
            worker_count,
            worker_in_use_list,
            worker_drain,
            _,
        ) = receive_node_data_db(True)
        worker_in_use = len(worker_in_use_list)
        worker_free = worker_count - worker_in_use

        # cluster configuration may be broken
        if worker_json is None:
            logger.warning("unable to receive worker data, reconfigure cluster")
            rescale_cluster(0)
            return

        if state == ScaleState.DELAY:
            # check for broken workers at cluster first
            cluster_workers = __verify_cluster_workers(
                get_cluster_workers(cluster_data), worker_json
            )
            if cluster_workers is None:
                state = ScaleState.SKIP
                logger.error("unable to receive cluster workers")
                continue
            if drain_large_nodes and jobs_pending > 0 and not drained:
                if set_nodes_to_drain(
                    jobs_pending_dict,
                    worker_json,
                    flavor_data,
                    cluster_workers,
                    jobs_running_dict,
                ):
                    # update data after drain + scale down
                    cluster_data = get_cluster_data()
                    drained = True
                    continue

        # create worker copy, only delete workers after a delay, give scheduler and network time to activate
        worker_copy = __worker_same_states(worker_json, worker_copy)

        logger.info(
            "worker_count: %s worker_in_use: %s worker_idle: %s jobs_pending: %s",
            worker_count,
            worker_in_use,
            worker_free,
            jobs_pending,
        )
        if worker_count == 0 and jobs_pending > 0 and not state == ScaleState.FORCE_UP:
            state = ScaleState.FORCE_UP
            logger.debug("zero worker and jobs pending, force up")
        # SCALE DOWN
        elif (
            worker_count > DOWNSCALE_LIMIT and worker_in_use == 0 and jobs_pending == 0
        ):
            # workers are not in use and not reached DOWNSCALE_LIMIT
            logger.info("---- SCALE DOWN - DELETE: workers are not in use")
            state = __multiscale_scale_down(
                state,
                worker_copy,
                worker_count,
                worker_free,
                jobs_pending_dict,
            )
        elif (
            worker_count > DOWNSCALE_LIMIT
            and worker_free >= 1
            and worker_in_use > 0
            and jobs_pending == 0
        ):
            logger.info("---- SCALE DOWN - DELETE: workers are free, zero jobs pending")
            state = __multiscale_scale_down(
                state,
                worker_copy,
                worker_count,
                worker_free,
                jobs_pending_dict,
            )
        # SCALE DOWN and UP
        elif worker_free > 0 and jobs_pending >= 1 and not state == ScaleState.FORCE_UP:
            # -> SCALE DOWN - delete free worker with too low resources
            # - force upscale after downscale
            logger.info("---- SCALE DOWN_UP - worker idle and jobs pending ----")
            if state == ScaleState.DOWN_UP:
                logger.info("---- SCALE DOWN_UP - cluster scale down ----")
                scale_down_value = __calculate_scale_down_value(
                    worker_count, worker_free, state
                )
                changed_data = cluster_scale_down_specific(
                    worker_copy,
                    scale_down_value,
                    Rescale.NONE,
                    jobs_pending_dict,
                )
                if not changed_data:
                    scale_down_value = 0
                logger.debug(
                    "scale_down_value %s, changed_data %s",
                    scale_down_value,
                    changed_data,
                )
                # skip one reconfigure at FORCE_UP
                state = ScaleState.FORCE_UP
            elif state == ScaleState.DELAY:
                logger.debug("---- SCALE DOWN_UP - SCALE_DELAY ----")
                state = ScaleState.DOWN_UP
                time.sleep(scale_delay)
            else:
                state = ScaleState.SKIP
                logger.info("---- SCALE DOWN_UP - condition changed - skip ----")
        # SCALE UP
        elif (
            worker_count == worker_in_use and jobs_pending >= 1
        ) or state == ScaleState.FORCE_UP:
            # if all workers are in use and jobs pending and jobs require more time
            logger.info("---- SCALE UP - all workers are in use with pending jobs ----")
            if not flavor_data:
                logger.error("missing flavor information")
                return
            logger.debug("flavor data count %s", len(flavor_data))
            if state == ScaleState.DELAY:
                state = ScaleState.UP
                time.sleep(scale_delay)
                # speed up scale-up without delay
            elif state == ScaleState.UP:
                cluster_scale_up(
                    jobs_pending_dict,
                    jobs_running_dict,
                    worker_count,
                    worker_json,
                    worker_drain,
                    state,
                    flavor_data,
                    cluster_workers,
                )
                state = ScaleState.DONE
            elif state == ScaleState.FORCE_UP:
                logger.debug("---- SCALE UP - force scale up ----")
                if (
                    not cluster_scale_up(
                        jobs_pending_dict,
                        jobs_running_dict,
                        worker_count - scale_down_value,
                        worker_json,
                        worker_drain,
                        state,
                        flavor_data,
                        cluster_workers,
                    )
                    and changed_data
                ):
                    # prevent unnecessary two rescales during scale down and up
                    # cluster data changed by scale down, but no scale up was possible

                    # rescale once from scale-down (combined rescale DOWN_UP)
                    rescale_cluster(worker_count - scale_down_value)

                state = ScaleState.DONE
            else:
                logger.info(
                    "---- SCALE UP - condition changed - skip ---- %s", state.name
                )
                state = ScaleState.SKIP
        else:
            if state in (ScaleState.UP, ScaleState.DOWN):
                logger.info("---- scaling condition changed - skip ----")
            state = ScaleState.SKIP

        logger.info("----------- scaling state: %s -----------", state.name)


def __cloud_api_(portal_url_scale, worker_data):
    logger.debug(f"---Scaling -- {portal_url_scale}\n\n\t {worker_data}")
    worker_data.update({"scaling_type": SCALING_TYPE})
    response = requests.post(url=portal_url_scale, json=worker_data)
    logger.debug(response.raise_for_status())
    logger.info("response code: %s, message: %s", response.status_code, response.text)

    try:
        response_json = response.json()
        if "password" in response_json:
            password = response_json["password"]
            logger.debug(f"Set New Password: {password}")
            with open(CLUSTER_PASSWORD_FILE, "w", encoding="utf8") as f:
                f.write(json.dumps(response_json))
            return True
        else:
            logger.debug("No password found in the response.")
            return False
    except json.JSONDecodeError:
        logger.debug("Invalid JSON response.")
        return False


def __sleep_on_server_error():
    sleep_time = int(config_mode["service_frequency"]) * WAIT_CLUSTER_SCALING
    if not systemd_start:
        sleep_time = WAIT_CLUSTER_SCALING
    logger.error("server error, sleep %s seconds", sleep_time)
    time.sleep(sleep_time)


def cloud_api(portal_url_scale, worker_data):
    """
    create connection to cloud api
    send data for up-scaling or down-scaling

    api response code is typically 200
    api response text contains a new cluster password
        CLUSTER_PASSWORD_FILE will be updated during execution

    api error response example: {"error":"1 additional workers requested, only 0 possible"}

    :param portal_url_scale: portal url, used for up- or down-scaling
    :param worker_data: sent data to the server
    :return: new cluster password
    """

    logger.info("send cloud_api data: ")
    logger.info(pformat(worker_data))
    logger.info("URL: %s ", portal_url_scale)
    try:
        with TerminateProtected():
            return __cloud_api_(portal_url_scale, worker_data)
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        if e.response.status_code == HTTP_CODE_UNAUTHORIZED:
            handle_code_unauthorized(res=e.response)
        else:
            __sleep_on_server_error()
        __csv_log_entry("E", 0, "13")
        return None
    except OSError as error:
        logger.error(error)
        return None
    except Exception as e:
        logger.error("error by accessing cloud api %s", e)
        return None


def handle_code_unauthorized(res):
    if res.status_code == HTTP_CODE_UNAUTHORIZED:
        error_msg = res.json()["message"]
        logger.error(error_msg)

        if "Invalid Password" in error_msg:

            logger.error(get_wrong_password_msg())
            sys.exit(1)

        elif "Wrong script version!" in error_msg:

            latest_version = res.json().get("latest_version", None)

            automatic_update(latest_version=latest_version)


def cluster_scale_up(
    jobs_pending_dict,
    jobs_running_dict,
    worker_count,
    worker_json,
    worker_drain,
    state,
    flavor_data,
    cluster_worker,
):
    """
    scale up and rescale cluster with data generation
    skip if scale up data return None
    :param worker_drain: worker list, only worker in state drain
    :param jobs_running_dict: dictionary with running jobs
    :param flavor_data: current flavor data
    :param worker_json: worker node data as json dictionary object
    :param state: current scaling state
    :param worker_count: number of total workers
    :param jobs_pending_dict: jobs as json dictionary object
    :param cluster_worker: worker data from cluster api
    :return: boolean, success
    """

    job_priority = __sort_jobs(jobs_pending_dict)

    flavor_depth_list = classify_jobs_to_flavors(job_priority, flavor_data)
    if not flavor_depth_list:
        return False
    flavor_col_len = len(flavor_depth_list)
    data_list = []
    flavor_depth = config_mode["flavor_depth"]
    upscale_cnt = 0  # number of started workers
    flavor_index_cnt = 0
    level_sup = False  # last level scale up
    flavors_started_cnt = 0  # workers are started
    worker_memory_usage = __get_worker_memory_usage(worker_json)
    worker_claimed = {}  # claimed worker, possible free resources for pending jobs

    skip_starts = False  # skip starts if life count detected from previous level
    while flavor_col_len > flavor_index_cnt:  # and not data
        jobs_pending_flavor, flavor_next, flavor_job_list = flavor_depth_list[
            flavor_index_cnt
        ]
        if not flavor_next:
            flavor_index_cnt += 1
            continue
        if flavors_started_cnt > 0 and flavor_depth == DEPTH_MULTI_SINGLE:
            skip_starts = True
        if level_sup and not skip_starts:
            # wait and recheck flavor data if workers started on previous level index
            time.sleep(WAIT_CLUSTER_SCALING)
            flavor_data = get_usable_flavors(
                True, True
            )  # check if available flavor count changed
            level_sup = False

        data_tmp, worker_memory_usage_tmp = __calculate_scale_up_data(
            flavor_job_list,
            jobs_pending_flavor,
            worker_count,
            worker_json,
            worker_drain,
            state,
            flavor_data,
            flavor_next,
            flavor_index_cnt,
            worker_memory_usage,
            jobs_running_dict,
            cluster_worker,
            flavors_started_cnt,
            flavor_col_len - flavor_index_cnt - 1,
            worker_claimed,
        )
        # multi-start
        if data_tmp:
            data_list.append(data_tmp)
            if not skip_starts:
                if cloud_api(get_url_scale_up(), data_tmp):
                    logger.debug("start workers for level %s", flavor_index_cnt)
                    worker_memory_usage = worker_memory_usage_tmp
                    upscale_cnt += data_tmp["upscale_count"]
                    worker_count += data_tmp["upscale_count"]
                    flavors_started_cnt += 1
                    level_sup = True
                else:
                    logger.debug(
                        "starting workers via API failed for level %s", flavor_index_cnt
                    )
        logger.debug(
            "flavor data generation for future jobs level: %s data: %s claimed %s",
            flavor_index_cnt,
            data_tmp,
            worker_claimed,
        )
        flavor_index_cnt += 1
    time_now = __get_time()

    if upscale_cnt > 0:
        rescale_cluster(worker_count)
        rescale_time = __get_time() - time_now
        if state == ScaleState.FORCE_UP:
            __csv_log_entry("U", upscale_cnt, "8")
        else:
            __csv_log_entry("U", upscale_cnt, "5")

        logger.info(
            "scale up started %s worker with %s different flavor in %s seconds",
            upscale_cnt,
            flavors_started_cnt,
            rescale_time,
        )
        return True
    return False


def cluster_scale_down_specific_hostnames(hostnames, rescale):
    """
    scale down with specific hostnames
    :param hostnames: hostname list as string "worker1,worker2,worker..."
    :param rescale: if rescale (with worker check) is desired
    :return:
    """
    worker_hostnames = "[" + hostnames + "]"
    logger.debug(hostnames)
    return cluster_scale_down_specific_hostnames_list(worker_hostnames, rescale)


def cluster_scale_down_specific_self_check(worker_hostnames, rescale):
    """
    scale down with specific hostnames
    include a final worker check with most up-to-date worker data before scale down
        exclude allocated and mix worker
    :param worker_hostnames: hostname list
    :param rescale: if rescale (with worker check) is desired
    :return: success
    """
    if not worker_hostnames:
        return False
    # wait for state update
    time.sleep(WAIT_CLUSTER_SCALING)
    worker_json, _, _, _, worker_drain_idle = receive_node_data_db(False)
    scale_down_list = []

    for wb in worker_hostnames:
        if wb in worker_json:
            if wb not in worker_drain_idle:
                if (
                    NODE_ALLOCATED in worker_json[wb]["state"]
                    or NODE_MIX in worker_json[wb]["state"]
                ):
                    logger.debug(
                        "safety check: worker allocated, rescue worker %s, state %s ! ",
                        wb,
                        worker_json[wb]["state"],
                    )
                else:
                    logger.debug(
                        "worker %s added with state  %s", wb, worker_json[wb]["state"]
                    )
                    scale_down_list.append(wb)
            else:
                logger.debug("worker marked as idle and drain %s", wb)
                scale_down_list.append(wb)
        else:
            logger.debug("not in scheduler worker list %s", wb)
            scale_down_list.append(wb)

    logger.debug(
        "last scale down check: available:\n%s\nidle %s:\n%s\nreceived %s:\n%s\nresult %s:\n%s, rescale %s",
        worker_json.keys(),
        len(worker_drain_idle),
        worker_drain_idle,
        len(worker_hostnames),
        worker_hostnames,
        len(scale_down_list),
        scale_down_list,
        rescale,
    )

    return cluster_scale_down_specific_hostnames_list(scale_down_list, rescale)


def cluster_scale_down_specific_hostnames_list(worker_hostnames, rescale):
    """
    scale down with specific hostnames
    :param worker_hostnames: hostname list
    :param rescale: if rescale (with worker check) is desired
    :return: success
    """
    logger.debug(
        "scale down %s worker, list: %s", len(worker_hostnames), worker_hostnames
    )

    if not worker_hostnames:
        return False

    data = {
        "password": __get_cluster_password(),
        "worker_hostnames": worker_hostnames,
        "version": AUTOSCALING_VERSION,
    }
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific())
    logger.debug(
        "\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n"
    )
    result_ = True
    if cloud_api(get_url_scale_down_specific(), data) is not None:
        if rescale == Rescale.CHECK:
            result_ = rescale_cluster(0)
        elif rescale == Rescale.INIT:
            result_ = rescale_init()
        __csv_log_entry("D", len(worker_hostnames), "3")
    else:
        __csv_log_entry("D", len(worker_hostnames), "4")
        result_ = False
    update_all_yml_files_and_run_playbook()

    return result_


def update_all_yml_files_and_run_playbook():
    # Download the scaling.py script
    scaling_script_url = __get_scaling_script_url()
    if update_scaling_script(url=scaling_script_url, filename=SCALING_SCRIPT_FILE):

        # Run the scaling.py script
        command = ["python3", SCALING_SCRIPT_FILE, "-p", __get_cluster_password()]
        subprocess.run(command)
    else:
        logger.error(f"Failed to download script from {scaling_script_url}.")


def update_scaling_script(url, filename):
    """Checks if the file needs to be downloaded or updated."""
    logger.debug(f"Check if file {filename} needs to be redownloaded from {url}...")

    if not os.path.exists(filename):
        print("File not present. Downloading...")
        if not download_file(url, filename):
            return False

    # Create .md5 and .etag files only after a successful download
    etag_filename = filename + ".etag"
    md5_filename = filename + ".md5"

    if not os.path.exists(etag_filename) or not os.path.exists(md5_filename):
        logger.debug(f"etag or md5 file for {filename} not present. Saving files...")
        # Save .md5 and .etag
        with open(md5_filename, "w") as f:
            f.write(calculate_md5(filename))
        response = requests.head(url)  # Use HEAD request to get headers only
        response.raise_for_status()
        remote_etag = response.headers.get("ETag")
        with open(etag_filename, "w") as f:
            f.write(remote_etag)

    else:
        existing_md5 = None
        with open(md5_filename, "r") as f:
            existing_md5 = f.read().strip()

        calculated_md5 = calculate_md5(filename)

        if calculated_md5 != existing_md5:
            logger.debug(f"md5 checksum mismatch - {filename}! Redownloading...")
            if not download_file(url, filename):
                return False
            # Save new .md5 and .etag after redownload
            with open(md5_filename, "w") as f:
                f.write(calculate_md5(filename))  # Calculate after download
            response = requests.head(url)  # Use HEAD request to get headers only
            response.raise_for_status()
            remote_etag = response.headers.get("ETag")
            with open(etag_filename, "w") as f:
                f.write(remote_etag)
        else:
            existing_etag = None
            with open(etag_filename, "r") as f:
                existing_etag = f.read().strip()

            response = requests.head(url)  # Use HEAD request to get headers only
            response.raise_for_status()
            remote_etag = response.headers.get("ETag")

            if remote_etag and remote_etag != existing_etag:
                logger.debug(f"etag mismatch - {filename} ! Downloading...")
                if not download_file(url, filename):
                    return False
                # Save new .md5 and .etag after redownload
                with open(md5_filename, "w") as f:
                    f.write(calculate_md5(filename))  # Calculate after download
                with open(etag_filename, "w") as f:
                    f.write(remote_etag)
            else:
                logger.debug("File {file} is up to date. Doing nothing.")

    return True  # Check completed


def download_file(url, filename):
    """Downloads a file from a URL."""
    try:
        logger.info(f"Downloading {filename} script from: {url}")

        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses

        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info("File downloaded and saved: %s", filename)
        return True  # Download successful
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during download: {e}")
        return False  # Download failed


def calculate_md5(file_path):
    """Calculates the MD5 checksum of a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()


def cluster_scale_down_specific(worker_json, worker_num, rescale, jobs_dict):
    """
    scale down a specific number of workers, downscale list is self generated
    :param jobs_dict: jobs dictionary with pending jobs
    :param worker_json: worker information as json dictionary object
    :param worker_num: number of worker to delete
    :param rescale: rescale cluster with worker check
    :return: boolean, success
    """
    worker_list = __generate_downscale_list(worker_json, worker_num, jobs_dict)
    return cluster_scale_down_specific_self_check(worker_list, rescale)


def __cluster_scale_down_complete():
    """
    scale down all scheduler workers
    :return: boolean, success
    """
    worker_json, worker_count, _, _, _ = receive_node_data_db(False)
    return cluster_scale_down_specific(worker_json, worker_count, Rescale.CHECK, None)


def __cluster_shut_down():
    """
    scale down all workers by cluster data
    :return:
    """
    cluster_workers = get_cluster_workers_from_api()
    worker_list = []
    if cluster_workers is not None:
        for cl in cluster_workers:
            if "worker" in cl["hostname"]:
                logger.debug("delete worker %s", cl["hostname"])
                worker_list.append(cl["hostname"])
        if worker_list:
            logger.info("scale down %s", worker_list)
            time.sleep(WAIT_CLUSTER_SCALING)
            return cluster_scale_down_specific_hostnames_list(
                worker_list, Rescale.CHECK
            )
    return False


def rescale_cluster(worker_count):
    """
    apply the new worker configuration
    execute scaling, modify and run ansible playbook
    wait for workers, all should be ACTIVE (check_workers)
    :param worker_count: expected worker number, 0 default delay
    :return: boolean, success
    """
    if worker_count == 0:
        logger.debug("initiate rescale, wait %s seconds", WAIT_CLUSTER_SCALING)
        time.sleep(WAIT_CLUSTER_SCALING)

    no_error_scale, cluster_data = check_workers(Rescale.NONE, worker_count)
    rescale_success = rescale_init()
    if not rescale_success:
        worker_json, _, _, _, _ = receive_node_data_db(False)
        worker_err_list = __check_cluster_node_workers(
            get_cluster_workers_from_api(), worker_json
        )
        if worker_err_list:
            cluster_scale_down_specific_self_check(worker_err_list, Rescale.NONE)
            __csv_log_entry("E", "0", "20")
            logger.error("playbook failed, possible server problem, wait 10 minutes")
            time.sleep(WAIT_CLUSTER_SCALING * 60)
    if no_error_scale and rescale_success:
        return True
    return False


def __cluster_scale_up_test(upscale_limit):
    """
    scale up cluster by number
    :param upscale_limit: scale up number of workers
    :return:
    """
    logger.info(
        "----- test upscaling, start %d new worker nodes with %s -----",
        upscale_limit,
        config_mode["flavor_default"],
    )
    up_scale_data = {
        "password": __get_cluster_password(),
        "worker_flavor_name": config_mode["flavor_default"],
        "upscale_count": upscale_limit,
        "version": AUTOSCALING_VERSION,
    }
    cloud_api(get_url_scale_up(), up_scale_data)
    rescale_cluster(0)


def __cluster_scale_up_specific(flavor_name, upscale_limit, rescale):
    """
    scale up cluster by flavor
    :param flavor_name: target flavor
    :param upscale_limit: scale up number of workers
    :param rescale: rescaling required
    :return: boolean, success
    """
    logger.info(
        "----- upscaling, start %d new worker nodes with %s -----",
        upscale_limit,
        flavor_name,
    )
    up_scale_data = {
        "password": __get_cluster_password(),
        "worker_flavor_name": flavor_name,
        "upscale_count": upscale_limit,
        "version": AUTOSCALING_VERSION,
    }
    _, worker_count, _, _, _ = receive_node_data_live()
    if not cloud_api(get_url_scale_up(), up_scale_data):
        return False
    if not rescale:
        return True
    if not rescale_cluster(worker_count + upscale_limit):
        return False
    return True


def __cluster_scale_down_idle():
    """
    scale down all idle worker
    :return:
    """
    worker_json, worker_count, worker_in_use, _, _ = receive_node_data_db(False)
    worker_cnt = worker_count - len(worker_in_use)
    cluster_scale_down_specific(worker_json, worker_cnt, Rescale.CHECK, None)


def __start_service():
    """
    Start service with config available check.
    :return:
    """
    if not os.path.exists(FILE_CONFIG_YAML):
        download_autoscaling_config()

    if not os.path.exists(CLUSTER_PASSWORD_FILE):
        logger.debug("cluster password file missing")
        __set_cluster_password()
    logger.debug("restarting service ... ")
    restart_systemd_service()


def __print_help():
    logger.debug(
        textwrap.dedent(
            """\
            Option      : Long option       :    Argument     : Meaning
            ----------------------------------------------------------------------------------
            _           :                   :                 : rescale with worker check
            -v          : -version          :                 : print the current version number
            -h          : -help             :                 : print this help information
            -fv         : -flavors          :                 : print available flavors
            -m          : -mode             :                 : activate mode from configuration
            -p          : -password         :                 : set cluster password
            -rsc        : -rescale          :                 : run scaling with ansible playbook
            -s          : -service          :                 : run as service
            -i          : -ignore           :                 : ignore workers
            -sdc        : -scaledownchoice  :                 : scale down (worker id) - interactive mode
            -sdb        : -scaledownbatch   :                 : scale down (batch id)  - interactive mode
            -suc        : -scaleupchoice    :                 : scale up - interactive mode
            -sdi        : -scaledownidle    :                 : scale down all workers (idle + worker check)
            -csd        : -clustershutdown  :                 : delete all workers from cluster (api)
            -u          : -update           :                 : update autoscaling (exclude configuration)
            -d          : -drain            :                 : set manual nodes to drain
                        : -reset            :                 : reset autoscaling and configuration
                        : -start            :                 : systemd: (re-)start service
                        : -stop             :                 : systemd: stop service
                        : -status           :                 : systemd: service status
                        : -visual           :                 : visualize log data (generate pdf)
                        : -visual           :"y-m-t-h:y-m-t-h": visualize log data by time range
                        : -clean            :                 : clean log data
        """
        )
    )


def __print_help_debug():
    if LOG_LEVEL == logging.DEBUG:
        logger.debug(
            textwrap.dedent(
                """\
            -cw         : -checkworker      :                 : check for broken worker
            -nd         : -node             :                 : receive node data
            -l          : -log              :                 : create csv log entry
            -c          : -clusterdata      :                 : print cluster data
            -su         : -scaleup          :                 : scale up default flavor (one worker)
            -su         : -scaleup          :  number         : scale up default flavor
            -sus        : -scaleupspecific  : "flavor"  "2"   : scale up by flavor and number
            -sd         : -scaledown        :                 : scale down cluster, all idle workers
            -sds        : -scaledownspecific:  wk1,wk2        : scale down cluster by workers
            -cdb        : -clusterbroken    :                 : delete broken workers (cluster vs scheduler)
            -t          : -test             :                 : test run
            -j          : -jobdata          :                 : print current job information
            -jh         : -jobhistory       :                 : print recent job history
            """
            )
        )


def setup_logger(log_file):
    """
    setup logger
    :param log_file: log file location
    :return: logger
    """
    logger_ = logging.getLogger(f"{__name__}.{os.getpid()}")
    logger_.setLevel(LOG_LEVEL)
    logger_.addFilter(ProcessFilter(pid=os.getpid()))
    if LOG_LEVEL == logging.DEBUG:
        file_handler = logging.FileHandler(log_file)
    else:
        file_handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=1)

    stream_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger_.addHandler(file_handler)
    logger_.addHandler(stream_handler)
    return logger_


def create_pid_file():
    """
    create pid file, exit if file exist
    :return: pid file
    """
    pid = str(os.getpid())
    logger.debug("start with pid %s", pid)
    pidfile_ = AUTOSCALING_FOLDER + FILE_PID
    if os.path.isfile(pidfile_):
        logger.critical(
            "please terminate the running autoscaling process,\npid file already exist %s, exit\n",
            pidfile_,
        )
        sys.exit(1)
    open(pidfile_, "w", encoding="utf8").write(pid)
    return pidfile_


def __example_configuration():
    config_example = {
        "scaling": {
            "portal_scaling_link": "https://simplevm.denbi.de/portal/api/autoscaling/",
            "portal_webapp_link": "https://simplevm.denbi.de/portal/webapp/#/clusters/overview",
            "scaling_script_url": "https://raw.githubusercontent.com/deNBI/user_scripts/refs/heads/master/bibigrid/scaling.py",
            "scheduler": "slurm",
            "active_mode": "basic",
            "automatic_update": True,
            "database_reset": False,
            "time_range_max": 1000,
            "time_range_min": 1,
            "pattern_id": "",
            "history_recall": 7,
            "ignore_workers": [],
            "resource_sorting": True,
            "pending_jobs_percent": 1.0,
            "mode": {
                "basic": {
                    "info": "",
                    "service_frequency": 60,
                    "limit_memory": 0,
                    "limit_worker_starts": 10,
                    "limit_workers": 0,
                    "limit_flavor_usage": None,
                    "scale_force": 0.6,
                    "scale_delay": 60,
                    "worker_cool_down": 60,
                    "worker_weight": 0.00,
                    "smoothing_coefficient": 0.00,
                    "forecast_by_flavor_history": False,
                    "forecast_by_job_history": False,
                    "forecast_active_worker": 0,
                    "forecast_occupied_worker": False,
                    "job_match_value": 0.95,
                    "job_time_threshold": 0.5,
                    "job_name_remove_numbers": True,
                    "job_name_remove_num_brackets": True,
                    "job_name_remove_pattern": "",
                    "job_name_remove_text_within_parentheses": True,
                    "flavor_restriction": 0,
                    "flavor_default": "",
                    "flavor_ephemeral": True,
                    "flavor_gpu": 1,
                    "flavor_depth": 0,
                    "large_flavors": False,
                    "large_flavors_except_hmf": True,
                    "auto_activate_large_flavors": 10,
                    "drain_large_nodes": False,
                    "drain_only_hmf": False,
                    "drain_delay": 0,
                    "scheduler_settings": "PriorityType=priority/multifactor\nPriorityFavorSmall=NO"
                    "\nPriorityWeightJobSize=100000\nAccountingStorageTRES=cpu,mem,"
                    "gres/gpu\nPriorityWeightTRES=cpu=1000,mem=2000,gres/gpu=3000\n ",
                }
            },
        }
    }
    return config_example


def __setting_overwrite(mode_setting):
    """
    load config from yaml file
    test if all values are available
    """
    config_example = __example_configuration()
    initial_mode = config_example["scaling"]["active_mode"]
    yaml_config = read_config_yaml()
    if yaml_config is None:
        logger.error("missing configuration data, use -reset")
        yaml_config = config_example["scaling"]
    if mode_setting is None:
        if yaml_config and "active_mode" in yaml_config:
            mode_setting = yaml_config["active_mode"]
        else:
            mode_setting = config_example["scaling"]["active_mode"]
            logger.error("mode missing ... %s", mode_setting)
    logger.debug("loading mode settings ... %s", mode_setting)

    for key in config_example["scaling"]:
        if key != "mode" and key not in yaml_config:
            yaml_config[key] = config_example["scaling"][key]
            logger.debug("patch missing %s : %s", key, yaml_config[key])

    if initial_mode not in yaml_config["mode"]:
        yaml_config["mode"].update({initial_mode: {}})
    for key, val in config_example["scaling"]["mode"][initial_mode].items():
        # add missing keys to default setting
        if key not in yaml_config["mode"][initial_mode]:
            yaml_config["mode"][initial_mode][key] = val
            logger.debug("patch missing key %s in mode %s: %s", key, initial_mode, val)

    if mode_setting in yaml_config["mode"]:
        for key, val in yaml_config["mode"][initial_mode].items():
            # add missing keys to active mode setting
            if key not in yaml_config["mode"][mode_setting]:
                yaml_config["mode"][mode_setting][key] = val
                logger.debug("patch %s in mode %s: %s", key, mode_setting, val)

        sconf = yaml_config["mode"][mode_setting]
        if "scheduler_settings" in sconf:
            __update_playbook_scheduler_config(sconf["scheduler_settings"])
        else:
            logger.debug("mode %s without scheduler parameters", mode_setting)
    else:
        logger.error("unknown mode: %s", mode_setting)
        sys.exit(1)
    logger.debug(pformat(sconf))
    return sconf, yaml_config


def __cluster_scale_up_choice():
    """
    scale up cluster
        * flavor number and scale up value can be specified after the call
        * interactive flavor selection
    :return:
    """
    flavors_data = get_usable_flavors(False, False)
    upscale_sum = 0
    flavor_names = []

    time_now = __get_time()
    while True:
        flavor_num = input("flavor number: ")
        upscale_num = input("scale up number: ")
        if flavor_num.isnumeric() and upscale_num.isnumeric():
            try:

                flavor_num = int(flavor_num)
                upscale_num = int(upscale_num)
                if len(flavors_data) > flavor_num:
                    flavor_available_cnt = flavors_data[flavor_num]["usable_count"]
                    flavor_name = flavors_data[flavor_num]["flavor"]["name"]
                    if flavor_available_cnt >= upscale_num:
                        logger.info("scale up %s - %sx", flavor_name, upscale_num)
                        time.sleep(WAIT_CLUSTER_SCALING)
                        __cluster_scale_up_specific(flavor_name, upscale_num, False)
                        upscale_sum += upscale_num
                        flavor_names.append({flavor_name, upscale_num})
                    else:
                        logger.debug("wrong scale up number")
                else:
                    logger.debug("wrong flavor number")
            except (ValueError, IndexError):
                logger.debug("wrong values")
                sys.exit(1)
        flavor_next = input("start another flavor? y/n: ")
        logger.debug(flavor_next.startswith("y"))
        logger.debug(flavor_names)
        if not flavor_next.startswith("y"):
            break

    if flavor_names:
        rescale_cluster(upscale_sum)
        rescale_time = __get_time() - time_now
        logger.debug(
            "scale up by choice success: flavors %s, %s seconds",
            flavor_names,
            rescale_time,
        )


def __cluster_scale_down_choice_batch():
    """
    scale down cluster by worker batch id
    :return:
    """
    # check for string: "worker-{batch-id}-"
    worker_dict, _, _, _, _ = receive_node_data_db(True)
    scale_down_list = []
    search_batch = None
    if worker_dict:
        for index, (key, value) in enumerate(worker_dict.items()):
            if "worker" in key:
                batch_id = key.split("-")[2]
                logger.info(
                    "batch id: %s - key: %s - state: %s cpus %s real_memory %s",
                    batch_id,
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                )
        worker_num = input("enter worker batch id for scale-down: ")
        if worker_num.isnumeric():
            search_batch = "worker-" + worker_num + "-"
    if search_batch:
        for index, (key, value) in enumerate(worker_dict.items()):
            if search_batch in key:
                logger.info(
                    "index %s key: %s - state: %s cpus %s real_memory %s",
                    index,
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                )
                scale_down_list.append(key)

        logger.info("generated scale-down list: %s", scale_down_list)
        time.sleep(WAIT_CLUSTER_SCALING)

        return cluster_scale_down_specific_hostnames_list(
            scale_down_list, Rescale.CHECK
        )
    return False


def __cluster_scale_down_choice():
    """
    scale down specific workers
    select multiple worker by number
    :return:
    """
    worker_dict, _, _, _, _ = receive_node_data_db(True)
    ids = []
    scale_down_list = []
    if worker_dict:
        for index, (key, value) in enumerate(worker_dict.items()):
            if "worker" in key:
                logger.info(
                    "index %s key: %s - state: %s cpus %s real_memory %s",
                    index,
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                )
        while True:
            worker_num = input("enter worker batch id for scale-down: ")
            if not worker_num.isnumeric():
                break
            ids.append(int(worker_num))

        for index, (key, value) in enumerate(worker_dict.items()):
            if "worker" in key:
                if index in ids:
                    scale_down_list.append(key)
        logger.info("generated scale-down list: %s", scale_down_list)
        time.sleep(WAIT_CLUSTER_SCALING)

        return cluster_scale_down_specific_hostnames_list(
            scale_down_list, Rescale.CHECK
        )
    logger.debug("worker unavailable!")
    return False


def __worker_drain_choice():
    """
    Set to drain multiple workers
        - select workers by id
    :return:
    """
    worker_dict, _, _, _, _ = receive_node_data_db(True)
    ids = []
    drain_list = []
    if worker_dict:
        for index, (key, value) in enumerate(worker_dict.items()):
            if "worker" in key:
                logger.info(
                    "index %s key: %s - state: %s cpus %s real_memory %s",
                    index,
                    json.dumps(key),
                    json.dumps(value["state"]),
                    json.dumps(value["total_cpus"]),
                    json.dumps(value["real_memory"]),
                )
        while True:
            worker_num = input("worker index: ")
            if not worker_num.isnumeric():
                break
            ids.append(int(worker_num))

        for index, (key, value) in enumerate(worker_dict.items()):
            if "worker" in key:
                if index in ids:
                    drain_list.append(key)
        logger.info("generated drain list: %s", drain_list)
        for worker in drain_list:
            scheduler_interface.set_node_to_drain(worker)
        return True
    logger.debug("worker unavailable!")
    return False


def __run_as_service():
    """
    Start autoscaling as a service with
        - cluster rescaling
        - worker resource watcher process
        - logging process for graphic analysis
        - start service
    :return:
    """

    flavor_data = get_usable_flavors(True, True)
    rescale_cluster(-1)
    update_database(flavor_data)

    log_watch = Process(target=__log_watch, args=())
    log_watch.daemon = True
    log_watch.start()
    logger.debug("start service!!!")
    __service()


def __log_watch():
    global logger
    if LOG_LEVEL == logging.DEBUG:
        logger = setup_logger(AUTOSCALING_FOLDER + IDENTIFIER + "_csv.log")
    time_ = 0
    while True:
        try:
            time_ = time.time()
            __csv_log_entry("C", "0", "0")
            time_ = time.time() - time_
            logger.debug("created clock log entry in %s seconds", time_)
        except KeyboardInterrupt:
            return
        except BaseException as e:
            logger.error("unable to create clock log entry, except %s", e)
        time.sleep(60.0 - (time_ % 60.0))


def __remove_file(file_path):
    if os.path.isfile(file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            logger.debug(f"Error: {e.filename} - {e.strerror}.")


def __clean_log_data():
    as_home = AUTOSCALING_FOLDER + IDENTIFIER
    __remove_file(as_home + ".csv")
    __remove_file(as_home + ".log")
    __remove_file(as_home + "_csv.log")
    __remove_file(as_home + "_database.json")


def __service():
    """
    Start autoscaling as a service.
        - rescale cluster
        - run multiscale in a loop
        - sleep for a time defined in service_frequency
    :return:
    """
    wait_time = int(config_mode["service_frequency"])
    forecast_by_flavor_history = config_mode["forecast_by_flavor_history"]
    forecast_by_job_history = config_mode["forecast_by_job_history"]
    while True:
        logger.debug("=== INIT ===")
        check_config()
        flavor_data = get_usable_flavors(True, True)
        if flavor_data:

            rescale_init()
            if forecast_by_flavor_history or forecast_by_job_history:
                update_database(flavor_data)
            multiscale(flavor_data)
        logger.debug("=== SLEEP ===")
        time.sleep(wait_time)


def __get_time():
    """
    :return: current time in seconds
    """
    return int(
        str((datetime.datetime.now().replace(microsecond=0).timestamp())).split(
            ".", maxsplit=1
        )[0]
    )


def job_data_from_database(dict_db, flavor_name, job_name, multi_flavor):
    """
    return flavor based data from given job name
    - search selected flavor data first
    - if not found and multi_flavor, browse other flavors
    :param multi_flavor: search on multiple flavors
    :param dict_db: job database
    :param flavor_name: current flavor
    :param job_name: search for this job
    :return: dict_db
    """
    fv_name = __clear_flavor_name(flavor_name)
    job_match_value = float(config_mode["job_match_value"])
    logger.debug("search in history job %s, flavor %s", job_name, fv_name)
    if fv_name in dict_db["flavor_name"]:
        for current_job in dict_db["flavor_name"][fv_name]["similar_data"]:
            diff_match = difflib.SequenceMatcher(None, job_name, current_job).ratio()
            if diff_match > job_match_value:
                # found
                logger.debug(
                    "diff_match %s, job_name %s current_job %s",
                    diff_match,
                    job_name,
                    current_job,
                )
                return dict_db["flavor_name"][fv_name]["similar_data"][current_job]

    if multi_flavor:
        for flavor_tmp in dict_db["flavor_name"]:
            if flavor_tmp == fv_name:
                continue
            for current_job in dict_db["flavor_name"][flavor_tmp]["similar_data"]:
                diff_match = difflib.SequenceMatcher(
                    None, job_name, current_job
                ).ratio()
                if diff_match > job_match_value:
                    logger.debug(
                        "diff_match %s, job_name %s current_job %s other flavor %s",
                        diff_match,
                        job_name,
                        current_job,
                        flavor_tmp,
                    )
                    return dict_db["flavor_name"][flavor_tmp]["similar_data"][
                        current_job
                    ]
    return None


def get_dummy_worker():
    """
    Generate dummy worker entry from the highest flavor from available flavor data.
    The highest flavor shut be on top on index 0.

    Example:
        {'cores': 28, 'ephemeral_disk': 4000, 'ephemerals': [],
        'hostname': 'bibigrid-worker-autoscaling_dummy', 'ip': '0.0.0.4',
        'memory': 512000, 'status': 'ACTIVE'}
    :param flavors_data: available flavors
    :return: dummy worker data as json
    """
    flavors_data = get_usable_flavors(True, False)
    ephemeral_disk = 0
    max_gpu = 0
    max_memory = 0
    max_cpu = 0

    if flavors_data:
        # over all flavors - include gpu
        for flavor_tmp in flavors_data:
            ephemeral_disk = max(
                ephemeral_disk,
                convert_gb_to_mib(int(flavor_tmp["flavor"]["ephemeral_disk"])),
            )
            max_memory = max(
                max_memory, convert_gb_to_mib(flavor_tmp["flavor"]["ram_gib"])
            )
            max_cpu = max(max_cpu, flavor_tmp["flavor"]["vcpus"])
            max_gpu = max(max_gpu, int(flavor_tmp["flavor"]["gpu"]))
    else:
        logger.error("no flavor available")

    ephemerals = [{"size": ephemeral_disk, "device": "/dev/vdb", "mountpoint": "/mnt"}]
    dummy_worker = {
        "cores": max_cpu,
        "ephemeral_disk": ephemeral_disk,
        "ephemerals": ephemerals,
        "hostname": NODE_DUMMY,
        "ip": "0.0.0.4",
        "memory": max(max_memory, 8192),
        "status": "ACTIVE",
        "gpu": max_gpu,
    }
    logger.debug("dummy worker data: %s", dummy_worker)
    return dummy_worker


def flavor_data_from_database(dict_db, flavor_name):
    fv_name = __clear_flavor_name(flavor_name)
    if fv_name in dict_db["flavor_name"]:
        return dict_db["flavor_name"][fv_name]["fv_time_norm"]
    return None


def create_database(flavor_data):
    """
    Create initial dictionary.
    :return: dict_db
    """
    if flavor_data is None:
        logger.error("unable to receive flavor data")
        return None
    time_start = (
        __get_time() - __get_history_recall() * 86400
    )  # config_data['history_init']

    dict_db = {
        "VERSION": AUTOSCALING_VERSION,
        "config_hash": config_hash,
        "update_time": time_start,
        "flavor_name": {},
    }
    __save_file(DATABASE_FILE, dict_db)
    return dict_db


def remove_suffix(input_string, suffix):
    if suffix and input_string.endswith(suffix):
        return input_string[: -len(suffix)]
    return input_string


def __clear_flavor_name(fv_name):
    """
    Remove pattern from flavor name.
    For example "ephemeral", if the scheduler does not save tmp_disk data.
    :param fv_name: original flavor name
    :return: modified flavor name
    """
    if DATABASE_WORKER_PATTERN:
        return remove_suffix(fv_name, DATABASE_WORKER_PATTERN)
    return fv_name


def __smoothing_it(a_t, f_t, smoothing_coefficient):
    """
    exponential smoothing
    :param a_t:
    :param f_t:
    :param smoothing_coefficient: exponential smoothing coefficient
    :return: f_t+1
    """
    return float(smoothing_coefficient * a_t + (1 - smoothing_coefficient) * float(f_t))


def smooth_flavor_time(dict_db, current_flavor, smoothing_coefficient):
    """
    Calculate forecast with exponential smoothing.
    f_(t+1) = alpha * a_t + (1-alpha) * f_t
    :param smoothing_coefficient: exponential smoothing coefficient
    :param dict_db:
    :param current_flavor:
    :return: f_(t+1)
    """
    f_t1 = smooth_time(
        dict_db["flavor_name"][current_flavor]["fv_time_norm"],
        dict_db["flavor_name"][current_flavor]["fv_time_avg"],
        smoothing_coefficient,
    )
    logger.debug("exponential smoothing %s: f_t1 %s", current_flavor, f_t1)
    return f_t1


def smooth_time(f_t, current_time, smoothing_coefficient):
    """
    Calculate forecast with exponential smoothing.
    f_(t+1) = alpha * a_t + (1-alpha) * f_t
    :param smoothing_coefficient: exponential smoothing coefficient
    :param f_t:
    :param current_time:
    :return: f_(t+1)
    """

    a_t = __calc_job_time_norm(current_time, 1)
    f_t1 = __smoothing_it(a_t, f_t, smoothing_coefficient)
    return f_t1


def update_database(flavor_data):
    """
    Update database file.
    Integrate job data since "update_time" from database file.
    - flavor based version
    :param flavor_data:
    :return: dict_db
    """

    dict_db = None
    smoothing_coefficient = float(config_mode["smoothing_coefficient"])
    job_match_value = float(config_mode["job_match_value"])
    # database file exist
    if os.path.exists(DATABASE_FILE):
        dict_db = __get_file(DATABASE_FILE)
        # create new database if config file changed to avoid incompatible settings
        if dict_db and (
            dict_db["config_hash"] != config_hash
            or dict_db["VERSION"] != AUTOSCALING_VERSION
        ):
            logger.info("config file changed")
            if config_data["database_reset"]:
                logger.info("recreate database")
                dict_db = None

    if dict_db is None:
        dict_db = create_database(flavor_data)

    if flavor_data is None:
        logger.error("unable to receive flavor data")
        return dict_db
    current_time = __get_time()
    last_update_time = int(dict_db["update_time"])
    logger.debug("get jobs from %s to %s", int(dict_db["update_time"]), current_time)
    jobs_dict_new = receive_completed_job_data(
        1 + __division_round(current_time - last_update_time, 86400)
    )
    if not jobs_dict_new:
        return dict_db
    dict_db["update_time"] = current_time
    # add new sum and cnt values from jobs to dict
    for _, value in jobs_dict_new.items():
        if (
            value["state"] != JOB_FINISHED
            or int(value["end"]) < last_update_time
            or value["elapsed"] < 0
        ):
            continue
        job_name = __clear_job_name(value)
        v_tmp = translate_metrics_to_flavor(
            value["req_cpus"],
            value["req_mem"],
            value["temporary_disk"],
            flavor_data,
            False,
            True,
        )
        job_time_tmp = __calc_job_time_norm(value["elapsed"], 1)
        if v_tmp is not None:
            fv_name = __clear_flavor_name(v_tmp["flavor"]["name"])
            found = False
            # flavor related - global normalized value
            if fv_name not in dict_db["flavor_name"]:
                # if not exist init flavor
                logger.debug("init flavor in database %s", fv_name)
                dict_db["flavor_name"].update(
                    {
                        fv_name: {
                            "fv_time_norm": job_time_tmp,
                            "fv_time_avg": value["elapsed"],
                            "fv_time_sum": value["elapsed"],
                            "fv_time_cnt": 1,
                            "similar_data": {},
                        }
                    }
                )
            else:
                # update flavor related data
                dict_db["flavor_name"][fv_name]["fv_time_cnt"] += 1
                dict_db["flavor_name"][fv_name]["fv_time_sum"] += value["elapsed"]
                dict_db["flavor_name"][fv_name]["fv_time_avg"] = value["elapsed"]

                if smoothing_coefficient != 0:
                    fv_norm = smooth_flavor_time(
                        dict_db, fv_name, smoothing_coefficient
                    )
                else:
                    fv_norm = __calc_job_time_norm(
                        dict_db["flavor_name"][fv_name]["fv_time_sum"],
                        dict_db["flavor_name"][fv_name]["fv_time_cnt"],
                    )
                dict_db["flavor_name"][fv_name]["fv_time_norm"] = fv_norm

            # update job related similar job data
            for current_job in dict_db["flavor_name"][fv_name]["similar_data"]:
                # logger.debug("update database job %s for %s flavor ...", current_job, fv_name)
                diff_match = difflib.SequenceMatcher(
                    None, job_name, current_job
                ).ratio()
                if diff_match > job_match_value:
                    found = True
                    job_cnt = (
                        int(
                            dict_db["flavor_name"][fv_name]["similar_data"][
                                current_job
                            ]["job_cnt"]
                        )
                        + 1
                    )
                    job_sum = (
                        dict_db["flavor_name"][fv_name]["similar_data"][current_job][
                            "job_sum"
                        ]
                        + value["elapsed"]
                    )
                    if smoothing_coefficient != 0:
                        job_norm = smooth_time(
                            dict_db["flavor_name"][fv_name]["similar_data"][
                                current_job
                            ]["job_norm"],
                            value["elapsed"],
                            smoothing_coefficient,
                        )
                    else:
                        job_norm = __calc_job_time_norm(job_sum, job_cnt)
                    dict_db["flavor_name"][fv_name]["similar_data"][current_job] = {
                        "job_norm": job_norm,
                        "job_elapsed": value["elapsed"],
                        "job_sum": job_sum,
                        "job_cnt": job_cnt,
                    }

            if not found:
                logger.debug(
                    "init job %s in database, elapsed %s", job_name, value["elapsed"]
                )
                dict_db["flavor_name"][fv_name]["similar_data"].update(
                    {
                        job_name: {
                            "job_norm": job_time_tmp,
                            "job_elapsed": value["elapsed"],
                            "job_sum": value["elapsed"],
                            "job_cnt": 1,
                        }
                    }
                )
        else:
            logger.error("missing flavor for job %s classification!", job_name)

    __save_file(DATABASE_FILE, dict_db)
    return dict_db


def __clear_job_name(job_values):
    """
    With similar job search, we certainly do not want to compare raw job names.
    Numbers in brackets are usually an indication just a counter for the same or similar job.
    Optional, use comment field for job name as identifier.
    :param job_values: job values including 'jobname'
    :return: modified job identifier as string
    """

    if (
        config_data["pattern_id"]
        and config_data["pattern_id"] in job_values
        and job_values[config_data["pattern_id"]]
    ):
        job_name = str(job_values[config_data["pattern_id"]])
    else:
        job_name = job_values["jobname"]
        if config_mode["job_name_remove_num_brackets"]:
            job_name = re.sub(r"\(\d+\)", "", job_name)
        if config_mode["job_name_remove_numbers"]:
            job_name = re.sub(r"\d+", "", job_name)
        if config_mode["job_name_remove_pattern"]:
            job_name = re.sub(config_mode["job_name_remove_pattern"], "", job_name)
        if config_mode["job_name_remove_text_within_parentheses"]:
            job_name = re.sub(r"\([^()]*\)", "", job_name)

    return job_name


def version_check_scale_data(version):
    """
    Compare passed version and with own version data and initiate an update in case of mismatch.
    If the program does not run via systemd, the user must carry out the update himself.
    :param version: current version from cloud api
    :return:
    """
    if version != AUTOSCALING_VERSION:
        logger.warning(
            OUTDATED_SCRIPT_MSG.format(
                SCRIPT_VERSION=AUTOSCALING_VERSION, LATEST_VERSION=version
            )
        )
        automatic_update(latest_version=version)


def version_check(version):
    """
    Compare passed version and with own version data and initiate an update in case of mismatch.
    If the program does not run via systemd, the user must carry out the update himself.
    :param version: current version from cloud api
    :return:
    """
    if version != AUTOSCALING_VERSION:
        logger.warning(
            OUTDATED_SCRIPT_MSG.format(
                SCRIPT_VERSION=AUTOSCALING_VERSION, LATEST_VERSION=version
            )
        )
        automatic_update(latest_version=version)


def get_latest_release_tag():
    release_url = REPO_API_LINK + "releases/latest"
    response = requests.get(release_url)
    latest_release = response.json()
    logger.info(f"latest release: {latest_release}")

    latest_tag = latest_release["tag_name"]
    return latest_tag


def automatic_update(latest_version: None):
    """
    Download the program from sources if automatic update is active.
    Initiate update if started with systemd option otherwise just download the updated version.
    :return:
    """
    logger.info(f"Starting Script Autoupdate --> {latest_version}")
    if config_data["automatic_update"]:
        logger.warning("I try to upgrade!")
        if not systemd_start:
            download_autoscaling(latest_version=latest_version)
            sys.exit(1)
        update_autoscaling(latest_version=latest_version)

    else:
        sys.exit(1)


def update_autoscaling(latest_version=None):
    """
    Download current autoscaling version and restart systemd autoscaling service.
    :return:
    """
    logger.debug(f"update autoscaling --> {latest_version}")
    if download_autoscaling(latest_version=latest_version):
        logger.debug("Restart Service")
        restart_systemd_service()


def update_autoscaling_re():
    """
    Update autoscaling version.
    :return:
    """
    if download_autoscaling():
        restart_systemd_service_request()


def restart_systemd_service_request():
    """
    Restart autoscaling service with restart request.
    """
    result = input("restart autoscaling service? y/n: ")
    if result.startswith("y"):
        restart_systemd_service()


def restart_systemd_service():
    """
    Restart systemd service
    :return:
    """
    os.system("sudo systemctl restart autoscaling.service")
    status_response_systemd_service()


def status_systemd_service():
    """
    Autoscaling status from systemd service
    :return:
    """
    os.system("systemctl status autoscaling.service")


def status_response_systemd_service():
    """
    Print status response call from autoscaling service
    :return:
    """
    get_status = subprocess.Popen(
        "systemctl is-active autoscaling.service", shell=True, stdout=subprocess.PIPE
    ).stdout
    response = get_status.read().decode()
    logger.info("autoscaling service: ", response)


def stop_systemd_service():
    """
    Restart systemd service
    :return:
    """
    os.system("sudo systemctl stop autoscaling.service")
    status_response_systemd_service()


def kill_service():
    """
    Kill the autoscaling service.
    Remove pid file.
    """
    if os.path.exists(AUTOSCALING_FOLDER + FILE_PID):
        os.remove(AUTOSCALING_FOLDER + FILE_PID)
    os.system("pkill -fenv " + FILE_PROG)


def reset_autoscaling():
    """
    Reset settings and update autoscaling version.
    :return:
    """
    download_autoscaling()
    download_autoscaling_config()
    delete_database()
    delete_logs()
    if not os.path.exists(CLUSTER_PASSWORD_FILE):
        logger.info("cluster password file missing")
        __set_cluster_password()
    restart_systemd_service_request()


def delete_logs():
    """
    Remove log files.
    :return:
    """

    if os.path.exists(LOG_CSV):
        os.remove(LOG_CSV)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)


def delete_database():
    """
    Clear job dictionary to avoid possible incompatibilities from new versions.
    :return:
    """

    if os.path.exists(DATABASE_FILE):
        os.remove(DATABASE_FILE)
    else:
        logger.debug("no database file available for removal")


def download_autoscaling_config():
    """
    Download autoscaling configuration.
    This function will replace user settings to the latest default version.
    :return:
    """
    source_link = RAW_REPO_LINK + get_latest_release_tag() + "/" + FILE_CONFIG
    logger.warning(f"Downloading new config via url: - {source_link}")
    return update_file(FILE_CONFIG_YAML, source_link, FILE_CONFIG)


def update_file(file_location, url, filename):
    """
    Update a file from an url link.
    :param file_location: File location with file name.
    :param url: Complete link to the external source.
    :param filename: Filename.
    :return: boolean, updated
    """
    try:
        logger.debug("download new  %s", filename)
        res = requests.get(url, allow_redirects=True)
        if res.status_code == HTTP_CODE_OK:
            open(file_location, "wb").write(res.content)
            return True
        logger.error(
            "unable to update autoscaling %s from %s, status: %s",
            filename,
            url,
            res.status_code,
        )
    except requests.exceptions.HTTPError as err:
        logger.debug("error code: ", err.response.status_code)
        logger.debug(err.response.text)
    return False


def download_autoscaling(latest_version=None):
    """
    Download current version of the program.
    Use autoscaling url from configuration file.
    :return:
    """
    logging.debug(f"Download Autoscaling provided version: {latest_version}")
    if latest_version:
        logger.info(f"Latest Version provided for update: {latest_version}")
        VERSION = latest_version
    else:
        logger.info("Latest Version not provided, requesting from github")
        VERSION = get_latest_release_tag()
    source_link = RAW_REPO_LINK + VERSION + "/" + FILE_ID
    logger.warning(f"Downloading new script via url: - {source_link}")
    return update_file(FILE_PROG, source_link, FILE_ID)


def function_test():
    """
    Test main autoscaling functions.
        - run scheduler test
        - get cluster data
        - get flavor data
        - scale up worker with the lowest available flavor
        - scale down all idle worker
    :return: boolean, success
    """
    result_ = True
    logger.info("start autoscaling test")
    if not scheduler_interface.scheduler_function_test():
        result_ = False
        logger.error("scheduler interface problem")
        return result_

    logger.info("Scheduler interface working")

    if get_cluster_workers_from_api() is None:
        logger.error("unable to receive cluster data")
        result_ = False
        return result_

    logger.info("Cluster data recieved")
    fv_tmp = get_usable_flavors(False, True)
    if fv_tmp is not None:

        smallest_flavor = fv_tmp[-1]
        logger.info(f"recieved flavor data  -- smallest -> {smallest_flavor}")

    else:
        logger.error("unable to receive flavor data")
        result_ = False
        return result_
    logger.info("Flavor data recieved")

    logger.debug("test scale up %s", smallest_flavor)
    if not __cluster_scale_up_specific(smallest_flavor["flavor"]["name"], 1, True):
        logger.error("unable to scale up")
        result_ = False
        return result_

    logger.info("Scale up completed")

    if not __cluster_scale_down_complete():
        result_ = False
        logger.error("unable to scale down")
        return result_

    logger.info("Scale down completed")

    logger.info("function test result: %s", result_)

    return result_


def visualize_cluster_data(time_range):
    """
    Visualize csv log data from LOG_CSV.
    Create PDF inside the AUTOSCALING_FOLDER.
    :param time_range: time range, format "y-m-t-h:y-m-t-h"
    :return:
    """

    file_path = LOG_CSV

    if time_range and os.path.exists(time_range):
        file_path = time_range
        logger.debug(file_path)
    file_path_complete = file_path + "_" + "visual.pdf"
    logger.debug("save to ", file_path_complete)
    pdf = matplotlib.backends.backend_pdf.PdfPages(file_path + "_" + "visual.pdf")
    with open(file_path, newline="", encoding="utf8") as csv_file:
        my_data = pd.read_csv(csv_file, sep=",", on_bad_lines="warn")

    mem_scale = 1000000
    if time_range:
        if ":" in time_range:
            time_range = time_range.split(":")
            start_time = list(map(int, time_range[0].split("-")))
            start_time = calendar.timegm(
                datetime.datetime(
                    start_time[0], start_time[1], start_time[2], start_time[3], 0, 0
                ).timetuple()
            )
            stop_time = list(map(int, time_range[1].split("-")))
            stop_time = calendar.timegm(
                datetime.datetime(
                    stop_time[0], stop_time[1], stop_time[2], stop_time[3], 0, 0
                ).timetuple()
            )
            logger.debug(
                "requested: \nstart_time ",
                start_time,
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time)),
            )
            logger.debug(
                "stop_time ",
                stop_time,
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stop_time)),
            )
            logger.debug(
                "available: \nstart_time",
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(my_data.time[0])),
            )
            logger.debug(
                "stop_time",
                time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(my_data.time[my_data.index[-1]])
                ),
            )
            my_data = my_data[(my_data.time > start_time) & (my_data.time < stop_time)]

    if my_data.empty:
        logger.debug("no data in this time range")
        return
    my_data["date"] = my_data["time"].astype("datetime64[s]")
    my_data["alloc"] = my_data["worker_mem_alloc"] / mem_scale
    my_data["alloc_drain"] = my_data["worker_mem_alloc_drain"] / mem_scale

    my_data["mix"] = my_data["worker_mem_mix"] / mem_scale
    my_data["mix_drain"] = my_data["worker_mem_mix_drain"] / mem_scale
    my_data["idle_drain"] = my_data["worker_mem_idle_drain"] / mem_scale
    my_data["idle"] = my_data["worker_mem_idle"] / mem_scale

    my_data["job_alloc"] = my_data["jobs_mem_alloc"] / mem_scale
    my_data["job_pending"] = my_data["jobs_mem_pending"] / mem_scale

    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)

    df = pd.DataFrame(my_data)

    data_count = my_data.size

    logger.debug("data_count: ", data_count, " ")
    data_count = data_count / 3600
    logger.debug("data_count: ", data_count)
    width_ = max(data_count * 5, 10)
    fig, ax = plt.subplots(figsize=(width_, 5))

    bottom = 0
    widths = np.diff(df["date"])
    widths = np.append(widths, widths[-1])
    names = ["allocated", "allocated + drain", "mix", "mix+drain", "idle", "idle+drain"]
    cnt = 0
    for col in ["alloc", "alloc_drain", "mix", "mix_drain", "idle", "idle_drain"]:
        if not df[col].eq(0.0).all(axis=0):
            ax.bar(
                df["date"],
                df[col],
                bottom=bottom,
                width=widths,
                align="edge",
                alpha=0.4,
                label=names[cnt],
                color=plt.cm.tab10.colors[cnt],
            )
            bottom += df[col]
        cnt += 1

    scale = my_data["worker_mem_alloc"].max() / mem_scale
    df_jy = df[["job_alloc", "job_pending"]]
    dfmark_a = df.loc[df["scale"] == "A"].copy()
    dfmark_u = df.loc[df["scale"] == "U"].copy()
    dfmark_d = df.loc[df["scale"] == "D"].copy()
    dfmark_m = df.loc[df["scale"] == "M"].copy()
    dfmark_e = df.loc[df["scale"] == "E"].copy()
    dfmark_y = df.loc[df["scale"] == "Y"].copy()
    dfmark_x = df.loc[df["scale"] == "X"].copy()
    dfmark_i = df.loc[df["scale"] == "I"].copy()
    dfmark_l = df.loc[df["scale"] == "L"].copy()

    dfmark_u["scale"] = dfmark_u["scale"].map({"U": scale / 4})
    dfmark_a["scale"] = dfmark_a["scale"].map({"A": scale / 5})
    dfmark_d["scale"] = dfmark_d["scale"].map({"D": scale / 6})
    dfmark_m["scale"] = dfmark_m["scale"].map({"M": scale / 7})
    dfmark_e["scale"] = dfmark_e["scale"].map({"E": scale / 8})
    dfmark_y["scale"] = dfmark_y["scale"].map({"Y": scale / 9})
    dfmark_x["scale"] = dfmark_x["scale"].map({"X": scale / 6})
    dfmark_i["scale"] = dfmark_i["scale"].map({"I": scale / 3})
    dfmark_l["scale"] = dfmark_l["scale"].map({"L": scale / 9})

    if not df_jy.empty:
        plt.plot(
            df["date"],
            df_jy["job_alloc"],
            linestyle="solid",
            color="blue",
            alpha=0.4,
            label="allocated by jobs",
        )
    if not dfmark_a.empty:
        plt.plot(
            dfmark_a["date"],
            dfmark_a["scale"],
            marker="p",
            linestyle="None",
            label="rescale",
            color="blue",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_u.empty:
        plt.plot(
            dfmark_u["date"],
            dfmark_u["scale"],
            marker="^",
            linestyle="None",
            label="scale-up",
            color="green",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_d.empty:
        plt.plot(
            dfmark_d["date"],
            dfmark_d["scale"],
            marker="v",
            linestyle="None",
            label="scale-down",
            color="orange",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_m.empty:
        plt.plot(
            dfmark_m["date"],
            dfmark_m["scale"],
            marker="1",
            linestyle="None",
            label="autoscale",
            color="black",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_e.empty:
        plt.plot(
            dfmark_e["date"],
            dfmark_e["scale"],
            marker="X",
            linestyle="None",
            label="error",
            color="red",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_y.empty:
        plt.plot(
            dfmark_y["date"],
            dfmark_y["scale"],
            marker="d",
            linestyle="None",
            label="scale-down drain",
            color="blue",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_x.empty:
        plt.plot(
            dfmark_x["date"],
            dfmark_x["scale"],
            marker="o",
            linestyle="None",
            label="reactivate worker",
            color="orange",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_i.empty:
        plt.plot(
            dfmark_i["date"],
            dfmark_i["scale"],
            marker=">",
            linestyle="None",
            label="scale-up (api)",
            color="green",
            alpha=0.4,
            markersize=4,
        )
    if not dfmark_l.empty:
        plt.plot(
            dfmark_l["date"],
            dfmark_l["scale"],
            marker="D",
            linestyle="None",
            label="insufficient resources",
            color="red",
            alpha=0.4,
            markersize=4,
        )

    ax.set_xlabel("time")
    ax.set_ylabel("memory resources (TB)")
    ax.set_title("Setting: " + str(config_data["active_mode"]), fontsize=15)

    ax.margins(x=0.01)
    plt.grid()
    plt.xticks(rotation=45)
    ax.legend(loc="upper left")
    pdf.savefig(fig, bbox_inches="tight")

    df = pd.DataFrame(my_data).fillna(0)
    fig, ax = plt.subplots(figsize=(width_, 5))
    bottom = 0
    names = ["pending jobs"]
    cnt = 0
    ax = plt.subplot()
    for col in ["job_cnt_pending"]:
        ax.bar(
            df["date"], df[col], width=widths, align="edge", alpha=0.4, label=names[cnt]
        )
        bottom += df[col]
        cnt += 1
    ax.set_xlabel("time")
    ax.set_ylabel("number of pending jobs")
    ax.margins(x=0.01)
    plt.xticks(rotation=45)
    plt.grid()
    ax.legend(loc="upper left")
    pdf.savefig(fig, bbox_inches="tight")
    pdf.close()


def __get_history_recall():
    """
    Read history recall value.
    :return: number of days
    """
    if config_data["history_recall"]:
        return config_data["history_recall"]
    return DATA_LONG_TIME


def __get_time_threshold(flavor):
    """
    Return time threshold from configuration.
    :param flavor: flavor name
    :return: threshold
    """
    if "flavor_time_threshold" in config_mode:
        if flavor in config_mode["flavor_time_threshold"]:
            return float(config_mode["flavor_time_threshold"][flavor])
    return float(config_mode["job_time_threshold"])


def select_mode():
    """
    Change mode on terminal.
        1. Load modes from yaml configuration file.
        2. Select active mode, by mode id.
        3. Ask for autoscaling service restart.
    :return:
    """
    modes_available = []
    try:
        yaml_config = __read_yaml_file(FILE_CONFIG_YAML)
        mode_active = yaml_config["scaling"]["active_mode"]
        for mode in yaml_config["scaling"]["mode"]:
            modes_available.append(mode)

            if "info" in yaml_config["scaling"]["mode"][mode]:
                info = yaml_config["scaling"]["mode"][mode]["info"]
                logger.debug(
                    f"id: {modes_available.index(mode):<2d}, {mode:<20s}: {info:>1s}"
                )
            else:
                logger.debug(f"id: {modes_available.index(mode):<2d}, {mode:<20s}")

        logger.debug(
            "current active mode: id {i}, {m}".format(
                i=modes_available.index(mode_active), m=mode_active
            )
        )
        mode_id = input("enter mode id: ")
        if mode_id.isnumeric() and modes_available:
            mode_id = int(mode_id)
            if len(modes_available) > mode_id:
                yaml_config["scaling"]["active_mode"] = modes_available[mode_id]
                ___write_yaml_file(FILE_CONFIG_YAML, yaml_config)
                restart_systemd_service_request()
                return
        logger.debug(f"missing mode {mode_id}")
    except TypeError:
        logger.debug("broken config, use -reset")


def __select_ignore_workers():
    yaml_config = __read_yaml_file(FILE_CONFIG_YAML)
    if not yaml_config:
        return
    workers_ignore = []
    if "ignore_workers" in yaml_config["scaling"]:
        workers_ignore = yaml_config["scaling"]["ignore_workers"]
    logger.debug("current ignored workers:", workers_ignore)

    node_dict = receive_node_data_live_uncut()

    logger.debug("ignore_workers: %s, node_dict %s", workers_ignore, node_dict.keys())
    while True:
        for i, k in enumerate(node_dict):
            logger.debug(i, k)
        worker_id = input("worker id (use: 'quit' & 'clear'): ")
        logger.debug("-" + worker_id + "-")
        if worker_id == "quit":
            break
        if worker_id == "clear":
            workers_ignore = []
            continue
        if not worker_id.isnumeric():
            continue
        for i, k in enumerate(node_dict):
            worker_id = int(worker_id)
            if worker_id == i and k != NODE_DUMMY:
                logger.debug(i, k)
                workers_ignore.append(k)
                break
        logger.debug("-----")
        logger.debug(workers_ignore)
    yaml_config["scaling"].update({"ignore_workers": workers_ignore})
    ___write_yaml_file(FILE_CONFIG_YAML, yaml_config)
    restart_systemd_service_request()


if __name__ == "__main__":
    logger = setup_logger(LOG_FILE)
    scheduler_interface = None
    systemd_start = False

    if len(sys.argv) == 2:
        if sys.argv[1] in ["-v", "--v", "-version", "--version"]:
            get_version()
            sys.exit(0)
        elif sys.argv[1] in ["-h", "--h", "-help", "--help"]:
            __print_help()
            __print_help_debug()
            sys.exit(0)
        elif sys.argv[1] in ["-p", "--p", "-password", "--password"]:
            __set_cluster_password()
            sys.exit(0)
        elif sys.argv[1] in ["-l", "--l", "-log", "--log"]:
            # temp csv log
            if LOG_LEVEL == logging.DEBUG:
                logger = setup_logger(AUTOSCALING_FOLDER + IDENTIFIER + "_csv.log")
        elif sys.argv[1] in ["-stop", "--stop"]:
            stop_systemd_service()
            sys.exit(0)
        elif sys.argv[1] in ["-kill", "--kill"]:
            kill_service()
            sys.exit(0)
        elif sys.argv[1] in ["-status", "--status"]:
            status_systemd_service()
            sys.exit(0)
        elif sys.argv[1] in ["-clean", "--clean"]:
            __clean_log_data()
            sys.exit(0)

    if len(sys.argv) == 2:
        if sys.argv[1] in ["-reset", "--reset"]:
            reset_autoscaling()
            sys.exit(0)
        elif sys.argv[1] in ["-start", "--start"]:
            __start_service()
            sys.exit(0)
        elif sys.argv[1] in ["-u", "-update", "--update"]:
            update_autoscaling_re()
            sys.exit(0)
    config_mode, config_data = __setting_overwrite(None)
    config_hash = generate_hash(FILE_CONFIG_YAML)
    scheduler = config_data["scheduler"]
    cluster_id = read_cluster_id()
    # load scheduler interface
    if scheduler == "slurm":
        logger.info("scheduler selected: %s", scheduler)
        import pyslurm

        scheduler_interface = SlurmInterface()
    if not scheduler_interface:
        logger.error("scheduler interface not available: %s", scheduler)
        sys.exit(1)

    if len(sys.argv) == 2:
        if sys.argv[1] in ["-l", "--l", "-log", "--log"]:
            __csv_log_entry("C", "0", "0")
            sys.exit(0)
        elif sys.argv[1] in ["-rsc", "--rsc", "-rescale", "--rescale"]:
            logger.debug("rescale cluster configuration")
            rescale_init()
            sys.exit(0)
        elif sys.argv[1] in ["-nd", "--nd", "-node", "--node"]:
            receive_node_data_live()
            receive_node_data_db(False)
            sys.exit(0)
        elif sys.argv[1] in ["-m", "--m", "-mode", "--mode"]:
            select_mode()
            sys.exit(0)
        elif sys.argv[1] in ["-i", "--i", "-ignore", "--ignore"]:
            __select_ignore_workers()
            sys.exit(0)
        elif sys.argv[1] in ["-j", "--j", "-jobdata", "--jobdata"]:
            pj, rj = receive_job_data()
            logger.debug(__sort_jobs(pj))
            logger.debug(__sort_jobs(rj))
            sys.exit(0)
        elif sys.argv[1] in ["-jh", "--jh", "-jobhistory", "--jobhistory"]:
            print_job_history(receive_completed_job_data(__get_history_recall()))
            sys.exit(0)
        elif sys.argv[1] in ["-fv", "--fv", "-flavor", "--flavor"]:
            logger.debug("flavors available:")
            fv_info = get_usable_flavors(False, True)
            if fv_info and LOG_LEVEL == logging.DEBUG:
                logger.debug(pformat(fv_info))
            sys.exit(0)
        elif sys.argv[1] in ["-c", "--c", "-clusterdata", "--clusterdata"]:
            logger.debug(get_cluster_workers_from_api())
            sys.exit(0)
        elif sys.argv[1] in ["-visual", "--visual"]:
            visualize_cluster_data(None)
            sys.exit(0)
        elif sys.argv[1] in ["-d", "--d", "-drain", "--drain"]:
            __worker_drain_choice()
            sys.exit(0)
    elif len(sys.argv) == 3:
        if sys.argv[1] in ["-visual", "--visual"]:
            visualize_cluster_data(sys.argv[2])
            sys.exit(0)

    pidfile = create_pid_file()

    try:
        logger.info(
            "\n#############################################"
            "\n########### START SCALING PROGRAM ###########"
            "\n#############################################"
        )

        systemd_start = False
        if len(sys.argv) > 1:
            arg = sys.argv[1]
            logger.debug("autoscaling with %s: ", " ".join(sys.argv))
            if len(sys.argv) == 2:
                if arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    logger.debug("scale-up")
                    __cluster_scale_up_test(1)
                elif arg in ["-suc", "--suc", "-scaleupchoice", "--scaleupchoice"]:
                    __cluster_scale_up_choice()
                elif arg in ["-sdc", "--sdc", "-scaledownchoice", "--scaledownchoice"]:
                    __cluster_scale_down_choice()
                elif arg in ["-sdb", "--sdb", "-scaledownbatch", "--scaledownbatch"]:
                    __cluster_scale_down_choice_batch()
                elif arg in ["-sd", "--sd", "-scaledown", "--scaledown"]:
                    logger.debug("scale-down")
                    __cluster_scale_down_idle()
                elif arg in ["-sdi", "--sdi", "-scaledownidle", "--scaledownidle"]:
                    logger.warning(
                        "warning: scale down in %s sec ...", WAIT_CLUSTER_SCALING
                    )
                    time.sleep(WAIT_CLUSTER_SCALING)
                    __cluster_scale_down_complete()
                elif arg in ["-csd", "--csd", "-clustershutdown", "--clustershutdown"]:
                    logger.warning(
                        "warning: delete all worker from cluster in %s sec ...",
                        WAIT_CLUSTER_SCALING,
                    )
                    time.sleep(WAIT_CLUSTER_SCALING)
                    __cluster_shut_down()
                elif arg in ["-cdb", "--cdb", "-clusterbroken", "--clusterbroken"]:
                    logger.warning(
                        "warning: delete all broken worker from cluster in %s sec ...",
                        WAIT_CLUSTER_SCALING,
                    )
                    time.sleep(WAIT_CLUSTER_SCALING)
                    worker_j, _, _, _, _ = receive_node_data_db(False)
                    __verify_cluster_workers(get_cluster_workers_from_api(), worker_j)
                elif arg in ["-pb", "--pb", "-playbook", "--playbook"]:
                    logger.debug("run_ansible_playbook")
                    run_ansible_playbook()
                elif arg in ["-cw", "--cw", "-checkworker", "--checkworker"]:
                    check_workers(Rescale.INIT, 0)
                elif arg in ["-s", "-service", "--service"]:
                    __run_as_service()
                elif arg in ["-systemd", "--systemd"]:
                    systemd_start = True
                    logger.debug("... start autoscaling with systemd")
                    __run_as_service()
                elif arg in ["-t", "-test", "--test"]:
                    if not function_test():
                        sys.exit(1)
                else:
                    logger.debug("No usage found for param: ", arg)
                sys.exit(0)
            elif len(sys.argv) == 3:
                arg2 = sys.argv[2]
                if arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    logger.debug("scale-up")
                    if arg2.isnumeric():
                        __cluster_scale_up_test(int(arg2))
                    else:
                        logger.error("we need a number, for example: -su 2")
                        sys.exit(1)
                elif arg in [
                    "-sds",
                    "--sds",
                    "-scaledownspecific",
                    "--scaledownspecific",
                ]:
                    logger.debug("scale-down-specific")
                    if "worker" in arg2:
                        cluster_scale_down_specific_hostnames(arg2, Rescale.CHECK)
                    else:
                        logger.error("hostname parameter without 'worker'")
                        sys.exit(1)
                else:
                    logger.debug("No usage found for param: ", arg, arg2)
                    sys.exit(1)
                sys.exit(0)
            elif len(sys.argv) == 4:
                if arg in ["-sus", "--sus", "-scaleupspecific", "--scaleupspecific"]:
                    logger.debug("scale-up")
                    if sys.argv[3].isnumeric():
                        __cluster_scale_up_specific(sys.argv[2], int(sys.argv[3]), True)
        else:
            logger.debug("\npass a mode via parameter, use -h for help")
            rescale_cluster(-1)
    finally:
        os.unlink(pidfile)
    sys.exit(0)
