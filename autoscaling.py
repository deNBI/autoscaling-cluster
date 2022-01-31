#!/usr/bin/python3

import difflib
import urllib.request
import os
import time
import sys
import pyslurm
import datetime
import json
import requests
import enum
import logging
import inspect
import textwrap
import yaml
from pathlib import Path
from pprint import pformat, pprint
from functools import total_ordering

SIMULATION = False
VERSION = "0.3.0"  # for testing purposes only
PORTAL_AUTH = False

# ----- SYSTEM PARAMETERS -----
# define server endpoint
AUTOSCALING_FOLDER = os.path.dirname(os.path.realpath(__file__)) + '/'
IDENTIFIER = "autoscaling"

FILE_BUP_NODE = AUTOSCALING_FOLDER + 'backup_node.json'
FILE_BUP_JOBS = AUTOSCALING_FOLDER + 'backup_jobs.json'

FILE_CONFIG_YAML = AUTOSCALING_FOLDER + 'autoscaling_config.yaml'
FILE_PORTAL_AUTH = AUTOSCALING_FOLDER + 'portal_auth.json'
FILE_PID = IDENTIFIER + ".pid"

PORTAL_LINK = 'https://cloud.denbi.de'
PORTAL_URL = PORTAL_LINK + '/portal/public'
PORTAL_URL_SCALING = PORTAL_URL + '/clusters_scaling/'
CLUSTER_PASSWORD_FILE = AUTOSCALING_FOLDER + 'cluster_pw.json'  # {"password":"PASSWORD"}
SCALING_URL = 'https://raw.githubusercontent.com/deNBI/user_scripts/master/bibigrid/scaling.py'
FILE_SCALING = 'scaling_v0.3_mod.py'

MEMORY_SYSTEM_BUFFER = 0  # memory system (MB)

LOG_LEVEL = logging.DEBUG
LOG_FILE = AUTOSCALING_FOLDER + IDENTIFIER + '.log'

# ----- SCHEDULER PARAMETERS -----
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_IDLE = "IDLE"

# ----- MODE PARAMETERS -----
SORT_JOBS_BY_MEMORY = True
SCALE_DELAY_WAIT = 2  # wait and recheck slurm values - SCALE_DELAY

SCALE_FORCE = 0.8  # higher - higher downscaling
SCALE_FORCE_WORKER = True
SCALE_FORCE_WORKER_WEIGHT = 0.05

WAIT_CLUSTER_SCALING_UP = 5  # wait seconds
WAIT_CLUSTER_SCALING_DOWN = 10

LIMIT_DOWNSCALE = 1
LIMIT_UPSCALE = 2  # not in use

JOB_TIME_LONG = 600  # anything above this value (in seconds) should be a time-consuming job - long_job_time
JOB_TIME_SHORT = 160
JOB_TIME_RANGE_AUTOMATIC = True  # ignore constant job time values

JOB_MATCH_VALUE = 0.9  # include only jobs in calculation with SequenceMatcher
JOB_MATCH_SIMILAR = 0.4  # lower value -tend to start new workers, although a similar job took comparatively little time
JOB_TIME_SIMILAR_SEARCH = True

FLAVOR_FIX = False  # if true, we use the default flavor
FLAVOR_DEFAULT = "de.NBI large + ephemeral"  # "de.NBI default" # default flavor ("de.NBI tiny","de.NBI default" ...)
FLAVOR_EPHEMERAL = True  # only use ephemeral flavors


@total_ordering
class ScaleState(enum.Enum):
    SCALE_UP = 1
    SCALE_DOWN = 0
    SCALE_SKIP = 2
    SCALE_DONE = 3
    SCALE_DOWN_UP = 4
    SCALE_DELAY = -1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


def get_version():
    print("Version: ", VERSION)


def __fetch_scheduler_node_data():
    """
    read scheduler data from api
    return a json object with node data
    replace this function to use other scheduler

    define the prerequisites for the json data structure and the required data
    :return json object expected, example:

    {
        'bibigrid-worker-3-1-gpwapcgoqhgkctt':
            {
                'cpus': 2,
                'real_memory': 2000,
                'state': 'IDLE',
                'tmp_disk': 0,
                'tres_fmt_str': 'cpu=2,mem=2000M,billing=2',
                ...
             },
        ...
    }
    """
    try:
        nodes = pyslurm.node()
        node_dict = nodes.get()

        return node_dict
    except ValueError as e:
        logger.error("Error: unable to receive node data \n%s", e)

        # TODO BUG? if zero workers available, try scale-up?
        global cid, cpw
        cluster_data = get_cluster_data(cid, cpw)
        available_cluster = len(dict(cluster_data))
        if available_cluster == 0:
            logger.error("%s workers found, try scale up \n", available_cluster)
            time.sleep(WAIT_CLUSTER_SCALING_UP)
            __cluster_scale_up_test(cid, cpw, 1)


def __fetch_scheduler_job_data(num_days):
    """
    read scheduler data from api
    return a json object with job data
    replace this function to use other scheduler

    define the prerequisites for the json data structure and the required data
    :param number of days
    :return json object expected, example:
    {
    20:
        {
        ...
        },
    33:
        {
            'alloc_nodes': 1,
            'cluster': 'bibigrid',
            'elapsed': 68,
            'eligible': 1643383328,
            'end': 1643383396,
            'jobid': 33,
            'jobname': 'nf-sayHello_(6)',
            'nodes': 'bibigrid-worker-3-1-gpwapcgoqhgkctt',
            'partition': 'debug',
            'priority': 71946,
            'req_cpus': 1,
            'req_mem': 5,
            'start': 1643383328,
            'state': 7,
            'state_str': 'NODE_FAIL',
            ...
        }
    }

    :return:
    """
    try:
        start = (datetime.datetime.utcnow() - datetime.timedelta(days=num_days)).strftime("%Y-%m-%dT00:00:00")
        end = (datetime.datetime.utcnow() + datetime.timedelta(days=num_days)).strftime("%Y-%m-%dT00:00:00")

        jobs = pyslurm.slurmdb_jobs()
        jobs_dict = jobs.get(starttime=start.encode("utf-8"), endtime=end.encode("utf-8"))
        return jobs_dict
    except ValueError as e:
        print("Error: unable to receive job data \n%s", e)
        sys.exit(1)


def read_config_yaml():
    """
    TODO read config from yaml file
    :return:
    """

    with open(FILE_CONFIG_YAML, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            logger.info("Load config from yaml file %s", FILE_CONFIG_YAML)
            if not config:
                logger.error("missing yaml config")
                return None
            logger.info("\n %s\n", pformat(config))

            logger.debug("mode adoptive %s", pformat(config['scaling']['mode']['adoptive']))
            return config['scaling']
        except yaml.YAMLError as exc:
            logger.error(exc)
            sys.exit(1)


def read_cluster_id():
    """
    read cluster id from hostname
    example hostname: bibigrid-master-clusterID
    :return: cluster id
    """

    with open('/etc/hostname', 'r') as file:
        try:
            cluster_id = file.read().rstrip().split('-')[2]
        except (ValueError, IndexError):
            logger.error("error: wrong cluster name \nexample: bibigrid-master-clusterID")
            sys.exit(1)
    return cluster_id


def get_url_scale_up(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale up url
    """
    return PORTAL_URL_SCALING + cluster_id + "/scale-up/"


def get_url_scale_down(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale down url
    """
    return PORTAL_URL_SCALING + cluster_id + "/scale-down/"


def get_url_scale_down_specific(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale down specific url
    """
    return PORTAL_URL_SCALING + cluster_id + "/scale-down/specific/"


def get_url_info_cluster(cluster_id):
    """
    :param cluster_id:
    :return: return portal api info url
    """
    return PORTAL_URL + '/clusters/' + cluster_id + '/'


def get_url_info_flavors(cluster_id):
    """
    :param cluster_id:
    :return: return portal api flavor info url
    """
    return PORTAL_URL + '/clusters_scaling/' + cluster_id + '/usable_flavors/'


def __reduce_flavor_memory(mem_gb):
    """
    receive raw memory in GB and reduce this to a usable value
    memory reduces to os requirements and other circumstances
    :param mem_gb: memory (raw gb value)
    :return: memory reduced value (mb)
    """
    gb2_mb = 1000  # 1024
    mem_low_border = 16 * gb2_mb
    mem_low_reserve = 512

    mem_high_gb = 64
    mem_high_border = mem_high_gb * gb2_mb

    mem_multiplier = 62.5
    mem_mb = mem_gb * gb2_mb

    if mem_low_border < mem_mb < mem_high_border:
        return mem_mb - (mem_gb * mem_multiplier)
    elif mem_mb > mem_high_border:
        return mem_high_border - (mem_high_gb * mem_multiplier)
    return mem_mb - mem_low_reserve


def __translate_cpu_mem_to_flavor(cpu, mem, flavors_data):
    """
    Select the flavor by cpu and memory.
    In the future, the server could select the flavor itself based on the required CPU and memory data.
    - "de.NBI tiny"     1 Core  , 2  GB Ram
    - "de.NBI mini"     4 Cores , 7  GB Ram
    - "de.NBI small"    8 Cores , 16 GB Ram
    - "de.NBI medium"  14 Cores , 32 GB Ram
    - "de.NBI large"   28 Cores , 64 GB Ram
    - "de.NBI large + ephemeral" 28 Cores , 64 GB Ram
    if ephemeral required, only ephemeral flavor possible
    check if flavor is available - if not, test the next higher
    :param cpu: required cpu value for job
    :param mem: required memory value for job (mb)
    :param flavors_data: flavor data (json) from cluster api
    :return: matched and available flavor
    """

    mem = (mem + MEMORY_SYSTEM_BUFFER)  # TODO remove (__reduce_flavor_memory)
    logger.debug(">>>>>>>>>>>>>> %s >>>>>>>>>>>>>>", inspect.stack()[0][3])
    logger.debug("searching for cpu %d mem %d", cpu, mem)

    step_over_flavor = False

    for fd in reversed(flavors_data):
        # logger.debug("flavor: %s -- ram: %s -- cpu: %s -- available: %sx", fd['flavor']['name'],
        #              fd['flavor']['ram'] * gb2_mb, fd['flavor']['vcpus'], fd['available_count'])

        if "ephemeral" not in fd['flavor']['name'] and FLAVOR_EPHEMERAL:
            # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", FLAVOR_EPHEMERAL)
            continue
        elif "ephemeral" in fd['flavor']['name'] and not FLAVOR_EPHEMERAL:
            # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", FLAVOR_EPHEMERAL)
            continue

        logger.debug("flavor: %s -- ram: %s -- cpu: %s -- available: %sx", fd['flavor']['name'],
                     __reduce_flavor_memory(fd['flavor']['ram']), fd['flavor']['vcpus'], fd['available_count'])

        if cpu <= fd['flavor']['vcpus'] and mem <= __reduce_flavor_memory(fd['flavor']['ram']):
            if fd['available_count'] > 0:
                logger.debug("-> match found %s", fd['flavor']['name'])
                return fd['flavor']['name']
            elif not step_over_flavor:
                # if we have already passed a suitable flavor, try only the next higher flavor
                step_over_flavor = True
                logger.debug("step_over_flavor %s", step_over_flavor)
            else:
                break

    logger.debug("unable to find a suitable flavor!")
    return None


def __job_print(job_data_list, name):
    """
    Print job list to debug log.
    :param job_data_list: job items
    :param name: print header name
    :return:
    """
    for key, value in job_data_list:
        logger.debug("job: %s prio: %s mem: %s cpu: %s time: %s state: %s",
                     value['jobid'], value['priority'], value['req_mem'],
                     value['req_cpus'], value['elapsed'], value['state_str'])


def __sort_job_priority(jobs_dict):
    """
    sort jobs by priority
    - priority should be calculated by scheduler
    - optional:
      - sort memory as secondary condition (optional, SORT_JOBS_BY_MEMORY)
    return: job data, jobs with high priority at the top
    :param jobs_dict: jobs as json dictionary object
    :return: job list sorted by priority
    """

    if SORT_JOBS_BY_MEMORY:
        # sort with secondary condition:
        priority_job_list = sorted(jobs_dict.items(), key=lambda k: (k[1]['priority'], k[1]['req_mem']), reverse=True)
    else:
        priority_job_list = sorted(jobs_dict.items(), key=lambda k: k[1]['priority'], reverse=True)

    return priority_job_list


def receive_node_data():
    """
    query the job data via pyslurm and return the json object
    including the number of workers and how many are currently in use
    return node_dict, worker_count, worker_in_use
    sinfo --Node
    :return:
        worker_json: worker information as json dictionary object
        worker_count: number of available worker's
        worker_in_use: number of workers in use
    """
    worker_in_use = 0
    worker_count = 0
    node_dict = __fetch_scheduler_node_data()
    if node_dict:
        for key, value in node_dict.items():
            logger.debug("key: %s - value: %s", json.dumps(key), json.dumps(value['state']))
            if 'worker' in key:
                worker_count += 1
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    worker_in_use += 1
                elif ('DRAIN' in value['state'] or 'DOWN' in value['state']) and 'IDLE' not in value['state']:
                    logger.error("workers are in DRAIN or DOWN state")
                    sys.exit(1)
        logger.info("nodes: I found %d worker, %d are ALLOCATED", worker_count, worker_in_use)
    else:
        logger.info("No nodes found !")
    if LOG_LEVEL == logging.DEBUG:
        __save_file(FILE_BUP_NODE, node_dict)
    return node_dict, worker_count, worker_in_use


def receive_job_data():
    """
    capture all jobs from the last 24h and return the json object
    including the number of pending jobs

    since slurm version 21.08, a direct json output is available, possible usage in future versions
    job state:
        0: PENDING
        1: RUNNING
        2:
        3: COMPLETED
    squeue -o "%A %C %m %Q %N"
    :return:
        jobs_dict: jobs as json dictionary object
        jobs_pending: number of pending jobs
    """
    jobs_pending = 0

    jobs_dict = __fetch_scheduler_job_data(1)

    if jobs_dict:
        for key, value in jobs_dict.items():
            if value['state'] == 0:
                jobs_pending += 1
                logger.debug("JOB PENDING: req_mem %s - req_cpus %s", value['req_mem'], value['req_cpus'])
            elif value['state'] == 1:
                logger.debug("JOB RUNNING: req_mem %s - req_cpus %s", value['req_mem'], value['req_cpus'])
    else:
        logger.info("No job found")
    logger.debug("Found %d pending jobs.", jobs_pending)
    if LOG_LEVEL == logging.DEBUG:
        __save_file(FILE_BUP_JOBS, jobs_dict)
    return jobs_dict, jobs_pending


def get_cluster_data(cluster_id, cluster_pw):
    """
    receive worker information from portal
    request example:
    requests:  {'active_worker': [{'ip': '192.168.1.17', 'cores': 4, 'hostname': 'bibigrid-worker-1-1-3zytxfzrsrcl8ku',
     'memory': 4096, 'status': 'ACTIVE', 'ephemerals': []}, {'ip': ...}, 'VERSION': '0.2.0'}
     :param cluster_id: current cluster identifier
     :param cluster_pw: current cluster password
    """

    try:
        response = requests.post(url=get_url_info_cluster(cluster_id),
                                 json={"scaling": "scaling_up", "password": cluster_pw})

        if response.status_code == 200:
            res = response.json()
            cluster_data = [data for data in res["active_worker"] if data is not None]

            return cluster_data
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        sys.exit(1)


def __get_flavor_available_count(flavors_data, flavor_name):
    """
    return the number of available workers with this flavor

    :param flavors_data:
    :param flavor_name:
    :return: maximum of new possible workers with this flavor
    """
    for fd in flavors_data:
        if fd['flavor']['name'] == flavor_name:
            return fd['available_count']
    return 0


def get_usable_flavors(cluster_id, cluster_pw):
    """
    receive flavor information from portal
    returned list:
    - the largest flavor on top
    - memory in GB
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return: available flavors as json
    """

    res = requests.post(url=get_url_info_flavors(cluster_id),
                        json={"password": cluster_pw})

    logger.debug("requests: %s", res)
    if res.status_code == 200:
        flavors_data = res.json()

        for fd in flavors_data:
            # logger.info ( "flavorData %s",fd )
            logger.debug("flavor: %s -- ram: %s GB -- cpu: %s -- available: %sx", fd['flavor']['name'],
                         fd['flavor']['ram'], fd['flavor']['vcpus'], fd['available_count'])
        return flavors_data
    else:
        logger.error("WRONG_PASSWORD_MSG")
        sys.exit(1)


def __get_file(read_file):
    """
    read json content from file and return it
    :param read_file: file to read
    :return: file content as json
    """
    try:
        with open(read_file, 'r', encoding='utf-8') as f:
            file_json = json.load(f)
        return file_json
    except IOError:
        logger.error("Error: file does not exist %s", read_file)
        sys.exit(1)


def __save_file(save_file, content):
    """
    read json content from file and return it
    :param save_file: file to write
    :return: file content as json
    """
    try:
        with open(save_file, 'w', encoding='utf-8') as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
    except IOError:
        logger.error("Error: file does not exist %s", save_file)
        sys.exit(1)


def __get_cluster_password():
    """
    read cluster password from CLUSTER_PASSWORD_FILE
    required file content: {"password":"CLUSTER_PASSWORD"}

    :return: "CLUSTER_PASSWORD"
    """
    pw_json = __get_file(CLUSTER_PASSWORD_FILE)
    cluster_pw = pw_json['password']
    logger.debug("pw_json: %s", cluster_pw)
    return cluster_pw


def __get_portal_auth():
    """
    read the authentication file
    :return: the authorization name and password
    """
    portal_auth = __get_file(FILE_PORTAL_AUTH)
    logger.debug(pformat(portal_auth))
    return portal_auth['server'], portal_auth['password']


def check_workers(cluster_id, cluster_pw):
    """
        fetch cluster data from server and check if all workers are active

        :param cluster_id: current cluster identifier
        :param cluster_pw: current cluster password
        :return: return true, when all workers are worker_ready (ACTIVE)
    """

    url_cluster_info = get_url_info_cluster(cluster_id)

    logger.debug("checkWorkers: %s ", url_cluster_info)
    worker_ready = False

    while not worker_ready:
        worker_ready = True
        if not SIMULATION:
            cluster_data = get_cluster_data(cluster_id, cluster_pw)

            for cl in cluster_data:
                logger.debug("hostname: '%s' status: '%s'", cl['hostname'], cl['status'])
                if 'DRAIN' in cl['status'] or 'DOWN' in cl['status']:
                    logger.error("possible broken worker!")
                    sys.exit(1)
                if cl['status'] != 'ACTIVE':  # SCHEDULING -> BUILD -> (CHECKING_CONNECTION) -> ACTIVE
                    logger.info("at least one worker is not 'ACTIVE', wait ... %d",
                                WAIT_CLUSTER_SCALING_UP)
                    worker_ready = False
                    time.sleep(WAIT_CLUSTER_SCALING_UP)
                    break

    logger.info("ALL WORKERS READY!!!")
    return True


def __generate_downscale_list(worker_data, count):
    """
        return all unallocated workers by name as json

        :param worker_data: worker data as json object
        :param count: number of workers
        :return: json list with idle workers,
                 example: {"password":"password","worker_hostnames":["host1","Host2",...]}
    """
    idle_workers_json = {'worker_hostnames': []}

    for key, value in worker_data.items():
        if 'worker' in key:
            # check for broken workers first
            if (NODE_ALLOCATED not in value['state']) and (NODE_MIX not in value['state']) and (
                    NODE_IDLE not in value['state']):
                # should never be reached, exit on DRAIN oder DOWN state @fetch_node_data @check_workers
                logger.error("worker is in unknown state: key %s  value %s", key, value['state'])
            elif value['state'] != NODE_ALLOCATED and (NODE_MIX not in value['state']) and count > 0:
                idle_workers_json['worker_hostnames'].append(key)
                logger.info("count: %d - type: %s ", count, type(idle_workers_json))
                count -= 1
    logger.debug(json.dumps(idle_workers_json, indent=4, sort_keys=True))
    if idle_workers_json['worker_hostnames'].__sizeof__() == 0:
        logger.error("unable to generate downscale list")
        sys.exit(1)
    return idle_workers_json


def __calculate_scale_down_value(worker_count, worker_free):
    """
    retain number of free workers to delete
    respect boundaries like DOWNSCALE_LIMIT and SCALE_FORCE

    :param worker_count: number of total workers
    :param worker_free: number of idle workers
    :return: number of workers to scale down
    """

    if worker_count > LIMIT_DOWNSCALE:
        if worker_free < worker_count:
            return round(worker_free * SCALE_FORCE, 0)

        else:  # all workers idle, max scale down
            return worker_free - LIMIT_DOWNSCALE
    else:
        return 0


def __similar_job_history(jobs_dict, job_pending_name, job_time_max, job_time_min):
    """
    search for jobs with a similar name in job history
    use the average execution time for this job name

    - last job has a similar name as a completed job in history
    - job time from similar job in history leads to upscaling flavor
    - maybe the similar job name requires the same job time

    return data and use it for upscaling decision ...

    :param jobs_dict: jobs as json dictionary object
    :param job_pending_name: pending job name as string
    :param job_time_max: longest job time
    :param job_time_min: shortest job time
    :return: if similar jobs found, return average time value in seconds for these jobs
             if no similar job found, return -1
    """
    global JOB_MATCH_VALUE
    job_count = 0
    job_time_sum: int = 0

    if jobs_dict:

        for key, value in jobs_dict.items():

            # pprint(value)
            if value['state'] == 3:  # 3 finished jobs
                diff_match = difflib.SequenceMatcher(None, value['jobname'], job_pending_name).ratio()
                # logger.debug("SequenceMatcher found a similar job with %s, compare %s %s", diff_match,
                #              value['jobname'], job_pending_name)
                if diff_match > JOB_MATCH_VALUE:
                    job_count += 1
                    job_time_sum += value['elapsed']
                    # logger.debug(
                    #     "id %s name %s - mem %s cpus %s - prio %s node %s - state %s - %s sec",
                    #     value['jobid'], value['jobname'], value['req_mem'], value['req_cpus'],
                    #     value['priority'], value['nodes'],
                    #     value['state_str'],
                    #     value['elapsed'])  # , value['start'], value['end'], value['end']-value['start'])

    norm = __calc_job_time_norm(job_time_sum, job_count, job_time_max, job_time_min)
    logger.debug("__similar_job_history for %s - norm %s - job_time_sum %s - job_count %s", job_pending_name, norm,
                 job_time_sum, job_count)
    return norm


def __calculate_scale_up_data(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count):
    """
    create scale-up data

    adoptive mode
    - user selection for SCALE_FORCE and long_job_time
    - in regular bases, we do not want to create a new worker for any single job in pending state

    - calculate a foresighted upscale value:
      - SCALE_FORCE value (0 to 1) - strength of scaling, TODO user selection - mode selection
      - job_times - value (0 to 1) - previous jobs were time consuming?
        - long job definition, user definition - TODO mode selection - if (job_time > long_job_time); then 1
          - at what time is a job a long time job
        - job times from generic previous jobs TODO user selection
            v job times from similar previous jobs (__similar_job_history) - TODO user selection
            - __calc_previous_job_time return a value from 0-1 to identify very short jobs
      - jobs_pending - how many jobs are in pending state

    - generate a worker adapted for the next job(s) in queue
      - job priority from slurm required
      - based on the next job in queue (with highest priority)
      - worker selection based on priority (cpu and memory) requirements

    - limited by flavor - mode selection
      - we can only start one flavor in one pass
      - start only workers for jobs, if they have the same flavor
      - jobs with another flavor in the next call
      - jobs should be priority sorted by scheduler (slurm: cpu+memory -> priority)

    - SCALE_FORCE_WORKER_WEIGHT - TODO mode option
      - value is multiplied by worker_count and  is subtracted from SCALE_FORCE
      - example: SCALE_FORCE_WORKER_WEIGHT = 0.05
      - include in the calculation how many workers are active, the SCALE_FORCE_WORKER value depends on the mode
      - reduce the chance starting a new worker
      -> The more workers are already active, the lower the probability of starting a new worker.

      :param cluster_id: current cluster identifier
      :param cluster_pw: current cluster password
      :param jobs_dict: jobs as json dictionary object
      :param jobs_pending: number of pending jobs
      :return: up_scale_data: scale up data for server
    """
    global FLAVOR_DEFAULT, SCALE_FORCE_WORKER, SCALE_FORCE_WORKER_WEIGHT, SCALE_FORCE
    job_priority = __sort_job_priority(jobs_dict)
    # include previous job time
    job_time_max, job_time_min = __calc_job_time_range(jobs_dict)
    job_times = __calc_previous_job_time(jobs_dict, job_time_max, job_time_min)
    flavors_data = get_usable_flavors(cluster_id, cluster_pw)

    # reduce starting new workers based on the number of currently existing workers
    if SCALE_FORCE_WORKER:
        force = SCALE_FORCE - (SCALE_FORCE_WORKER_WEIGHT * worker_count)
        logger.debug("SCALE_FORCE_WORKER_WEIGHT: %s - worker_count %s", SCALE_FORCE_WORKER_WEIGHT, worker_count)
        logger.debug("SCALE_FORCE: %s - force %s", SCALE_FORCE, force)
        if force < 0.2:  # prevent too low scale force
            logger.debug("scale force too low: %s - reset 0.2", force)
            force = 0.2
    else:
        force = SCALE_FORCE

    upscale_limit = int(round(jobs_pending * (job_times + force) / 2, 0))  # 1 + ?

    logger.info("maybe we need %d new worker", upscale_limit)

    # TODO upscale_limit only with high pending jobs? - use only job time similar
    # TODO check job history with daytime
    # as an alternative, test at least one worker
    if upscale_limit == 0 and JOB_TIME_SIMILAR_SEARCH:
        logger.debug("test at least one worker with similar search")
        upscale_limit = 1

    upscale_counter = upscale_limit
    flavor_tmp = ""
    up_scale_data = {}

    if upscale_limit > 0:
        cnt_worker_remove = 0
        for key, value in job_priority:
            if value['state'] == 0:  # pending jobs
                # check for cpu and memory usage
                logger.debug("WORKER %d , jid %s:", upscale_counter, key)
                # use the upscale limit - we may not need a new worker for any single pending job
                if upscale_counter > 0:
                    upscale_counter -= 1
                    # logger.info(jobs_dict)
                    logger.debug("+ cpu %s mem %s MB priority %s for jobid %s name %s", value['req_cpus'],
                                 value['req_mem'],
                                 value['priority'], value['jobid'], value['jobname'])

                    # --- test similar job in history with significant low execution time

                    logger.debug("revaluate required worker with previous job name in history, %s ",
                                 JOB_TIME_SIMILAR_SEARCH)
                    if JOB_TIME_SIMILAR_SEARCH:
                        job_time_norm = __similar_job_history(jobs_dict, value['jobname'], job_time_max, job_time_min)
                        # recalculate for this job
                        # normalize the job time to 0-1 value
                        if job_time_norm < JOB_MATCH_SIMILAR:
                            # TODO if 0 worker available? - broken system ?
                            logger.debug("NOT need this worker %s", job_time_norm)
                            upscale_limit -= 1
                            cnt_worker_remove += 1
                        else:
                            logger.debug("NEED this worker %s", job_time_norm)
                        # logger.info("__similar_job_history recalculate \nint(round(%s * (%s + %s) / 2, 0))=%s",
                        #             jobs_pending, job_time_norm, SCALE_FORCE,
                        #             int(round(jobs_pending * (job_time_norm + SCALE_FORCE) / 2, 0)))
                        logger.debug("--- automatic flavor selection disabled: %s ---", FLAVOR_FIX)
                    if not FLAVOR_FIX:
                        # only ONE flavor possible - I calculate one for every new worker
                        flavor_tmp_current = __translate_cpu_mem_to_flavor(value['req_cpus'],
                                                                           value['req_mem'],
                                                                           flavors_data)
                        # break up on flavor change
                        if not flavor_tmp and flavor_tmp_current:
                            flavor_tmp = flavor_tmp_current
                            logger.debug("FLAVOR_FIX new flavor - %s", flavor_tmp)
                        elif flavor_tmp == flavor_tmp_current and flavor_tmp:
                            logger.debug("FLAVOR_FIX same flavor - %s", flavor_tmp)
                        elif flavor_tmp and flavor_tmp_current and flavor_tmp != flavor_tmp_current:
                            logger.debug("FLAVOR_FIX reached another flavor - need to skip here %s - %s", flavor_tmp,
                                         flavor_tmp_current)
                            break
                        else:
                            logger.debug("FLAVOR_FIX unable to select flavor")
                            break
                        # --------
                    else:
                        flavor_tmp = FLAVOR_DEFAULT
                else:
                    # break
                    logger.debug("- cpu %s mem %s priority %s ", value['req_cpus'], value['req_mem'], value['priority'])
        logger.info("similar job search removed %d worker and the calculated flavor is: %s", cnt_worker_remove,
                    flavor_tmp)
        # check with available flavors on cluster
        flavor_count_available = __get_flavor_available_count(flavors_data, flavor_tmp)
        if upscale_limit > flavor_count_available:
            logger.info("reduce worker upscale count %s - allowed %s ", upscale_limit, flavor_count_available)
            upscale_limit = flavor_count_available

        up_scale_data = {"password": cluster_pw, "worker_flavor_name": flavor_tmp,
                         "upscale_count": upscale_limit}
        logger.debug("UpList: \n%s", pformat(up_scale_data))

    if upscale_limit == 0 or not flavor_tmp:
        logger.info("calculated upscale_limit is %d or missing flavor '%s', exit", upscale_limit, flavor_tmp)
        sys.exit(0)
    return up_scale_data


def __calc_job_time_range(jobs_dict):
    """
    automatic setup maximal and minimal job-time from job history
    mode setting - JOB_TIME_RANGE_AUTOMATIC
    check the elapsed value on completed jobs

    :param jobs_dict: jobs as json dictionary object
    :return:
            job_time_max: longest job time
            job_time_min: shortest job time
    """
    global JOB_TIME_RANGE_AUTOMATIC, JOB_TIME_LONG, JOB_TIME_SHORT

    if JOB_TIME_RANGE_AUTOMATIC:

        job_time_min = 1
        job_time_max = 1
        for key, value in jobs_dict.items():
            if value['state'] == 3:  # finished jobs
                # logger.debug("%s   %s", value['state'], value['elapsed'])
                if job_time_min == 1 and value['elapsed'] > 0:
                    job_time_min = value['elapsed']
                elif job_time_min > value['elapsed'] > 0:
                    job_time_min = value['elapsed']

                if job_time_max == 1 and value['elapsed'] > 0:
                    job_time_max = value['elapsed']
                elif job_time_max < value['elapsed']:
                    job_time_max = value['elapsed']

        logger.debug("job_time_max %s job_time_min %s", job_time_max, job_time_min)

        logger.debug("JOB_TIME_RANGE_AUTOMATIC %s", JOB_TIME_RANGE_AUTOMATIC)

        return job_time_max, job_time_min
    else:
        return JOB_TIME_LONG, JOB_TIME_SHORT


def __calc_job_time_norm(job_time_sum, job_num, job_time_max, job_time_min):
    """
    generate a usable value for scale up
    calculate an average job time and normalize

    generate normalized value in relation to maximum and minimum job time
    last completed jobs where ...
    0: very short
    1: took long

    :param job_time_sum: sum of job times
    :param job_num: job count
    :param job_time_max: longest job time
    :param job_time_min: shortest job time
    :return: normalized value from 0 to 1

    """

    # generate normalized value
    if job_num > 0 and job_time_sum > 1:
        logger.debug("job_time_sum: %d job_num: %d", job_time_sum, job_num)
        job_time_average = (job_time_sum / job_num)

        norm = float(job_time_average - job_time_min) / float(job_time_max - job_time_min)
        logger.debug("job_time_average: %d norm: %f", job_time_average, norm)

        if 1 > norm > 0:
            return norm
        else:
            logger.debug("job_time_average: miscalculation")
            if norm < 0:
                return 0.1
            else:
                return 1
    return 1


def __calc_previous_job_time(jobs_dict, job_time_max, job_time_min):
    """
    check for finished jobs

    prevent too many new workers when previous jobs are done in a few minutes
    :param jobs_dict: jobs as json dictionary object
    :param job_time_max: longest job time
    :param job_time_min: shortest job time
    :return: normalized value from 0 to 1
    """

    if jobs_dict:
        job_time_sum = 0
        counter = 0
        for key, value in jobs_dict.items():
            if value['state'] == 3 and value['elapsed'] > 1:  # 3 finished jobs
                counter += 1
                # logger.debug("%s   %s", value['state'], value['elapsed'])
                job_time_sum += (value['end'] - value['start'])
        return __calc_job_time_norm(job_time_sum, counter, job_time_max, job_time_min)
    else:
        logger.info("No job found")
        return 1


def __scale_down_job_frequency(jobs_dict, jobs_pending):
    """
    limit scale down frequency
    compare with job history

    if jobs have started in a short interval, then it might not be worth deleting workers
    TODO analyze job history by hour and day ?
    * wait at least x minutes before scale down

    :return:
        True : if scale down allowed
        False: if scale down denied
    """
    if jobs_pending == 0:  # if pending jobs + scale-down initiated -> worker not suitable
        time_now = str((datetime.datetime.now().replace(microsecond=0).timestamp())).split('.')[0]
        wait_time = 30 * 60
        for key, value in reversed(jobs_dict.items()):
            # logger.debug(pformat(value))

            if value['state'] == 3:  # if last job is completed
                logger.debug("job end time: %s , %s", datetime.datetime.fromtimestamp(value['end']), value['end'])
                elapsed_time = float(time_now) - float(value['end'])
                logger.debug("elapsed_time: %s wait_time %s time_now %s value['end'] %s", elapsed_time, wait_time,
                             time_now,
                             value['end'])
                if elapsed_time < wait_time:
                    logger.info("prevent scale down by frequency")
                    return False
            break
    return True


def __multiscale_scale_down(cluster_id, cluster_pw, scale_state, worker_json, worker_down_cnt, jobs_dict, jobs_pending):
    """

    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param scale_state: current scale state
    :param worker_json: worker information as json dictionary object
    :return: new scale state
    """
    global SCALE_DELAY_WAIT

    if __scale_down_job_frequency(jobs_dict, jobs_pending):
        if scale_state == ScaleState.SCALE_DELAY:
            scale_state = ScaleState.SCALE_DOWN
            time.sleep(SCALE_DELAY_WAIT)  # wait and recheck slurm values after SCALE_DELAY
        elif scale_state == ScaleState.SCALE_DOWN:
            cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_down_cnt)
            scale_state = ScaleState.SCALE_DONE
        else:
            scale_state = ScaleState.SCALE_SKIP
            logger.info("---- SCALE DOWN - condition changed - skip ----")
    else:
        scale_state = ScaleState.SCALE_SKIP
        logger.info("---- SCALE DOWN - high job frequency ----")
    return scale_state


def multiscale(cluster_id, cluster_pw):
    """
    make scaling decision with cpu and memory information
    evaluate cpu and memory information from pending jobs
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    """

    logger.debug("================= %s =================", inspect.stack()[0][3])

    state = ScaleState.SCALE_DELAY

    while state != ScaleState.SCALE_SKIP and state != ScaleState.SCALE_DONE:
        jobs_json, jobs_pending = receive_job_data()
        worker_json, worker_count, worker_in_use = receive_node_data()
        worker_free = (worker_count - worker_in_use)

        logger.info(
            "\n------------------------------------------------------"
            "\n-----------#### scaling loop state: %s ####"
            "\n------------------------------------------------------", state.name)
        logger.info("worker_count: %s worker_in_use: %s worker_free: %s jobs_pending: %s", worker_count,
                    worker_in_use, worker_free, jobs_pending)
        # SCALE DOWN
        if worker_count > LIMIT_DOWNSCALE and worker_in_use == 0:
            # generateDownScaleList(worker_json, worker_free)
            # workers are not in use and not reached DOWNSCALE_LIMIT
            # nothing to do here, just scale down any worker

            logger.info("SCALE DOWN - DELETE:" + "\nworkers are not in use and not reached DOWNSCALE_LIMIT")
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_json,
                                            __calculate_scale_down_value(worker_count, worker_free), jobs_json,
                                            jobs_pending)
        elif worker_count > LIMIT_DOWNSCALE and worker_free >= 1 and worker_in_use > 0 and jobs_pending == 0:
            logger.info("SCALE DOWN - DELETE:" + "\nworkers are free but no jobs pending - not reached DOWNSCALE_LIMIT")
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_json, worker_free, jobs_json,
                                            jobs_pending)
        elif worker_count > LIMIT_DOWNSCALE and worker_free > round(SCALE_FORCE * 4, 0):  # TODO merge
            # if medium or high CPU usage, but multiple workers are idle
            # more workers are idle than the force allows
            # __generate_downscale_list(worker_json, round(SCALE_FORCE * 4, 0))

            logger.info("SCALE DOWN - DELETE: more workers %s are idle than the force %s allows", worker_free,
                        round(SCALE_FORCE * 4, 0))
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_json,
                                            __calculate_scale_down_value(worker_count, worker_free), jobs_json,
                                            jobs_pending)

        # SCALE DOWN and UP
        elif worker_free > 0 and jobs_pending >= 1:
            # required CPU/MEM usage from next pending job may over current worker resources ?
            # jobs pending, but workers are idle, possible low resources ?
            # -> if workers are free, but jobs are pending and CPU/MEM usage may not sufficient
            # -> SCALE DOWN - delete free worker with too low resources
            # start at least one worker, plus additional ones if a lot of jobs pending

            logger.info("---- SCALE DOWN_UP - worker_free and jobs_pending ----")
            if state == ScaleState.SCALE_DOWN_UP:
                logger.info("---- SCALE DOWN_UP - cluster_scale_down_specific ----")
                cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_free)
                state = ScaleState.SCALE_UP
            elif state == ScaleState.SCALE_DELAY:
                logger.info("---- SCALE DOWN_UP - SCALE_DELAY_WAIT ----")
                state = ScaleState.SCALE_DOWN_UP
                time.sleep(SCALE_DELAY_WAIT)
            else:
                state = ScaleState.SCALE_SKIP
                logger.info("---- SCALE DOWN_UP - condition changed - skip ----")
        # SCALE UP
        elif worker_count == worker_in_use and jobs_pending >= 1:  # and OLD_JOB_TIME > JOB_EXECTIME:
            # -> if all workers are in use and jobs pending and jobs require more time
            # calculate
            #    -> SCALE_FORCE*JOBS_WAITING - check resources for every job + worker
            #        -> initiate scale up

            logger.info("---- SCALE UP - all workers are in use with pending jobs ----")
            if state == ScaleState.SCALE_DELAY:
                state = ScaleState.SCALE_UP
                time.sleep(SCALE_DELAY_WAIT)
            elif state == ScaleState.SCALE_UP:
                cluster_scale_up(cluster_id, cluster_pw, jobs_json, jobs_pending, worker_count)
                state = ScaleState.SCALE_DONE
            else:
                state = ScaleState.SCALE_SKIP
                logger.info("---- SCALE UP - condition changed - skip ----")
        else:
            if state == ScaleState.SCALE_UP or state == ScaleState.SCALE_DOWN:
                logger.info("---- SCALE - condition changed - skip ----")
            else:
                logger.debug("<<<< skip scaling >>>> ")
            state = ScaleState.SCALE_SKIP

    logger.info(
        "\n------------------------------------------------------"
        "\n-----------#### scaling loop state: %s ####"
        "\n------------------------------------------------------", state.name)


def cloud_api(cluster_id, portal_url_scale, worker_data):
    """
    create connection to cloud api
    send data for up-scaling or down-scaling

    api response code is typically 200
    api response text contains a new cluster password
        CLUSTER_PASSWORD_FILE will be updated during execution

    api error response example: {"error":"1 additional workers requested, only 0 possible"}

    :param cluster_id: current cluster identifier
    :param portal_url_scale: portal url, used for up- or down-scaling
    :param worker_data: sent data to the server
    :return: new cluster password
    """

    logger.debug("URL: %s ", portal_url_scale)
    if not SIMULATION:
        try:
            if PORTAL_AUTH:
                auth_name, auth_pw = __get_portal_auth()
                response = requests.post(url=portal_url_scale, json=worker_data,
                                         auth=(auth_name, auth_pw))
            else:
                response = requests.post(url=portal_url_scale, json=worker_data)
            response.raise_for_status()
            logger.info("### response.text: %s", response.text)
            logger.info("### response.status_code: %s", response.status_code)

            cluster_pw = json.loads(response.text)['password']  # response.text.split(':')[1]
            logger.debug("new cluster password is: %s", cluster_pw)
        except requests.exceptions.HTTPError as e:
            logger.error(e.response.text)
            logger.error(e.response.status_code)
            sys.exit(1)

        # backup cluster password
        with open(CLUSTER_PASSWORD_FILE, 'w') as f:
            f.write(response.text)

        # extract response text
        logger.debug("portal response is:%s" % response.text)
        rescale_slurm_cluster(cluster_id, cluster_pw)
        return cluster_pw
    else:
        logger.debug("%s: JUST A DRY RUN!!", inspect.stack()[0][3])
        return ""


def cluster_scale_up(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count):
    """

    :param worker_count: number of total workers
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param jobs_dict: jobs as json dictionary object
    :param jobs_pending: number of pending jobs
    :return:
    """

    data = __calculate_scale_up_data(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count)
    cloud_api(cluster_id, get_url_scale_up(cluster_id), data)


def cluster_scale_down(cluster_id, cluster_pw, worker_count):
    """
    scale down by a number of worker
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param worker_count: number of worker to delete
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    worker_batches = [{"flavor": {"name": FLAVOR_DEFAULT}, "index": 1, "delete_count": worker_count}]
    data = {"password": cluster_pw, 'worker_batches': worker_batches}
    cloud_api(cluster_id, get_url_scale_down(cluster_id), data)


def cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, hostnames):
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])

    worker_hostnames = '[' + hostnames + ']'
    logger.debug(hostnames)
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostnames}
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific(cluster_id))
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cloud_api(cluster_id, get_url_scale_down_specific(cluster_id), data)


def cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_num):
    """
    scale down with
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param worker_json: worker information as json dictionary object
    :param worker_num: number of worker to delete
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    worker_hostname = __generate_downscale_list(worker_json, worker_num)

    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostname['worker_hostnames']}
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific(cluster_id))
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cloud_api(cluster_id, get_url_scale_down_specific(cluster_id), data)


def rescale_slurm_cluster(cluster_id, cluster_pw):
    """
    apply the new worker configuration
    execute scaling script, to rerun ansible playbook
    wait for workers, all should be ACTIVE (check_workers)
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    logger.debug("initiateScaling wait %s seconds", WAIT_CLUSTER_SCALING_DOWN)
    time.sleep(WAIT_CLUSTER_SCALING_DOWN)
    logger.debug("...")

    path = Path(AUTOSCALING_FOLDER)
    scaling_file = path.joinpath(FILE_SCALING)

    if not scaling_file.is_file():
        logger.debug("scaling file not found:\n%s", scaling_file)
        # use modified scaling.py with PORTAL and PW parameter
        urllib.request.urlretrieve(SCALING_URL,
                                   str(scaling_file))
    if check_workers(cluster_id, cluster_pw):
        os.system("python3 " + str(scaling_file) + " " + cluster_pw)
        logger.debug("python3 %s pw: %s", scaling_file, cluster_pw)


def __cluster_scale_up_test(cluster_id, cluster_pw, upscale_limit):
    """
    scale up cluster by number
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param upscale_limit: scale up number of workers
    :return:
    """
    logger.info("----- test upscaling, start %d new worker nodes with %s -----", upscale_limit, FLAVOR_DEFAULT)
    up_scale_data = {"password": cluster_pw, "worker_flavor_name": FLAVOR_DEFAULT,
                     "upscale_count": upscale_limit}
    cloud_api(cluster_id, get_url_scale_up(cluster_id), up_scale_data)


def __cluster_scale_down_specific_test(cluster_id, cluster_pw):
    """
    scale down all idle worker
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    worker_json, worker_count, worker_in_use = receive_node_data()
    worker_cnt = worker_count - worker_in_use
    cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_cnt)


def __cluster_scale_down_workers_test(cluster_id, cluster_pw, worker_hostname):
    """
    scale down cluster by hostnames
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param worker_hostname: "[host_1, host_2, ...]"
    :return:
    """
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostname}
    logger.debug("cluster_scaledown_workers_test: %s", get_url_scale_down_specific(cluster_id))
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cloud_api(cluster_id, get_url_scale_down_specific(cluster_id), data)


def __print_help():
    print(textwrap.dedent("""\
        Option      Long option         Argument    Meaning
        -v          -version                        print the current version number
        -h          -help                           print this help information
        -su         -scaleup                        scale up cluster by one worker (default flavor)
        -su         -scaleup            2           scale up cluster by a given number of workers (default flavor)
        -sd         -scaledown                      scale down cluster, all idle workers
        -sds        -scaledownspecific  wk1,wk2,... scale down cluster by given workers
        -rsc        -rescale                        run scaling script with ansible playbook 
        -m          -mode               adoptive    scaling with a high adaptation
        -m          -mode               min
        -m          -mode               max
        _                                           no argument - standard adoptive scaling
    """))


if __name__ == '__main__':

    if len(sys.argv) == 2:
        if sys.argv[1] in ["-v", "--v", "-version", "--version"]:
            get_version()
            sys.exit(0)
        elif sys.argv[1] in ["-h", "--h", "-help", "--help"]:
            __print_help()
            sys.exit(0)

    # setup logger
    logger = logging.getLogger('')
    logger.setLevel(LOG_LEVEL)
    fh = logging.FileHandler(LOG_FILE)
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
                                  datefmt='%a, %d %b %Y %H:%M:%S')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)

    # create PID file
    pid = str(os.getpid())
    pidfile = AUTOSCALING_FOLDER + FILE_PID
    if os.path.isfile(pidfile):
        logger.critical("pid file already exists %s, exiting\n", pidfile)
        sys.exit()
    open(pidfile, 'w').write(pid)
    try:

        logger.info('\n#############################################'
                    '\n########### START SCALING PROGRAM ###########'
                    '\n#############################################')

        cid = read_cluster_id()
        cpw = __get_cluster_password()

        if len(sys.argv) > 1:
            arg = sys.argv[1]
            print('len(sys.argv): ', len(sys.argv))
            if len(sys.argv) == 2:
                if arg in ["-c", "--c", "-clusterdata", "--clusterdata"]:
                    pprint(get_cluster_data(cid, cpw))
                    sys.exit(0)
                elif arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    __cluster_scale_up_test(cid, cpw, 1)
                    sys.exit(0)
                elif arg in ["-sd", "--sd", "-scaledown", "--scaledown"]:
                    print('scale-down')
                    __cluster_scale_down_specific_test(cid, cpw)
                    sys.exit(0)
                elif arg in ["-rsc", "--rsc", "-rescale", "--rescale"]:
                    print('rerun scaling')
                    rescale_slurm_cluster(cid, cpw)
                    sys.exit(0)
                else:
                    print("No usage found for param: ", arg)
                sys.exit(0)
            elif len(sys.argv) == 3:
                arg2 = sys.argv[2]
                if arg in ["-m", "--m", "-mode", "--mode"]:
                    print('mode parameter: ', arg2)
                    # TODO setup mode options with read_config_yaml()
                elif arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    if arg2.isnumeric():
                        __cluster_scale_up_test(cid, cpw, int(arg2))
                    else:
                        logger.error("we need a number, for example: -su 2")
                        sys.exit(1)
                    sys.exit(0)
                elif arg in ["-sds", "--sds", "-scaledownspecific", "--scaledownspecific"]:
                    print('scale-down-specific')
                    if "worker" in arg2:
                        cluster_scale_down_specific_hostnames(cid, cpw, arg2)
                    else:
                        logger.error("hostname parameter without 'worker'")
                    sys.exit(0)
                else:
                    print("No usage found for param: ", arg, arg2)
                    sys.exit(0)
        else:
            print('\npass a mode via parameter')

        multiscale(cid, cpw)

    finally:
        os.unlink(pidfile)

    sys.exit(0)
