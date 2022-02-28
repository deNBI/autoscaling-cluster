#!/usr/bin/python3

import difflib
import os
import time
import sys
import re
import pyslurm
# import pandas as pd
import datetime
import json
import requests
import enum
import logging
import inspect
import textwrap
import yaml
import csv
from pathlib import Path
from pprint import pformat, pprint
from functools import total_ordering

SIMULATION = False
VERSION = "0.3.0"  # for testing purposes only
OUTDATED_SCRIPT_MSG = "Your script is outdated [VERSION: {SCRIPT_VERSION} - latest is {LATEST_VERSION}] " \
                      "-  please download the current version and run it again!"
PORTAL_AUTH = False

# ----- SYSTEM PARAMETERS -----
AUTOSCALING_FOLDER = os.path.dirname(os.path.realpath(__file__)) + '/'
IDENTIFIER = "autoscaling"

FILE_BUP_NODE = AUTOSCALING_FOLDER + 'backup_node.json'
FILE_BUP_JOBS = AUTOSCALING_FOLDER + 'backup_jobs.json'

FILE_CONFIG_YAML = AUTOSCALING_FOLDER + 'autoscaling_config.yaml'
FILE_PORTAL_AUTH = AUTOSCALING_FOLDER + 'portal_auth.json'
FILE_PID = IDENTIFIER + ".pid"

PORTAL_LINK = 'https://cloud.denbi.de'
CLUSTER_PASSWORD_FILE = AUTOSCALING_FOLDER + 'cluster_pw.json'  # {"password":"PASSWORD"}

LOG_LEVEL = logging.DEBUG
LOG_FILE = AUTOSCALING_FOLDER + IDENTIFIER + '.log'
LOG_CSV = AUTOSCALING_FOLDER + IDENTIFIER + '.csv'

# ----- NODE STATES -----
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_IDLE = "IDLE"
NODE_DRAIN = "DRAIN"
NODE_KEEP = False  # keep a high worker (if required from scheduler), set LIMIT_DOWNSCALE to 1
NODE_DUMMY = "bibigrid-worker-autoscaling_dummy"
NODE_DUMMY_REQ = True

LIMIT_DOWNSCALE = 0  # 1, if scheduler required
LIMIT_UPSCALE = 2  # not in use

WAIT_CLUSTER_SCALING_UP = 10  # wait seconds
WAIT_CLUSTER_SCALING_DOWN = 10

# ----- MODE PARAMETERS -----
SORT_JOBS_BY_MEMORY = True
SCALE_DELAY_WAIT = 60  # wait and recheck slurm values - SCALE_DELAY

SCALE_FORCE = 0.6  # higher - higher scaling - user decision, mode
SCALE_FORCE_WORKER = True
SCALE_FORCE_WORKER_WEIGHT = 0.02  # reduce chance of workers by current worker count
SCALE_FORCE_JOB_WEIGHT = 0.03  # similar job search, increases chance of new worker by job pending count

SCALE_FORCE_HIGH_NODES = True  # start higher nodes if possible, run multiple jobs on node with higher resources
SCALE_FORCE_DRAIN_NODES = False  # set nodes in drain state, if pending jobs require lower resources than worker available

# weight for cpu memory and minutes -> calculate_current_prediction
PREDICTION_FORCE = False
PREDICTION_BALANCE_CPU = 0.9
PREDICTION_BALANCE_MEM = 1.1
PREDICTION_BALANCE_MIN = 1

SCALE_DOWN_FREQ = 5  # wait time before scale down since last job start

JOB_TIME_LONG = 600  # anything above this value (in seconds) should be a time-consuming job - long_job_time
JOB_TIME_SHORT = 160
JOB_TIME_RANGE_AUTOMATIC = True  # ignore constant job time values

JOB_MATCH_VALUE = 0.9  # include only jobs in calculation with SequenceMatcher
JOB_MATCH_SIMILAR = 0.4  # lower value -tend to start new workers, although a similar job took comparatively little time
JOB_TIME_SIMILAR_SEARCH = True

FLAVOR_FIX = False  # if true, we use the default flavor
FLAVOR_DEFAULT = "de.NBI highmem large"
FLAVOR_EPHEMERAL = False  # only use ephemeral flavors

# ---- rescale cluster ----
HOME = str(Path.home())
PLAYBOOK_DIR = HOME + '/playbook'
PLAYBOOK_VARS_DIR = HOME + '/playbook/vars'
ANSIBLE_HOSTS_FILE = PLAYBOOK_DIR + '/ansible_hosts'
INSTANCES_YML = PLAYBOOK_VARS_DIR + '/instances.yml'
SLURM_YML = PLAYBOOK_VARS_DIR + '/slurm_config.yml'
SLURM_TEMPLATE = PLAYBOOK_DIR + '/roles/common/templates/slurm/slurm.conf'


class ScalingDown:

    def __init__(self, password, dummy_worker):
        self.password = password
        self.dummy_worker = dummy_worker
        self.data = self.get_scaling_down_data()
        self.valid_delete_ips = [ip for ip in self.data["private_ips"] if ip is not None and self.validate_ip(ip)]
        self.master_data = self.data["master"]

        if len(self.valid_delete_ips) > 0:

            self.remove_worker_from_instances()
            self.delete_ip_yaml()
            self.remove_ip_from_ansible_hosts()
        else:
            self.remove_worker_from_instances()
            print("No valid Ips found!")

    def get_scaling_down_data(self):
        res = requests.post(url=get_url_info_cluster(cid),
                            json={"scaling": "scaling_down", "password": self.password},
                            )
        if res.status_code == 200:
            data_json = res.json()
            version = data_json["VERSION"]
            if version != VERSION:
                print(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
                sys.exit(1)
        else:
            print(get_wrong_password_msg())
            sys.exit(1)
        return res.json()

    def validate_ip(self, ip):
        print("Validate  IP: ", ip)
        valid = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip)
        if not valid:
            print("{} is no valid Ip! SKIPPING".format(ip))
        return valid

    def remove_worker_from_instances(self):
        print("Removing workers from instances")
        with open(INSTANCES_YML, 'a+') as stream:
            stream.seek(0)
            try:
                instances = yaml.safe_load(stream)
                if not instances:
                    instances = {"workers": [], "deletedWorkers": [], "master": self.master_data}
                instances_mod = {"workers": [], "deletedWorkers": instances["deletedWorkers"],
                                 "master": self.master_data}

                # logger.debug("rev version:")
                # logger.debug(len(instances['workers']))
                # logger.debug(pformat(instances))

                if not (instances['workers'] is None):  # error if no workers
                    for idx, worker in enumerate(instances['workers']):
                        # rescue worker data to deletedWorkers
                        if worker['ip'] in self.valid_delete_ips:
                            instances_mod['deletedWorkers'].append(worker)
                        # rescue real worker data
                        elif worker['ip'] != self.dummy_worker['ip']:
                            instances_mod['workers'].append(worker)

                stream.seek(0)
                stream.truncate()
                yaml.dump(instances_mod, stream)
                logger.debug("removed version:")
                logger.debug(pformat(instances_mod))
            except yaml.YAMLError as exc:
                print(exc)
                sys.exit(1)

    def delete_ip_yaml(self):
        print("Delete yaml file")
        for ip in self.valid_delete_ips:
            yaml_file = PLAYBOOK_VARS_DIR + '/' + ip + '.yml'
            if os.path.isfile(yaml_file):
                print("Found: ", yaml_file)
                os.remove(yaml_file)
                print("Deleted: ", yaml_file)
            else:
                print("Yaml already deleted: ", yaml_file)

    def remove_ip_from_ansible_hosts(self):

        print("Remove ips from ansible_hosts")
        lines = []
        with open(ANSIBLE_HOSTS_FILE, 'r+') as ansible_hosts:
            for line in ansible_hosts:
                if not any(bad_word in line for bad_word in self.valid_delete_ips):
                    lines.append(line)
            ansible_hosts.seek(0)
            ansible_hosts.truncate()
            for line in lines:
                ansible_hosts.write(line)


class ScalingUp:

    def __init__(self, password, dummy_worker):
        self.password = password
        self.dummy_worker = dummy_worker
        self.data = self.get_cluster_data()
        self.master_data = self.data["master"]
        self.cluster_data = [worker for worker in self.data["active_worker"] if
                             worker is not None and worker["status"] == "ACTIVE" and worker["ip"] is not None]

        self.valid_upscale_ips = [cl["ip"] for cl in self.cluster_data]

        if len(self.cluster_data) > 0:
            workers_data = self.create_yml_file()
            self.add_new_workers_to_instances(worker_data=workers_data)
            self.add_ips_to_ansible_hosts()
        else:
            # keep dummy worker alive
            workers_data = [dummy_worker]
            print(pformat(workers_data))
            self.add_new_workers_to_instances(worker_data=workers_data)

            print("No active worker found!")

    def get_cluster_data(self):

        res = requests.post(url=get_url_info_cluster(cid),
                            json={"scaling": "scaling_up", "password": self.password})
        if res.status_code == 200:
            res = res.json()
            version = res["VERSION"]
            if version != VERSION:
                print(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
                sys.exit(1)
            return res
        else:
            print(get_wrong_password_msg())
            sys.exit(1)

    def validate_ip(self, ip):
        print("Validate  IP: ", ip)
        return re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip)

    def add_new_workers_to_instances(self, worker_data):
        print("Add workers to instances")
        with open(INSTANCES_YML, 'a+') as stream:
            stream.seek(0)

            try:
                instances = yaml.safe_load(stream)
                # print(instances)
                if not instances:
                    instances = {"workers": [], "deletedWorkers": [], "master": self.master_data}
                if not instances['workers']:
                    instances['workers'] = []
                instances['workers'].append(self.dummy_worker)  # dummy test
                worker_ips = [worker['ip'] for worker in instances['workers']]

                for new_worker in worker_data:
                    if not new_worker['ip'] in worker_ips:
                        instances['workers'].append(new_worker)
                    else:
                        print("Worker with IP {} already registered!".format(new_worker['ip']))
                stream.seek(0)
                stream.truncate()
                # logger.debug("new instances version:")
                # logger.debug(pformat(instances))
                yaml.dump(instances, stream)
            except yaml.YAMLError as exc:
                print(exc)
                sys.exit(1)

    def create_yml_file(self):
        workers_data = []
        for data in self.cluster_data:
            yaml_file_target = PLAYBOOK_VARS_DIR + '/' + data['ip'] + '.yml'
            if not os.path.exists(yaml_file_target):
                with  open(yaml_file_target, 'w+') as target:
                    try:
                        yaml.dump(data, target)
                    except yaml.YAMLError as exc:
                        print(exc)
                        sys.exit(1)
            else:
                print("Yaml for worker with IP {} already exists".format(data['ip']))
            workers_data.append(data)

        return workers_data

    def add_ips_to_ansible_hosts(self):
        print("Add ips to ansible_hosts")
        with open(ANSIBLE_HOSTS_FILE, "r") as in_file:
            buf = in_file.readlines()

        with open(ANSIBLE_HOSTS_FILE, "w") as out_file:
            for line in buf:
                if "[workers]" in line:
                    for ip in self.valid_upscale_ips:
                        ip_line = f"{ip} ansible_connection=ssh ansible_python_interpreter=/usr/bin/python3 ansible_user=ubuntu\n"
                        if not ip_line in buf:
                            line = line + ip_line + "\n"
                out_file.write(line)


@total_ordering
class ScaleState(enum.Enum):
    SCALE_UP = 1
    SCALE_DOWN = 0
    SCALE_SKIP = 2
    SCALE_DONE = 3
    SCALE_DOWN_UP = 4
    SCALE_FORCE_UP = 5
    SCALE_DELAY = -1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


def run_ansible_playbook():
    os.chdir(PLAYBOOK_DIR)
    os.system('ansible-playbook -v -i ansible_hosts  site.yml')


def get_dummy_worker(cluster_id, cluster_pw):
    flavors_data = get_usable_flavors(cluster_id, cluster_pw)
    if len(flavors_data) > 0:
        logger.debug("get_dummy_worker %s", flavors_data[0]['flavor']['ram'])
        max_memory = int(flavors_data[0]['flavor']['ram']) * 1024
        # __reduce_flavor_memory by ansible

        max_cpu = flavors_data[0]['flavor']['vcpus']
    else:
        logger.error("%s flavors available ", len(flavors_data))
        max_memory = 512
        max_cpu = 0

    dummy_worker = {'cores': max_cpu, 'ephemerals': [], 'hostname': NODE_DUMMY,
                    'ip': '0.0.0.4', 'memory': max_memory, 'status': 'ACTIVE'}
    return dummy_worker


def rescale_init(cluster_id, cluster_pw):
    dummy = get_dummy_worker(cluster_id, cluster_pw)
    logger.debug("calculated dummy: %s", pformat(dummy))
    ScalingDown(password=cluster_pw, dummy_worker=dummy)
    ScalingUp(password=cluster_pw, dummy_worker=dummy)
    logger.debug("--- run playbook ---")
    run_ansible_playbook()


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


def __fetch_scheduler_job_data(num_days):
    """
    read scheduler data from api
    return a json object with job data
    replace this function to use other scheduler

    define the prerequisites for the json data structure and the required data
    :param num_days: number of days
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
        # print(pformat(jobs_dict))
        return jobs_dict
    except ValueError as e:
        print("Error: unable to receive job data \n%s", e)
        sys.exit(1)


def __update_playbook_scheduler_config(set_config):
    """
    update playbook scheduler (slurm) configuration
    :return:
    """
    # SLURM_YML
    scheduler_config = __read_yaml_file(SLURM_YML)

    # logger.debug(pformat(scheduler_config))
    if scheduler_config['slurm_config']['priority_settings']:
        scheduler_config['slurm_config']['priority_settings'] = set_config
        logger.debug(pformat(scheduler_config['slurm_config']['priority_settings']))
        ___write_yaml_file(SLURM_YML, scheduler_config)
    else:
        logger.debug("scheduler_config, missing: slurm_config, scheduler_settings")


def __update_slurm_config_set(set_config):
    """
    replaced with ansible config generation
    add slurm config settings to slurm config file and remove old settings
    :param set_config:
    :return:
    """
    __update_slurm_config_clean()
    # logger.debug(set_config)
    if not (set_config is None):
        with open(SLURM_TEMPLATE, "a") as in_file:
            in_file.write(set_config)
        in_file.close()


def __update_slurm_config_clean():
    """
    replaced with ansible config generation
    remove old slurm configs from file
    :return:
    """
    with open(SLURM_TEMPLATE, "r") as in_file:
        buf = in_file.readlines()
    in_file.close()
    with open(SLURM_TEMPLATE, "w") as out_file:
        for line in buf:
            if "PriorityType=priority" in line:
                continue
            elif "PriorityFlags=" in line:
                continue
            elif "PriorityFavorSmall=" in line:
                continue
            elif "PriorityWeightJobSize=" in line:
                continue
            elif "AccountingStorageTRES=" in line:
                continue
            elif "PriorityWeightTRES=" in line:
                continue
            out_file.write(line)
    out_file.close()


def __update_dummy_worker():
    """
    manual dummy worker update
    should be replaced by ansible settings
    :return:
    """
    flavors_data = get_usable_flavors(cid, cpw)
    max_memory = flavors_data[0]['available_memory']
    # __reduce_flavor_memory(flavors_data[0]['flavor']['ram'])
    max_cpu = flavors_data[0]['flavor']['vcpus']
    dummy_node = "NodeName=" + NODE_DUMMY
    dummy_worker_line = dummy_node + " SocketsPerBoard=" \
                        + str(max_cpu) + " CoresPerSocket=1 RealMemory=" + str(max_memory) + "\n"
    line_found = False
    logger.debug(dummy_worker_line)
    with open(SLURM_TEMPLATE, "r") as in_file:
        buf = in_file.readlines()
    for line in buf:
        if dummy_node in line:
            line_found = True
            if line == dummy_worker_line:
                logger.debug("dummy worker is up to date")
                return
    if not line_found:
        logger.error("missing dummy worker in slurm template")
        sys.exit(1)

    with open(SLURM_TEMPLATE, "w") as out_file:
        for line in buf:
            if dummy_node in line:
                line = dummy_worker_line
                logger.debug("dummy worker updated: %s", dummy_worker_line)
            out_file.write(line)


def get_current_time():
    time_now = str((datetime.datetime.now().replace(microsecond=0).timestamp())).split('.')[0]
    current_hour = datetime.datetime.fromtimestamp(int(time_now)).hour
    current_minute = datetime.datetime.fromtimestamp(int(time_now)).minute
    return current_hour, current_minute


def __get_job_data_list():
    if SIMULATION:
        job_data = __get_file(FILE_BUP_JOBS)
    else:
        job_data = __fetch_scheduler_job_data(60)
        if LOG_LEVEL == logging.DEBUG:
            __save_file(FILE_BUP_JOBS, job_data)
    # pprint(jobdata)
    data_list = []

    for key, value in reversed(job_data.items()):
        if value['state'] == 3:
            data_list.append(
                [datetime.datetime.fromtimestamp(int(value['start'])).hour,
                 (int(value['req_cpus'])),
                 (int(value['req_mem'])),
                 (int(value['elapsed']))
                 ])
    # print("data_list")
    # print(data_list)

    return data_list


def calculate_prediction_values(num_hour):
    """
    receive job data from scheduler
    use completed jobs for calculation
    - calculate normalized mean values (0-1) for cpu (1), memory (2), minutes (3)
    - calculate mean value for every hour - grouped by time of day
    :param num_hour:
    :return: json dict
    """
    # comment this section to deactivate prediction!
    # - if not in use, required panda packages and dependencies do not need to be installed
    # ---------------- comment ----------------
    # data_list = __get_job_data_list()
    # logger.debug(len(data_list))
    # if (len(data_list)) > 100:
    #     df = pd.DataFrame(data_list)
    #     df = df.groupby(0).mean()
    #     normalized_df = (df - df.min()) / (df.max() - df.min())
    #     result = json.loads(normalized_df.to_json())
    #     # logger.debug(pformat(result))
    #     num_hour = str(num_hour)
    #     try:
    #         mean_cpu = float(result['1'][num_hour])
    #         mean_mem = float(result['2'][num_hour])
    #         mean_min = float(result['3'][num_hour])
    #
    #         logger.info(">> data_list: hour %s - mean normalized values: cpu %s + mem %s + elapsed %s", num_hour,
    #                     mean_cpu,
    #                     mean_mem,
    #                     mean_min)
    #         return mean_cpu, mean_mem, mean_min
    #     except KeyError as e:
    #         logger.error("missing data for hour %s", e)
    # else:
    #     logger.info("too few entries for evaluation")
    # ---------------- comment ----------------
    return None, None, None


def calculate_prediction(num_hour):
    """
    calculate prediction value for specific hour
    merge cpu, memory, elapsed values
    :param num_hour:
    :return:
    """
    mean_cpu, mean_mem, mean_min = calculate_prediction_values(num_hour)
    if not (mean_cpu is None):
        mean_overall = ((mean_cpu * PREDICTION_BALANCE_CPU)
                        + (mean_mem * PREDICTION_BALANCE_MEM)
                        + (mean_min * PREDICTION_BALANCE_MIN)) / 3
        logger.debug("current_hour %d force %s", num_hour, mean_overall)
        return mean_overall
    else:
        # logger.info("too few entries for evaluation")
        return None  # 0.5


def calculate_current_prediction():
    """
    calculate prediction with
    - current time
    - if the current minute exceeds 40, we calculate the prediction for the next hour
    :return: current prediction value (0-1)
    """
    current_hour, current_minute = get_current_time()

    if current_minute > 40:
        return calculate_prediction((current_hour + 1) % 24)
    else:
        return calculate_prediction(current_hour)


def __count_current_cpu_mem_usage(worker_json, jobs_json):
    """
    for csv log only, temporary logging - debug
    :param worker_json: worker information as json dictionary object
    :param jobs_json:
    :return:
    """
    cpu_alloc, mem_alloc = 0, 0
    cpu_idle, mem_idle = 0, 0
    jobs_cpu_alloc, jobs_mem_alloc = 0, 0
    jobs_cpu_pending, jobs_mem_pending = 0, 0
    if jobs_json:
        for key, value in jobs_json.items():
            if value['state'] == 0:  # jobs_pending
                jobs_cpu_pending += value['req_cpus']
                jobs_mem_pending += value['req_mem']
            elif value['state'] == 1:  # jobs_running
                jobs_cpu_alloc += value['req_cpus']
                jobs_mem_alloc += value['req_mem']
    if worker_json:
        for key, value in worker_json.items():
            # logger.debug("key: %s - value: %s", json.dumps(key), json.dumps(value['state']))
            if 'worker' in key:
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    cpu_alloc += value['cpus']
                    mem_alloc += value['real_memory']
                elif NODE_IDLE in value['state']:
                    cpu_idle += value['cpus']
                    mem_idle += value['real_memory']

    return cpu_alloc, mem_alloc, cpu_idle, mem_idle, jobs_cpu_alloc, jobs_mem_alloc, jobs_cpu_pending, jobs_mem_pending


def __csv_log_with_refresh_data(scale_ud):
    """
    log current job and worker data after scale up and down
    - give scheduler some seconds to allocate workers
    - refresh node and worker data
    - write data to csv log
    for debug only
    :param scale_ud:
    :return:
    """
    if LOG_LEVEL == logging.DEBUG:
        logger.debug("---- __csv_log_with_refresh_data ----")
        jobs_json, jobs_pending, jobs_running = receive_job_data()
        worker_json, worker_count, worker_in_use, worker_drain = receive_node_data()
        worker_free = (worker_count - worker_in_use)
        __csv_log('1', jobs_running, jobs_pending, worker_count, worker_in_use, worker_free, scale_ud,
                  __count_current_cpu_mem_usage(worker_json, jobs_json))


def __csv_log(start_, jobs_cnt_running, job_cnt_pending, worker_cnt, worker_cnt_use, worker_cnt_free, scale_ud,
              cpu_mem_sum):
    cpu_alloc, mem_alloc, cpu_idle, mem_idle, jobs_cpu_alloc, jobs_mem_alloc, jobs_cpu_pending, jobs_mem_pending \
        = cpu_mem_sum
    time_now = str((datetime.datetime.now().replace(microsecond=0).timestamp())).split('.')[0]
    w_data = [time_now, str(scale_ud),
              str(worker_cnt), str(worker_cnt_use), str(worker_cnt_free),
              cpu_alloc, cpu_idle,
              mem_alloc, mem_idle,
              str(jobs_cnt_running), str(job_cnt_pending),
              jobs_cpu_alloc, jobs_cpu_pending,
              jobs_mem_alloc, jobs_mem_pending]
    __csv_writer(LOG_CSV, w_data)


def __csv_writer(csv_file, log_data):
    """
    update log file
    :param csv_file: write to file
    :param log_data: write this new line
    :return:
    """
    # logger.debug("csv_writer")
    with open(csv_file, 'a', newline='') as csvfile:
        fw = csv.writer(csvfile, delimiter=',')
        fw.writerow(log_data)


def __read_yaml_file(file_path):
    """
    read yaml data from file
    :param file_path:
    :return:
    """
    with open(file_path, "r") as stream:
        try:
            yaml_data = yaml.safe_load(stream)

            # logger.debug(pformat(yaml_data))

            return yaml_data
        except yaml.YAMLError as exc:
            logger.error(exc)
            sys.exit(1)


def ___write_yaml_file(yaml_file_target, yaml_data):
    """
    write yaml data to file
    :param yaml_file_target:
    :param yaml_data:
    :return:
    """
    with open(yaml_file_target, 'w+') as target:
        try:
            yaml.dump(yaml_data, target)
        except yaml.YAMLError as exc:
            print(exc)
            sys.exit(1)


def read_config_yaml():
    """
    read config from yaml file
    :return:
    """
    yaml_config = __read_yaml_file(FILE_CONFIG_YAML)
    logger.info("Load config from yaml file %s", FILE_CONFIG_YAML)
    if not yaml_config:
        logger.error("missing yaml config")
        return None
    # logger.info("\n %s\n", pformat(yaml_config))

    # logger.debug("mode adoptive %s", pformat(yaml_config['scaling']['mode']['adoptive']))
    return yaml_config['scaling']


def read_ephemeral_state():
    """
    testing TODO
    read ephemeral state from master
    :return:
    """
    global FLAVOR_EPHEMERAL
    yaml_data = __read_yaml_file(os.path.expanduser('~') + "/playbook/vars/instances.yml", )

    if yaml_data['master']['ephemerals']:
        # logger.debug("test instances.yml, ephemeral list is not empty")
        FLAVOR_EPHEMERAL = True
        logger.debug("ephemeral check %s", FLAVOR_EPHEMERAL)
        return True
    else:
        # logger.debug("test instances.yml, ephemeral list is empty")
        FLAVOR_EPHEMERAL = False
        logger.debug("ephemeral check %s", FLAVOR_EPHEMERAL)
        return False


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


def __get_portal_url_webapp():
    return PORTAL_LINK + '/portal/webapp/#/virtualmachines/clusterOverview'


def get_wrong_password_msg():
    print(f"The password seems to be wrong. Please verify it again, otherwise you can generate a new one "
          f"for the cluster on the Cluster Overview ({__get_portal_url_webapp()})")


def __get_portal_url():
    return PORTAL_LINK + '/portal/public'


def __get_portal_url_scaling():
    return __get_portal_url() + '/clusters_scaling/'


def get_url_scale_up(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale up url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-up/"


def get_url_scale_down(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale down url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-down/"


def get_url_scale_down_specific(cluster_id):
    """
    :param cluster_id:
    :return: return portal api scale down specific url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-down/specific/"


def get_url_info_cluster(cluster_id):
    """
    :param cluster_id:
    :return: return portal api info url
    """
    return __get_portal_url() + '/clusters/' + cluster_id + '/'


def get_url_info_flavors(cluster_id):
    """
    :param cluster_id:
    :return: return portal api flavor info url
    """
    return __get_portal_url() + '/clusters_scaling/' + cluster_id + '/usable_flavors/'


def __reduce_flavor_memory(mem_gb):
    """
    receive raw memory in GB and reduce this to a usable value
    memory reduces to os requirements and other circumstances
    :param mem_gb: memory
    :return: memory reduced value (mb)
    """
    # mem_gb = mem/1024
    mem = mem_gb * 1000
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    mem_border_high = 500000
    # {% set mem = worker.memory // 1024 * 1000 %}
    # {% if mem < 16001 %}{{ mem - [ mem // 16, 512] | max }}
    # {% if mem > 16000 %}{{ mem - [mem // 16, 4000] | min }}
    sub = int(mem / 16)
    if mem <= mem_border:
        if sub < mem_min:
            sub = mem_min
    elif mem_border < mem < mem_border_high:
        if sub > mem_max:
            sub = mem_max
    else:
        if mem == mem_border_high:
            sub = 0
        else:
            sub = mem_max
    # logger.debug("reduce_flavor_memory: mem_gb: %s , substract: %s , real: %s", mem, sub, mem - sub)
    return int(mem - sub)


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
    cpu = int(cpu)
    mem = int(mem)
    logger.debug(">>>>>>>>>>>>>> %s >>>>>>>>>>>>>>", inspect.stack()[0][3])
    logger.debug("searching for cpu %d mem %d", cpu, mem)

    step_over_flavor = False

    for fd in reversed(flavors_data):
        if fd['flavor']['ephemeral_disk'] == 0:
            ephemeral_disk = False
        else:
            ephemeral_disk = True

        if not ephemeral_disk and FLAVOR_EPHEMERAL:
            # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", FLAVOR_EPHEMERAL)
            continue
        elif ephemeral_disk and not FLAVOR_EPHEMERAL:
            # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", FLAVOR_EPHEMERAL)
            continue

        logger.debug("flavor: \"%s\" -- ram: %s -- cpu: %s -- available: %sx ephemeral %s id: %s", fd['flavor']['name'],
                     fd['available_memory'], fd['flavor']['vcpus'], fd['available_count'],
                     ephemeral_disk, fd['flavor']['id'])

        if cpu <= int(fd['flavor']['vcpus']) and mem <= int(fd['available_memory']):
            # available_memory = __reduce_flavor_memory(fd['flavor']['ram']
            if fd['available_count'] > 0:
                logger.debug("-> match found %s", fd['flavor']['name'])
                return fd
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
    logger.debug("job list %s", name)
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
    worker_drain = 0

    if SIMULATION:
        node_dict = __get_file(FILE_BUP_NODE)
    else:
        node_dict = __fetch_scheduler_node_data()
        if LOG_LEVEL == logging.DEBUG:
            __save_file(FILE_BUP_NODE, node_dict)

    if node_dict:

        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            logger.error("%s found, but dummy mode is not active", NODE_DUMMY)
            sys.exit(1)
        else:
            logger.error("%s not found, but dummy mode is active", NODE_DUMMY)

        for key, value in node_dict.items():
            logger.debug("key: %s - state: %s cpus %s real_memory %s", json.dumps(key), json.dumps(value['state']),
                         json.dumps(value['cpus']), json.dumps(value['real_memory']))
            # logger.debug(pformat(key))
            # logger.debug(pformat(value))
            if 'worker' in key:
                worker_count += 1
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    worker_in_use += 1
                elif NODE_DRAIN in value['state']:
                    worker_drain += 1
                elif ('DOWN' in value['state']) and 'IDLE' not in value['state']:
                    logger.error("workers are in DRAIN or DOWN state")
                else:
                    pass
                    # logger.info("worker in %s state", value['state'])
        logger.info("nodes: I found %d worker, %d ALLOCATED", worker_count, worker_in_use)
    else:
        logger.info("No nodes found !")

    return node_dict, worker_count, worker_in_use, worker_drain


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
    jobs_running = 0

    jobs_dict = __fetch_scheduler_job_data(14)

    if jobs_dict:
        for key, value in jobs_dict.items():
            if value['state'] == 0:
                jobs_pending += 1
                logger.debug("JOB PENDING: req_mem %s - req_cpus %s", value['req_mem'], value['req_cpus'])
            elif value['state'] == 1:
                jobs_running += 1
                logger.debug("JOB RUNNING: req_mem %s - req_cpus %s", value['req_mem'], value['req_cpus'])
    else:
        logger.info("No job found")
    logger.debug("Found %d pending jobs.", jobs_pending)

    return jobs_dict, jobs_pending, jobs_running


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
            version = res["VERSION"]
            if version != VERSION:
                print(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
                sys.exit(1)
            logger.debug(pformat(res))
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
            return int(fd['available_count'])
    return 0


def get_usable_flavors(cluster_id, cluster_pw):
    """
    receive flavor information from portal
    returned list:
    - the largest flavor on top
    - memory in GB (memory/1024)
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return: available flavors as json
    """

    res = requests.post(url=get_url_info_flavors(cluster_id),
                        json={"password": cluster_pw})

    logger.debug("requests: %s", res)
    if res.status_code == 200:
        flavors_data = res.json()
        # logger.info ( "flavorData %s",flavors_data )

        counter = 0
        flavors_data_cnt = []
        for fd in flavors_data:
            # logger.info ( "flavorData %s",fd )
            available_memory = __reduce_flavor_memory(int(fd['flavor']['ram']), )
            logger.info("flavor: %s - %s - ram: %s GB - cpu: %s - available: %sx - ephemeral_disk: %s real: %s - id %s",
                        counter,
                        fd['flavor']['name'],
                        fd['flavor']['ram'],
                        fd['flavor']['vcpus'],
                        fd['available_count'],
                        fd['flavor']['ephemeral_disk'],
                        available_memory,
                        fd['flavor']['id']
                        )
            counter += 1
            val_tmp = []
            for fd_item in fd.items():
                val_tmp.append(fd_item)
            val_tmp.append(('cnt', counter))
            val_tmp.append(('available_memory', available_memory))
            flavors_data_cnt.append(dict(val_tmp))
        # print("------flavors_data_cnt----------")
        # print(flavors_data_cnt)
        # print (pformat(list(flavors_data_cnt)))
        # print("------flavors_data----------")
        # print(flavors_data)
        # print(pformat(flavors_data))
        return list(flavors_data_cnt)
    else:
        logger.error(get_wrong_password_msg())
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
        logger.error("Error writing file %s", save_file)
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


def __set_cluster_password():
    """
    save cluster password to CLUSTER_PASSWORD_FILE
    the password is entered when prompted
    :return:
    """
    flavor_num = input("enter cluster password (copy&paste): ")

    tmp_pw = {'password': flavor_num}
    # backup cluster password
    __save_file(CLUSTER_PASSWORD_FILE, tmp_pw)


def __get_portal_auth():
    """
    read the authentication file
    :return: the authorization name and password
    """
    portal_auth = __get_file(FILE_PORTAL_AUTH)
    logger.debug(pformat(portal_auth))
    return portal_auth['server'], portal_auth['password']


def __worker_states(cluster_id, cluster_pw):
    """
    fetch cluster data from server and return worker statistics
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    url_cluster_info = get_url_info_cluster(cluster_id)

    logger.debug("checkWorkers: %s ", url_cluster_info)
    worker_active = 0
    worker_error = 0
    worker_down = 0
    worker_unknown = 0
    worker_planned = 0
    worker_err_list = ""

    if not SIMULATION:
        cluster_data = get_cluster_data(cluster_id, cluster_pw)
        # logger.debug(pformat(cluster_data))
        if not (cluster_data is None):
            for cl in cluster_data:
                logger.debug("hostname: '%s' status: '%s'", cl['hostname'], cl['status'])
                if 'DRAIN' in cl['status'] or 'DOWN' in cl['status']:
                    worker_down += 1
                elif 'PLANNED' in cl['status']:
                    worker_planned += 1
                    if worker_err_list != "":
                        worker_err_list = worker_err_list + "," + cl['hostname']
                    else:
                        worker_err_list = cl['hostname']
                elif cl['status'] == 'ERROR':
                    worker_error += 1
                    logger.error("ERROR %s", cl['hostname'])
                    if worker_err_list != "":
                        worker_err_list = worker_err_list + "," + cl['hostname']
                    else:
                        worker_err_list = cl['hostname']
                elif cl['status'] == 'ACTIVE':  # SCHEDULING -> BUILD -> (CHECKING_CONNECTION) -> ACTIVE
                    worker_active += 1
                else:
                    worker_unknown += 1
    return worker_active, worker_error, worker_down, worker_unknown, worker_err_list, worker_planned


def check_workers(cluster_id, cluster_pw):
    """
    fetch cluster data from server and check
    - all workers are active
    - remove broken workers
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    worker_ready = False
    num_tries = 20
    start_time = time.time()
    max_time = 1200
    while not worker_ready:
        worker_active, worker_error, worker_down, worker_unknown, worker_err_list, worker_planned \
            = __worker_states(cluster_id, cluster_pw)

        logger.debug("check: wa %s we %s wd %s wu %s wel %s wp %s", worker_active, worker_error, worker_down,
                     worker_unknown, worker_err_list, worker_planned)
        if ((time.time() - start_time) > max_time) and worker_planned > 0:
            logger.error("workers are stuck in panned: %s", worker_err_list)
            cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, worker_err_list, True)
            sys.exit(1)
        elif worker_unknown == 0 and worker_planned == 0:
            if worker_error > 0 >= num_tries and num_tries < 2:
                logger.error("scale down error workers: %s", worker_err_list)
                cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, worker_err_list, True)
                sys.exit(1)
            elif worker_error > 0 and num_tries > 0:
                num_tries -= 1
                time.sleep(WAIT_CLUSTER_SCALING_DOWN)
            else:
                logger.info("ALL WORKERS READY!!!")
                return True
        else:
            logger.info("at least one worker is not 'ACTIVE', wait ... %d seconds",
                        WAIT_CLUSTER_SCALING_UP)
            time.sleep(WAIT_CLUSTER_SCALING_UP)


def __generate_downscale_list(worker_data, count):
    """
        return all unallocated workers by name as json

        :param worker_data: worker data as json object
        :param count: number of workers
        :return: json list with idle workers,
                 example: {"password":"password","worker_hostnames":["host1","Host2",...]}
    """
    idle_workers_json = {'worker_hostnames': []}
    # sort workers by resources, memory 'real_memory' False low mem first - True high first
    # if the most capable worker should remain active
    worker_mem_sort = sorted(worker_data.items(), key=lambda k: (k[1]['real_memory']), reverse=False)
    for key, value in worker_mem_sort:
        logger.info("for : %d - key: %s ", count, key)
        if 'worker' in key and count > 0:
            # check for broken workers first
            if (NODE_ALLOCATED not in value['state']) and (NODE_MIX not in value['state']) and (
                    NODE_IDLE not in value['state']) and (NODE_DRAIN not in value['state']):
                # should never be reached, exit on DRAIN oder DOWN state @fetch_node_data @check_workers
                logger.error("worker is in unknown state: key %s  value %s", key, value['state'])
                idle_workers_json['worker_hostnames'].append(key)
            # elif (NODE_IDLE in value['state']) and (NODE_DRAIN in value['state']):
            #     idle_workers_json['worker_hostnames'].append(key)
            elif (NODE_ALLOCATED not in value['state']) and (NODE_MIX not in value['state']) and count > 0:
                idle_workers_json['worker_hostnames'].append(key)
                # logger.info("idle: %d - key: %s ", count, key)
                count -= 1
    logger.debug(json.dumps(idle_workers_json, indent=4, sort_keys=True))
    if idle_workers_json['worker_hostnames'].__sizeof__() == 0:
        logger.error("unable to generate downscale list")
        sys.exit(1)
    return idle_workers_json


def __calculate_scale_down_value(worker_count, worker_free, state):
    """
    retain number of free workers to delete
    respect boundaries like DOWNSCALE_LIMIT and SCALE_FORCE

    :param worker_count: number of total workers
    :param worker_free: number of idle workers
    :return: number of workers to scale down
    """

    if worker_count > LIMIT_DOWNSCALE:
        if worker_free < worker_count and not state == ScaleState.SCALE_DOWN_UP:
            return round(worker_free * SCALE_FORCE, 0)

        else:
            max_scale_down = worker_free - LIMIT_DOWNSCALE
            if max_scale_down > 0:
                return max_scale_down
            else:
                return 0
    else:
        return 0


def __similar_job_history(jobs_dict, job_pending_name, job_time_max, job_time_min, jobs_pending):
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
    job_count = 0
    job_time_sum: int = 0
    logger.debug("----- __similar_job_history -----")
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
    logger.debug("__calc_job_time_norm result %s jobs_pending %s SCALE_FORCE_JOB_WEIGHT %s", norm, jobs_pending,
                 SCALE_FORCE_JOB_WEIGHT)
    if not (norm is None):
        norm = norm + (jobs_pending * SCALE_FORCE_JOB_WEIGHT)
        # norm = norm * (jobs_pending * SCALE_FORCE_JOB_WEIGHT)
    logger.debug("__similar_job_history for %s - norm %s - job_time_sum %s - job_count %s", job_pending_name, norm,
                 job_time_sum, job_count)
    return norm


def __current_worker_capable_(job_, worker_json):
    """
    test if current workers are capable to process given job
    :param job_:
    :param worker_json:
    :return:
    """

    found_match = False
    # logger.debug("j_key %s j_value %s",j_key,j_value)
    for w_key, w_value in worker_json.items():
        # logger.debug("w_key %s w_value %s",w_key,w_value)
        if 'worker' in w_key:
            # logger.debug("j_value['req_mem'] %s w_mem_tmp %s",type(j_value['req_mem']),type(w_mem_tmp))
            w_mem_tmp = __reduce_flavor_memory(int(w_value['real_memory']) / 1000)
            if (job_['req_mem'] <= w_mem_tmp) and (
                    job_['req_cpus'] <= w_value['cpus']):
                found_match = True
                logger.debug("capable y - job mem %s worker mem %s, jcpu %s wcpu %s", job_['req_mem'],
                             w_mem_tmp, job_['req_cpus'], w_value['cpus'])
            else:
                logger.debug("capable n - job mem %s worker mem %s, jcpu %s wcpu %s", job_['req_mem'],
                             w_mem_tmp, job_['req_cpus'], w_value['cpus'])

    return found_match


def __current_workers_capable(job_priority, worker_json):
    """
    check if workers are capable for next job in queue

    :return:
    """

    found_pending = False
    not_capable_counter = 0
    for j_key, j_value in job_priority:
        if j_value['state'] == 0:  # pending jobs
            found_pending = True
            found_match = False
            # logger.debug("j_key %s j_value %s",j_key,j_value)
            for w_key, w_value in worker_json.items():
                # logger.debug("w_key %s w_value %s", w_key, w_value)
                if 'worker' in w_key:
                    # logger.debug("j_value['req_mem'] %s w_mem_tmp %s",type(j_value['req_mem']),type(w_mem_tmp))
                    w_mem_tmp = __reduce_flavor_memory(int(w_value['real_memory']))
                    if (j_value['req_mem'] <= w_mem_tmp) and (
                            j_value['req_cpus'] <= w_value['cpus']):
                        found_match = True
                        logger.debug("capable y - job mem %s worker mem %s, jcpu %s wcpu %s", j_value['req_mem'],
                                     w_mem_tmp, j_value['req_cpus'], w_value['cpus'])
                    else:
                        logger.debug("capable n - job mem %s worker mem %s, jcpu %s wcpu %s", j_value['req_mem'],
                                     w_mem_tmp, j_value['req_cpus'], w_value['cpus'])
            if not found_match:
                not_capable_counter += 1
                logger.debug("capable n - not_capable_counter %s", not_capable_counter)
    logger.debug("capable - found %s jobs without capable worker", not_capable_counter)
    if found_pending and not_capable_counter > 0:
        return False, not_capable_counter
    else:
        return True, not_capable_counter


def __worker_flavor_pre_check(job_priority, flavors_data):
    """
    return the expected number of workers with the same flavor
    :param job_priority:
    :param flavors_data:
    :return:
    """
    flavor_tmp_current = None
    counter = 0
    for key, value in job_priority:
        if value['state'] == 0:  # pending jobs
            logger.debug("flavor_tmp_current cpu: %s mem: %s c: %s", value['req_cpus'], value['req_mem'], counter)
            flavor_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], flavors_data)
            if flavor_tmp_current is None:
                flavor_tmp_current = flavor_tmp['flavor']['name']
                logger.debug("flavor_tmp_current is None: %s counter: %s", flavor_tmp_current, counter)
                counter += 1
            elif flavor_tmp_current == flavor_tmp['flavor']['name']:
                counter += 1
                logger.debug("flavor_tmp_current is same: %s counter: %s", flavor_tmp_current, counter)
            else:
                return counter
    logger.debug("flavor_tmp_current: %s counter: %s", flavor_tmp_current, counter)
    return counter


def set_nodes_to_drain(jobs_dict, worker_json, flavor_data):
    """
    calculate the largest worker for pending jobs
    search workers
    - if they are over the required resources, set them to drain state
    - if they have the required resources and are in drain state, remove drain state
    - use SCALE_FORCE_DRAIN_NODES as option
    :param jobs_dict:
    :param worker_json:
    :param flavor_data:
    :return:
    """
    missing_flavors = False
    fv_bup = __translate_cpu_mem_to_flavor("1", "1", flavor_data)  # lowest flavor
    if fv_bup:
        for key, value in jobs_dict.items():

            if value['state'] == 0:  # pending jobs
                logger.debug("set_nodes_to_drain: job %s", int(value['req_mem']))
                fv_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], flavor_data)
                if fv_tmp:
                    if fv_tmp['flavor']['ram'] >= fv_bup['flavor']['ram'] and fv_tmp['flavor']['vcpus'] >= \
                            fv_bup['flavor']['vcpus']:
                        fv_bup = fv_tmp
                else:
                    logger.debug("no suitable flavor available for job %s - maybe after downscale",
                                 int(value['req_mem']))
                    missing_flavors = True

        logger.debug("set_nodes_to_drain: %s", pformat(fv_bup))

        for w_key, w_value in worker_json.items():
            # logger.debug("w_key %s w_value %s",w_key,w_value)
            if 'worker' in w_key:
                logger.debug("set_nodes_to_drain: w_mem_tmp %s", int(w_value['real_memory']) / 1000)
                w_mem_tmp = __reduce_flavor_memory(int(w_value['real_memory']) / 1000)
                if missing_flavors and (NODE_DRAIN in w_value['state']):
                    logger.debug("missing_flavors: undrain %s - keep worker active!?", w_key)
                    # __set_node_to_undrain(w_key)  # TODO reactivate - or scale down and recheck resources ... ?
                elif fv_bup['flavor']['ram'] < w_mem_tmp and fv_bup['flavor']['vcpus'] < w_value['cpus'] and (
                        NODE_DRAIN not in w_value['state']) and not missing_flavors:
                    logger.debug("high worker here: set to drain %s", w_key)
                    __set_node_to_drain(w_key)
                elif (NODE_DRAIN in w_value['state']) and fv_bup['flavor']['ram'] <= w_mem_tmp and fv_bup['flavor'][
                    'vcpus'] <= w_value['cpus']:
                    logger.debug("high worker still required: undrain %s", w_key)
                    __set_node_to_undrain(w_key)


def __set_node_to_drain(w_key):
    """
    set scheduler option, nodes to drain
    - currently running jobs will keep running
    - no further job will be scheduled on that node
    :param w_key:
    :return:
    """
    # replace with slurm ide (currently not supported by this slurm version)
    os.system("sudo scontrol update nodename=" + str(w_key) + " state=drain reason=REPLACE")


def __set_node_to_undrain(w_key):
    """
    set scheduler option, undrain required nodes
    - further job will be scheduled on that node
    :param w_key:
    :return:
    """
    # replace with slurm ide (currently not supported by this slurm version)
    os.system("sudo scontrol update nodename=" + str(w_key) + " state=resume")


def __convert_to_high_workers(upscale_limit, flavors_data, flavor_tmp, job_targets):
    """

    :param upscale_limit: current worker limit
    :param flavors_data: all flavor data
    :param flavor_tmp: current calculated flavor
    :param job_targets: selected jobs for which a worker is required after calculation
    :return:
    """

    pending_jobs = len(job_targets)
    logger.debug("pending_jobs %s len(flavors_data) %s", pending_jobs, len(flavors_data))

    current_calc_flavor = None
    current_calc_flavor_tmp = None
    current_job_sum = 0
    current_worker_count = 0
    tmp_cpu = 0
    tmp_mem = 0
    # ---- select highest flavor
    for value in job_targets:
        value = list(value.values())[0]
        tmp_fv = __translate_cpu_mem_to_flavor(value['req_cpus'] + tmp_cpu, value['req_mem'] + tmp_mem, flavors_data)
        if not (tmp_fv is None):
            logger.debug("flavor name %s", tmp_fv['flavor']['name'])
        # first worker flavor, fix flavor
        if not (tmp_fv is None) and tmp_fv['available_count'] >= 1:
            # found flavor, check another loop
            tmp_mem += value['req_mem']
            tmp_cpu += value['req_cpus']
            current_job_sum += 1
            current_calc_flavor_tmp = tmp_fv
            logger.debug("__convert_to_high_workers selected %s for %s jobs %s in line, tmp_mem %s tmp_cpu %s",
                         tmp_fv['flavor']['name'], current_job_sum, pending_jobs - current_job_sum, tmp_mem, tmp_cpu)
        # worker flavor, tmp_fv
        elif current_job_sum > 1:  # we can combine multiple jobs into one flavor
            current_calc_flavor = current_calc_flavor_tmp  # created flavor limit with current_worker_count
            current_worker_count += 1
            logger.debug(
                "__convert_to_high_workers current_calc_flavor %s current_worker_count %s, tmp_mem %s tmp_cpu %s",
                current_calc_flavor['flavor']['name'], current_worker_count, tmp_mem, tmp_cpu)
            break
    # ---- calculate number of workers
    if current_calc_flavor is None:  # one flavor for all jobs
        current_calc_flavor = current_calc_flavor_tmp
        current_worker_count += 1
    if not (current_calc_flavor is None):
        max_cpu = int(current_calc_flavor['flavor']['vcpus'])
        max_mem = int(__reduce_flavor_memory(current_calc_flavor['flavor']['ram']))

        for value in job_targets:
            value = list(value.values())[0]
            if current_job_sum > 0:
                current_job_sum -= 1
                continue
            tmp_mem += value['req_mem']
            tmp_cpu += value['req_cpus']
            if tmp_mem > max_mem or tmp_cpu > max_cpu:
                if tmp_mem > max_mem / 2 and tmp_cpu > max_cpu / 2:
                    current_worker_count += 1
                    logger.debug("add one more worker %s", current_worker_count)
                    tmp_mem = 0
                    tmp_cpu = 0
                else:
                    logger.debug("one more worker would be inefficient %s tmp_mem %s max_mem %s", current_worker_count,
                                 tmp_mem, max_mem)
        logger.debug("__convert_to_high_workers: %s : %s ", current_calc_flavor, current_worker_count)
        return current_calc_flavor['flavor']['name'], current_worker_count
    return None, None


def __calculate_scale_up_data(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count, worker_json, state,
                              flavors_data):
    """
    create scale-up data

    adoptive mode
    - user selection for SCALE_FORCE and long_job_time
    - in regular bases, we do not want to create a new worker for any single job in pending state

    - calculate a foresighted upscale value:
      - SCALE_FORCE value (0 to 1) - strength of scaling - user selection
      - job_times - value (0 to 1) - previous jobs were time-consuming?
        - long job definition, user definition -  user selection - if (job_time > long_job_time); then 1
          - at what time is a job a long time job
        - job times from generic previous jobs - user selection
            v job times from similar previous jobs (__similar_job_history) - user selection
            - __calc_previous_job_time return a value from 0-1 to identify very short jobs
      - jobs_pending - how many jobs are in pending state

    - generate a worker adapted for the next job(s) in queue
      - job priority from slurm required
      - based on the next job in queue (with the highest priority)
      - worker selection based on priority (cpu and memory) requirements

    - limited by flavor - mode selection
      - we can only start one flavor in one pass
      - start only workers for jobs, if they have the same flavor
      - jobs with another flavor in the next call
      - jobs should be priority sorted by scheduler (slurm: cpu+memory -> priority)

    - SCALE_FORCE_WORKER_WEIGHT - user selection
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

    job_priority = __sort_job_priority(jobs_dict)
    # include previous job time
    job_time_max, job_time_min = __calc_job_time_range(jobs_dict)
    job_times = __calc_previous_job_time(jobs_dict, job_time_max, job_time_min)
    need_workers = False
    worker_capable, worker_not_capable = __current_workers_capable(job_priority, worker_json)

    # if worker_count > 0:
    #     worker_capable_value = worker_not_capable / worker_count
    # else:
    #     worker_capable_value = 1

    # if PREDICTION_FORCE:
    #     current_prediction = calculate_current_prediction()
    #     if not (current_prediction is None):
    #         current_force = (SCALE_FORCE + current_prediction) / 2
    #     else:
    #         current_force = SCALE_FORCE
    # else:
    #     current_force = SCALE_FORCE
    current_force = SCALE_FORCE  # remove PREDICTION_FORCE

    # precheck - how many workers in line, need the same flavor
    jobs_pending_counter = jobs_pending
    jobs_pending_flavor = __worker_flavor_pre_check(job_priority, flavors_data)
    logger.debug("jobs_pending %s jobs_pending_flavor %s", jobs_pending, jobs_pending_flavor)

    # reduce starting new workers based on the number of currently existing workers
    if SCALE_FORCE_WORKER:
        force = current_force - (SCALE_FORCE_WORKER_WEIGHT * worker_count)
        logger.debug("SCALE_FORCE_WORKER_WEIGHT: %s - worker_count %s", SCALE_FORCE_WORKER_WEIGHT, worker_count)
        logger.debug("SCALE_FORCE: %s - force %s", current_force, force)
        if force < 0.2:  # prevent too low scale force
            logger.debug("scale force too low: %s - reset 0.2", force)
            force = 0.2
    else:
        force = current_force

    # upscale_limit = int(round(jobs_pending * (job_times + force) / 2, 0))
    if not (job_times is None):
        upscale_limit = int(
            jobs_pending_flavor * (job_times + force) / 2)
        # logger.info("maybe we need %d new worker", upscale_limit)
    else:
        upscale_limit = int(jobs_pending_flavor * force)

    upscale_limit = upscale_limit + (upscale_limit % 2 > 0)

    logger.info("maybe we need %d new worker", upscale_limit)

    # test at least one worker
    if upscale_limit == 0 and JOB_TIME_SIMILAR_SEARCH:
        logger.debug("test at least one worker with similar search")
        upscale_limit = 1

    upscale_counter = upscale_limit
    flavor_tmp = ""
    up_scale_data = {}

    if state == ScaleState.SCALE_FORCE_UP or worker_count == 0 or not worker_capable:
        # if up scaling triggered and ...
        #   we need a scale up after a scale down with sufficient resources
        #   zero workers available
        #   workers are not capable for next jobs in queue
        need_workers = True
        logger.debug("scale_up need_workers %s upscale_limit %s", need_workers, upscale_limit)
        if upscale_limit == 0:
            upscale_limit = need_workers

    if upscale_limit > 0:
        cnt_worker_remove = 0
        job_targets = []  # backup selected jobs for high worker calculation
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

                    logger.debug("revaluate required worker with previous job name in history, %s ",
                                 JOB_TIME_SIMILAR_SEARCH)
                    if JOB_TIME_SIMILAR_SEARCH:
                        logger.debug("JOB_TIME_SIMILAR_SEARCH - jobs_pending %s jobs_pending_flavor %s", jobs_pending,
                                     jobs_pending_flavor)
                        job_time_norm = __similar_job_history(jobs_dict, value['jobname'], job_time_max, job_time_min,
                                                              jobs_pending_flavor)
                        if not (job_time_norm is None):
                            # recalculate for this job
                            # normalize the job time to 0-1 value
                            if job_time_norm < JOB_MATCH_SIMILAR:
                                logger.debug("NOT need this worker %s", job_time_norm)
                                upscale_limit -= 1
                                cnt_worker_remove += 1
                            else:
                                logger.debug("NEED this worker %s", job_time_norm)
                                job_targets.append({key: value})
                            # logger.info("__similar_job_history recalculate \nint(round(%s * (%s + %s) / 2, 0))=%s",
                            #             jobs_pending, job_time_norm, SCALE_FORCE,
                            #             int(round(jobs_pending * (job_time_norm + SCALE_FORCE) / 2, 0)))
                            logger.debug("--- automatic flavor selection disabled: %s ---", FLAVOR_FIX)
                        else:
                            job_targets.append({key: value})
                    else:
                        job_targets.append({key: value})
                    if not FLAVOR_FIX:
                        # only ONE flavor possible - I calculate one for every new worker
                        flavor_tmp_current = __translate_cpu_mem_to_flavor(value['req_cpus'],
                                                                           value['req_mem'],
                                                                           flavors_data)['flavor']['name']
                        # break up on flavor change
                        if not flavor_tmp and flavor_tmp_current:
                            flavor_tmp = flavor_tmp_current
                            logger.debug("FLAVOR_FIX new flavor - %s", flavor_tmp)
                        elif flavor_tmp == flavor_tmp_current and flavor_tmp:
                            logger.debug("FLAVOR_FIX same flavor - %s", flavor_tmp)
                        elif flavor_tmp and flavor_tmp_current and flavor_tmp != flavor_tmp_current:
                            logger.debug("FLAVOR_FIX reached another flavor - need to skip here %s - next %s",
                                         flavor_tmp, flavor_tmp_current)
                            break
                        else:
                            logger.debug("FLAVOR_FIX unable to select flavor")
                            break
                    else:
                        flavor_tmp = FLAVOR_DEFAULT
                else:
                    # break
                    logger.debug("- cpu %s mem %s priority %s ", value['req_cpus'], value['req_mem'], value['priority'])
                jobs_pending_counter -= 1
                jobs_pending_flavor -= 1
        logger.info("similar job search removed %d worker and the calculated flavor is: %s", cnt_worker_remove,
                    flavor_tmp)
        if need_workers and upscale_limit < 1:
            logger.info("scale force up %s, worker_count %s", state, worker_count)
            upscale_limit = need_workers
        elif SCALE_FORCE_HIGH_NODES:
            # convert to high workers if possible
            flavor_tmp, upscale_limit = __convert_to_high_workers(upscale_limit, flavors_data, flavor_tmp, job_targets)
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
        return None
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
    try:
        logger.debug("job_time_sum: %d job_num: %d", job_time_sum, job_num)
        if job_num > 0 and job_time_sum > 1:
            job_time_average = (job_time_sum / job_num)

            norm = float(job_time_average - job_time_min) / float(job_time_max - job_time_min)
            logger.debug("job_time_average: %d norm: %s", job_time_average, norm)

            if 1 > norm > 0.01:
                return norm
            else:
                logger.debug("job_time_average: miscalculation")
                if norm <= 0.01:
                    return 0.01
                else:
                    return 1

    except ZeroDivisionError:
        logger.debug('cannot divide by zero, unusable data')
    logger.debug("job_time_average: skip calculation")

    return None


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
        return None


def __scale_down_job_frequency(jobs_dict, jobs_pending):
    """
    limit scale down frequency
    compare with job history

    if jobs have started in a short interval, then it might not be worth deleting workers
    * analyze job history by hour and day
    * wait x minutes before scale down

    :return:
        True : if scale down allowed
        False: if scale down denied
    """
    if jobs_pending == 0:  # if pending jobs + scale-down initiated -> worker not suitable
        time_now = str((datetime.datetime.now().replace(microsecond=0).timestamp())).split('.')[0]
        current_prediction = calculate_current_prediction()
        if current_prediction is None:
            wait_time = SCALE_DOWN_FREQ * 60
        else:
            wait_time = SCALE_DOWN_FREQ * current_prediction * 60
        jobs_dict_rev = dict(reversed(list(jobs_dict.items()))).items()

        for key, value in jobs_dict_rev:
            # logger.debug(pformat(value))

            if value['state'] == 3:  # if last job is completed

                elapsed_time = float(time_now) - float(value['start'])
                logger.debug("elapsed_time: %s wait_time %s time_now %s job_start %s", elapsed_time, wait_time,
                             time_now,
                             value['start'])
                if elapsed_time < wait_time:
                    logger.info("prevent scale down by frequency")
                    return False
                break
    return True


def __multiscale_scale_down(cluster_id, cluster_pw, scale_state, worker_json, worker_down_cnt, jobs_dict, jobs_pending):
    """
    scale down from multiscale
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param scale_state: current scale state
    :param worker_json: worker information as json dictionary object
    :return: new scale state
    """

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


def __compare_custer_node_workers(cluster_data, worker_json):
    """
    compare cluster nodes with scheduler
    if a cluster worker is active, but not added to scheduler - possible broken worker
    :return: worker error list
    """
    # verify cluster workers with node data
    worker_err_list = ""
    if not (cluster_data is None):
        for cl in cluster_data:
            if not (cl['hostname'] in worker_json):
                if worker_err_list != "":
                    worker_err_list = worker_err_list + "," + cl['hostname']
                else:
                    worker_err_list = cl['hostname']
    if worker_err_list != "":
        logger.debug("__compare_custer_node_workers found missing workers in node data %s", worker_err_list)
    return worker_err_list


def __verify_cluster_workers(cluster_id, cluster_pw, cluster_data_old, worker_json, manual_run):
    """
    check via cluster api if workers are broken
    - delete workers if states on cluster are stuck at error, down, planned or drain
    - server side (check worker)
    """
    # logger.debug(pformat(cluster_data_new))
    cluster_data_new = get_cluster_data(cluster_id, cluster_pw)
    if manual_run:
        cluster_data_old = cluster_data_new
    if cluster_data_old is None:
        return cluster_data_new
    else:
        worker_err_list = __compare_custer_node_workers(cluster_data_new, worker_json)
        cluster_data_copy_tmp = []
        for cl in cluster_data_old:
            logger.debug("hostname: '%s' status: '%s'", cl['hostname'], cl['status'])
            for clt in cluster_data_new:
                if cl['hostname'] == clt['hostname'] and cl['status'] == clt['status']:
                    # logger.debug("no change")
                    cluster_data_copy_tmp.append(cl)
                    if 'DRAIN' in cl['status'] or 'DOWN' in cl['status'] or \
                            'ERROR' in cl['status'] or 'PLANNED' in cl['status']:
                        if worker_err_list != "":
                            worker_err_list = worker_err_list + "," + cl['hostname']
                        else:
                            worker_err_list = cl['hostname']

        if worker_err_list != "":
            logger.error("found broken workers on cluster, no state change %s in ", worker_err_list, SCALE_DELAY_WAIT)
            time.sleep(WAIT_CLUSTER_SCALING_DOWN)
            cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, worker_err_list, True)
            sys.exit(0)
    return cluster_data_copy_tmp


def __worker_same_states(worker_json, worker_copy):
    """
    create worker copy from scheduler
    - compare two worker objects, return only workers with the same state
    - remove most capable and active worker from list, due scheduler limitations
    :param worker_json: worker information as json dictionary object
    :param worker_copy: worker copy for comparison
    :return: worker object with workers stuck in the same state
    """
    if worker_copy is None:
        worker_copy = {}
        for key, value in worker_json.items():
            if 'worker' in key:
                worker_copy[key] = value
        return worker_copy
    else:
        worker_keep_alive = ""
        if NODE_KEEP:
            # workaround - if required from scheduler, the most capable worker should remain active
            worker_mem_sort = sorted(worker_json.items(), key=lambda k: (k[1]['real_memory']), reverse=True)

            for key, value in worker_mem_sort:
                if NODE_ALLOCATED in value['state'] or NODE_MIX in value['state'] or NODE_IDLE in value['state']:
                    logger.debug("keep_alive : key %s - : mem %s cpus %s", key, value['real_memory'], value['cpus'])
                    worker_keep_alive = key
                    break

        worker_copy_tmp = {}
        for key, value in worker_json.items():
            if key in worker_copy:
                if value['state'] == worker_copy[key]['state'] and key != worker_keep_alive:
                    worker_copy_tmp[key] = value
                    logger.debug("%s state: %s vs %s - state no change", key, value['state'], worker_copy[key]['state'])
                else:
                    logger.debug("%s state: %s vs %s - state recently changed or keep alive", key, value['state'],
                                 worker_copy[key]['state'])
        return worker_copy_tmp


def multiscale(cluster_id, cluster_pw):
    """
    make scaling decision with cpu and memory information
    evaluate cpu and memory information from pending jobs
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    """
    logger.debug("================= %s =================", inspect.stack()[0][3])
    __csv_log_with_refresh_data('M')
    state = ScaleState.SCALE_DELAY

    # create worker copy, only use delete workers after a delay, give scheduler and network time to activate
    worker_copy = None
    cluster_worker = None

    while state != ScaleState.SCALE_SKIP and state != ScaleState.SCALE_DONE:
        jobs_json, jobs_pending, jobs_running = receive_job_data()
        worker_json, worker_count, worker_in_use, worker_drain = receive_node_data()
        worker_free = (worker_count - worker_in_use)
        flavor_data = get_usable_flavors(cluster_id, cluster_pw)

        if SCALE_FORCE_DRAIN_NODES and ScaleState.SCALE_DELAY and jobs_pending > 0:
            # TODO scale down nodes in drain state: "ALLOCATED+DRAIN" -> "IDLE+DRAIN" | __generate_downscale_list
            set_nodes_to_drain(jobs_json, worker_json, flavor_data)

        # check for broken workers at cluster first
        cluster_worker = __verify_cluster_workers(cluster_id, cluster_pw, cluster_worker, worker_json, False)

        # create worker copy, only delete workers after a delay, give scheduler and network time to activate
        worker_copy = __worker_same_states(worker_json, worker_copy)

        # check workers'
        # - server side (check worker) - __verify_cluster_workers
        # - scheduler side (node information) - at scale down
        # save workers in list during delay , recheck if same states - __worker_same_states
        # - use only workers with no state change at scale down

        logger.info("worker_count: %s worker_in_use: %s worker_free: %s jobs_pending: %s", worker_count,
                    worker_in_use, worker_free, jobs_pending)
        # SCALE DOWN  # merge
        if worker_count > LIMIT_DOWNSCALE and worker_in_use == 0 and jobs_pending == 0:
            # generateDownScaleList(worker_json, worker_free)
            # workers are not in use and not reached DOWNSCALE_LIMIT
            # nothing to do here, just scale down any worker

            logger.info("---- SCALE DOWN - DELETE: workers are not in use")
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state), jobs_json,
                                            jobs_pending)
        elif worker_count > LIMIT_DOWNSCALE and worker_free >= 1 and worker_in_use > 0 and jobs_pending == 0:
            logger.info("---- SCALE DOWN - DELETE: workers are free and no jobs pending")
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state), jobs_json,
                                            jobs_pending)
        elif worker_count > LIMIT_DOWNSCALE and worker_free > round(SCALE_FORCE * 4, 0) and jobs_pending == 0:
            # if medium or high CPU usage, but multiple workers are idle
            # more workers are idle than the force allows
            # __generate_downscale_list(worker_json, round(SCALE_FORCE * 4, 0))

            logger.info("---- SCALE DOWN - DELETE: more workers %s are idle than the force %s allows", worker_free,
                        round(SCALE_FORCE * 4, 0))
            state = __multiscale_scale_down(cluster_id, cluster_pw, state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state), jobs_json,
                                            jobs_pending)
        # SCALE DOWN and UP
        elif worker_free > 0 and jobs_pending >= 1 and not state == ScaleState.SCALE_FORCE_UP:
            # required CPU/MEM usage from next pending job may over current worker resources ?
            # jobs pending, but workers are idle, possible low resources ?
            # -> if workers are free, but jobs are pending and CPU/MEM usage may not sufficient
            # -> SCALE DOWN - delete free worker with too low resources
            # start at least one worker, plus additional ones if a lot of jobs pending
            # - force upscale after downscale
            logger.info("---- SCALE DOWN_UP - worker_free and jobs_pending ----")
            if state == ScaleState.SCALE_DOWN_UP:
                logger.info("---- SCALE DOWN_UP - cluster_scale_down_specific ----")
                cluster_scale_down_specific(cluster_id, cluster_pw, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state))
                state = ScaleState.SCALE_FORCE_UP
                cluster_pw = __get_cluster_password()
            elif state == ScaleState.SCALE_DELAY:
                logger.info("---- SCALE DOWN_UP - SCALE_DELAY_WAIT ----")
                state = ScaleState.SCALE_DOWN_UP
                time.sleep(SCALE_DELAY_WAIT)
            else:
                state = ScaleState.SCALE_SKIP
                logger.info("---- SCALE DOWN_UP - condition changed - skip ----")
        # SCALE UP
        elif (worker_count == worker_in_use and jobs_pending >= 1) or state == ScaleState.SCALE_FORCE_UP:
            # -> if all workers are in use and jobs pending and jobs require more time
            # calculate
            #    -> SCALE_FORCE*JOBS_WAITING - check resources for every job + worker
            #        -> initiate scale up

            logger.info("---- SCALE UP - all workers are in use with pending jobs ----")
            if state == ScaleState.SCALE_DELAY:
                state = ScaleState.SCALE_UP
                time.sleep(SCALE_DELAY_WAIT)
            elif state == ScaleState.SCALE_UP:
                cluster_scale_up(cluster_id, cluster_pw, jobs_json, jobs_pending, worker_count, worker_json, state,
                                 flavor_data)
                state = ScaleState.SCALE_DONE
            elif state == ScaleState.SCALE_FORCE_UP:
                logger.debug("---- SCALE UP - force scale up ----")
                cluster_scale_up(cluster_id, cluster_pw, jobs_json, jobs_pending, worker_count, worker_json, state,
                                 flavor_data)
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

        logger.info("-----------#### scaling loop state: %s ####-----------", state.name)


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
    logger.debug("send cloud_api data: ")
    logger.debug(pformat(worker_data))
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
        return cluster_pw
    else:
        logger.debug("%s: JUST A DRY RUN!!", inspect.stack()[0][3])
        return ""


def cluster_scale_up(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count, worker_json, state, flavor_data):
    """

    :param worker_json: worker information as json dictionary object
    :param state: current scaling state
    :param worker_count: number of total workers
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param jobs_dict: jobs as json dictionary object
    :param jobs_pending: number of pending jobs
    :return:
    """

    data = __calculate_scale_up_data(cluster_id, cluster_pw, jobs_dict, jobs_pending, worker_count, worker_json, state,
                                     flavor_data)
    if not (data is None):
        cluster_pw = cloud_api(get_url_scale_up(cluster_id), data)
        rescale_slurm_cluster(cluster_id, cluster_pw)
        __csv_log_with_refresh_data('U')


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
    cluster_pw = cloud_api(get_url_scale_down(cluster_id), data)
    rescale_slurm_cluster(cluster_id, cluster_pw)


def cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, hostnames, rescale):
    """
    scale down with specific hostnames
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param hostnames: hostname list as string "worker1,worker2,worker..."
    :param rescale: if rescale is desired
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])

    worker_hostnames = '[' + hostnames + ']'
    logger.debug(hostnames)
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostnames}
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific(cluster_id))
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cluster_pw = cloud_api(get_url_scale_down_specific(cluster_id), data)
    if rescale:
        rescale_slurm_cluster(cluster_id, cluster_pw)


def cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_num):
    """
    scale down a specific number of workers, downscale list is self generated
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param worker_json: worker information as json dictionary object
    :param worker_num: number of worker to delete
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    if worker_num > 0:
        __cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_num)


def __cluster_scale_down_complete(cluster_id, cluster_pw):
    """
    scale down all slurm workers
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    worker_json, worker_count, worker_in_use, worker_drain = receive_node_data()
    __cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_count)


def __cluster_shut_down(cluster_id, cluster_pw):
    """
    scale down all workers by cluster data
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    cluster_data = get_cluster_data(cluster_id, cluster_pw)
    # logger.debug(pformat(cluster_data))
    worker_list = ""
    if not (cluster_data is None):
        for cl in cluster_data:
            if 'worker' in cl['hostname']:
                logger.debug("delete worker %s", cl['hostname'])
                if worker_list == "":
                    worker_list = cl['hostname']
                else:
                    worker_list += "," + cl['hostname']
        if worker_list != "":
            logger.debug("scale down %s", worker_list)
            time.sleep(WAIT_CLUSTER_SCALING_DOWN)
            cluster_scale_down_specific_hostnames(cluster_id, cluster_pw, worker_list, True)


def __cluster_scale_down_specific(cluster_id, cluster_pw, worker_json, worker_num):
    worker_hostname = __generate_downscale_list(worker_json, worker_num)
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostname['worker_hostnames']}
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific(cluster_id))
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cluster_pw = cloud_api(get_url_scale_down_specific(cluster_id), data)
    rescale_slurm_cluster(cluster_id, cluster_pw)
    __csv_log_with_refresh_data('D')


def rescale_slurm_cluster(cluster_id, cluster_pw):
    """
    apply the new worker configuration
    execute scaling, modify and run ansible playbook
    wait for workers, all should be ACTIVE (check_workers)
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    logger.debug("initiateScaling wait %s seconds", WAIT_CLUSTER_SCALING_DOWN)
    time.sleep(WAIT_CLUSTER_SCALING_DOWN)
    logger.debug("...")

    if check_workers(cluster_id, cluster_pw):
        rescale_init(cluster_id, cluster_pw)


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
    cluster_pw = cloud_api(get_url_scale_up(cluster_id), up_scale_data)
    rescale_slurm_cluster(cluster_id, cluster_pw)


def __cluster_scale_up_specific(cluster_id, cluster_pw, flavor, upscale_limit):
    """
    scale up cluster by flavor
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :param flavor: flavor name as string
    :param upscale_limit: scale up number of workers
    :return:
    """
    logger.info("----- test upscaling, start %d new worker nodes with %s -----", upscale_limit, flavor)
    up_scale_data = {"password": cluster_pw, "worker_flavor_name": flavor,
                     "upscale_count": upscale_limit}
    cluster_pw = cloud_api(get_url_scale_up(cluster_id), up_scale_data)
    rescale_slurm_cluster(cluster_id, cluster_pw)


def __cluster_scale_down_specific_test(cluster_id, cluster_pw):
    """
    scale down all idle worker
    :param cluster_id: current cluster identifier
    :param cluster_pw: current cluster password
    :return:
    """
    worker_json, worker_count, worker_in_use, worker_drain = receive_node_data()
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
    cluster_pw = cloud_api(get_url_scale_down_specific(cluster_id), data)
    rescale_slurm_cluster(cluster_id, cluster_pw)


def __print_help():
    print(textwrap.dedent("""\
        Option      Long option         Argument    Meaning
        -v          -version                        print the current version number
        -h          -help                           print this help information
        -fv         -flavors                        print available flavors
        -m          -mode               adoptive    scaling with a high adaptation
        -m          -mode               ...         select mode name from yaml file
        -p          -password                       set cluster password
        _                                           no argument - default mode (yaml config)
    """))


def __print_help_debug():
    if LOG_LEVEL == logging.DEBUG:
        print(textwrap.dedent("""\
        ---------------------------- < debug options > ----------------------------
        Option      Long option         Argument    Meaning
        -cw         -checkworker                    print worker from portal
        -nd         -node                           receive node data
        -l          -log                            create csv log entry
        -c          -clusterdata                    print cluster data
        -su         -scaleup                        scale up cluster by one worker (default flavor)
        -su         -scaleup            2           scale up cluster by a given number of workers (default flavor)
        -sus        -scaleupspecific    "flavor"    scale up one worker by given flavorname (check with -fv)
        -suc        -scaleupchoice                  scale up - interactive mode
        -sd         -scaledown                      scale down cluster, all idle workers
        -sds        -scaledownspecific  wk1,wk2,... scale down cluster by given workers
        -sdc        -scaledowncomplete              scale down all workers
        -csd        -clustershutdown                delete all workers from cluster (api)
        -cdb        -clusterbroken                  delete all broken workers from cluster (api)
        -rsc        -rescale                        run scaling with ansible playbook 
        -s          -service                        run as service (mode: default)
        -s          -service             mode       run as service
        """))


def setup_logger():
    # setup logger
    logger_ = logging.getLogger('')
    logger_.setLevel(LOG_LEVEL)
    fh = logging.FileHandler(LOG_FILE)
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
                                  datefmt='%a, %d %b %Y %H:%M:%S')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    logger_.addHandler(fh)
    logger_.addHandler(sh)
    return logger_


def create_pid_file():
    # create PID file
    pid = str(os.getpid())
    pidfile_ = AUTOSCALING_FOLDER + FILE_PID
    if os.path.isfile(pidfile_):
        logger.critical("pid file already exists %s, exit\n", pidfile_)
        sys.exit(1)
    open(pidfile_, 'w').write(pid)
    return pidfile_


def __setting_overwrite(mode_setting):
    """
    load config from yaml file
    test if all values are available
    """
    global PORTAL_LINK, SORT_JOBS_BY_MEMORY, SCALE_DELAY_WAIT, SCALE_FORCE, SCALE_FORCE_WORKER, \
        SCALE_FORCE_WORKER_WEIGHT, SCALE_FORCE_JOB_WEIGHT, PREDICTION_FORCE, PREDICTION_BALANCE_CPU, \
        PREDICTION_BALANCE_MEM, PREDICTION_BALANCE_MIN, SCALE_DOWN_FREQ, JOB_TIME_LONG, JOB_TIME_SHORT, \
        JOB_TIME_RANGE_AUTOMATIC, JOB_MATCH_VALUE, JOB_MATCH_SIMILAR, JOB_TIME_SIMILAR_SEARCH, FLAVOR_FIX, \
        FLAVOR_FIX, FLAVOR_DEFAULT, FLAVOR_EPHEMERAL, SCALE_FORCE_HIGH_NODES, SCALE_FORCE_DRAIN_NODES

    yaml_config = read_config_yaml()
    if 'portal_link' in yaml_config['portal']:
        PORTAL_LINK = yaml_config['portal']['portal_link']
    else:
        logger.error("missing portal_link")
        sys.exit(1)
    if mode_setting in yaml_config['mode']:
        sconf = yaml_config['mode'][mode_setting]
        # TODO replace global parameters - yaml object only ----------------------
        SORT_JOBS_BY_MEMORY = sconf['sort_jobs_by_memory']
        SCALE_DELAY_WAIT = sconf['scale_delay_wait']
        SCALE_FORCE = sconf['scale_force']
        SCALE_FORCE_WORKER = sconf['scale_force_worker']
        SCALE_FORCE_WORKER_WEIGHT = sconf['scale_force_worker_weight']
        SCALE_FORCE_JOB_WEIGHT = sconf['scale_force_job_weight']
        JOB_TIME_LONG = sconf['job_time_long']
        JOB_TIME_SHORT = sconf['job_time_short']
        JOB_TIME_RANGE_AUTOMATIC = sconf['job_time_range_automatic']
        JOB_MATCH_VALUE = sconf['job_match_value']
        JOB_MATCH_SIMILAR = sconf['job_match_similar']
        JOB_TIME_SIMILAR_SEARCH = sconf['job_match_search']
        FLAVOR_FIX = sconf['flavor_fix']
        FLAVOR_DEFAULT = sconf['flavor_default']
        FLAVOR_EPHEMERAL = sconf['flavor_ephemeral']
        SCALE_DOWN_FREQ = sconf['prediction_force']
        PREDICTION_BALANCE_CPU = sconf['prediction_balance_cpu']
        PREDICTION_BALANCE_MEM = sconf['prediction_balance_mem']
        PREDICTION_BALANCE_MIN = sconf['prediction_balance_min']
        SCALE_FORCE_HIGH_NODES = sconf['prefer_high_nodes']
        SCALE_FORCE_DRAIN_NODES = sconf['drain_high_nodes']

        # __update_slurm_config_set(sconf['scheduler_settings']) # depreciated
        __update_playbook_scheduler_config(sconf['scheduler_settings'])
    else:
        logger.error("unknown mode: %s", mode_setting)
        sys.exit(1)

    return yaml_config['mode'][mode_setting]


def __cluster_scale_up_choice():
    """
    scale up cluster
    * flavor number and scale up value can be specified after the call
      * it will be prompted interactively to enter the data
    :return:
    """
    flavors_data = get_usable_flavors(cid, cpw)
    flavor_num = input("flavor number: ")
    upscale_num = input("scale up number: ")
    if flavor_num.isnumeric() and upscale_num.isnumeric():
        try:
            flavor_num = int(flavor_num)
            upscale_num = int(upscale_num)
            if len(flavors_data) > flavor_num:
                flavor_available_cnt = flavors_data[flavor_num]['available_count']
                flavor_name = flavors_data[flavor_num]['flavor']['name']
                if flavor_available_cnt >= upscale_num:
                    logger.info("scale up %s - %sx", flavor_name, upscale_num)
                    time.sleep(WAIT_CLUSTER_SCALING_UP)
                    __cluster_scale_up_specific(cid, cpw, flavor_name, upscale_num)
                else:
                    logger.error("wrong scale up number")
            else:
                logger.error("wrong flavor number")
        except (ValueError, IndexError):
            logger.error("wrong values")
            sys.exit(1)


def __run_as_service(cluster_id, cluster_pw):
    # if NODE_DUMMY_REQ: # ansible update
    #     __update_dummy_worker()
    rescale_slurm_cluster(cluster_id, cluster_pw)

    while True:
        logger.debug("=== INIT ===")
        multiscale(cluster_id, cluster_pw)
        cluster_pw = __get_cluster_password()
        logger.debug("=== SLEEP ===")
        time.sleep(300)


if __name__ == '__main__':

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

    # setup logger
    logger = setup_logger()

    if len(sys.argv) == 3:
        if sys.argv[2] in ["-m", "--m", "-mode", "--mode"]:
            logger.warning("mode parameter: %s", sys.argv[2])
            sc = __setting_overwrite(sys.argv[2])
            logger.warning("settings overwrite successful")
        else:
            __setting_overwrite('default')
    else:
        __setting_overwrite('default')
    # temp csv log
    if len(sys.argv) == 2:
        if sys.argv[1] in ["-l", "--l", "-log", "--log"]:
            __csv_log_with_refresh_data('C')
            sys.exit(0)

    # create PID file
    pidfile = create_pid_file()
    try:

        logger.info('\n#############################################'
                    '\n########### START SCALING PROGRAM ###########'
                    '\n#############################################')

        cid = read_cluster_id()
        cpw = __get_cluster_password()

        if len(sys.argv) > 1:
            arg = sys.argv[1]
            logger.debug('autoscaling with %s: ', ' '.join(sys.argv))
            if len(sys.argv) == 2:
                if arg in ["-c", "--c", "-clusterdata", "--clusterdata"]:
                    pprint(get_cluster_data(cid, cpw))
                elif arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    __cluster_scale_up_test(cid, cpw, 1)
                elif arg in ["-suc", "--suc", "-scaleupchoice", "--scaleupchoice"]:
                    __cluster_scale_up_choice()
                elif arg in ["-sd", "--sd", "-scaledown", "--scaledown"]:
                    print('scale-down')
                    __cluster_scale_down_specific_test(cid, cpw)
                elif arg in ["-sdc", "--sdc", "-scaledowncomplete", "--scaledowncomplete"]:
                    logger.warning("warning: complete scale down in %s sec ...", WAIT_CLUSTER_SCALING_DOWN)
                    time.sleep(WAIT_CLUSTER_SCALING_DOWN)
                    __cluster_scale_down_complete(cid, cpw)
                elif arg in ["-csd", "--csd", "-clustershutdown", "--clustershutdown"]:
                    logger.warning("warning: delete all worker from cluster in %s sec ...", WAIT_CLUSTER_SCALING_DOWN)
                    time.sleep(WAIT_CLUSTER_SCALING_DOWN)
                    __cluster_shut_down(cid, cpw)
                elif arg in ["-cdb", "--cdb", "-clusterbroken", "--clusterbroken"]:
                    logger.warning("warning: delete all broken worker from cluster in %s sec ...",
                                   WAIT_CLUSTER_SCALING_DOWN)
                    time.sleep(WAIT_CLUSTER_SCALING_DOWN)
                    worker_j, _, _, _ = receive_node_data()
                    __verify_cluster_workers(cid, cpw, None, worker_j, True)
                elif arg in ["-rsc", "--rsc", "-rescale", "--rescale"]:
                    print('rerun scaling')
                    rescale_slurm_cluster(cid, cpw)
                elif arg in ["-fv", "--fv", "-flavors", "--flavors"]:
                    print('flavors available:')
                    get_usable_flavors(cid, cpw)
                elif arg in ["-cw", "--cw", "-checkworker", "--checkworker"]:
                    check_workers(cid, cpw)
                elif arg in ["-nd", "--nd", "-node", "--node"]:
                    receive_node_data()
                elif arg in ["-s", "-service", "--service"]:
                    __run_as_service(cid, cpw)
                else:
                    print("No usage found for param: ", arg)
                sys.exit(0)
            elif len(sys.argv) == 3:
                arg2 = sys.argv[2]
                if arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    if arg2.isnumeric():
                        __cluster_scale_up_test(cid, cpw, int(arg2))
                    else:
                        logger.error("we need a number, for example: -su 2")
                        sys.exit(1)
                elif arg in ["-sus", "--sus", "-scaleupspecific", "--scaleupspecific"]:
                    print('scale-up')
                    __cluster_scale_up_specific(cid, cpw, arg2, 1)
                elif arg in ["-sds", "--sds", "-scaledownspecific", "--scaledownspecific"]:
                    print('scale-down-specific')
                    if "worker" in arg2:
                        cluster_scale_down_specific_hostnames(cid, cpw, arg2, True)
                    else:
                        logger.error("hostname parameter without 'worker'")
                        sys.exit(1)
                elif arg in ["-s", "-service", "--service"]:
                    __setting_overwrite(arg2)
                    __run_as_service(cid, cpw)
                else:
                    print("No usage found for param: ", arg, arg2)
                    sys.exit(1)
                sys.exit(0)
        else:
            print('\npass a mode via parameter, use -h for help')
            multiscale(cid, cpw)
    finally:
        os.unlink(pidfile)

    sys.exit(0)
