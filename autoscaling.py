#!/usr/bin/python3

import difflib
import os
import time
import sys
import re
import datetime
import json
import textwrap
import enum
import logging
import inspect
import csv
import yaml
from pathlib import Path
from pprint import pformat, pprint
from functools import total_ordering
from subprocess import Popen, PIPE
import requests
import subprocess
import random
import hashlib
from multiprocessing import Pool, Process
import signal

LOG_LEVEL = logging.INFO  # DEBUG , INFO
SIMULATION = False
VERSION = "0.3.3"
OUTDATED_SCRIPT_MSG = "Your script is outdated [VERSION: {SCRIPT_VERSION} - latest is {LATEST_VERSION}] " \
                      "-  please download the current version and run it again!"
PORTAL_AUTH = False

# ----- SYSTEM PARAMETERS -----
AUTOSCALING_FOLDER = os.path.dirname(os.path.realpath(__file__)) + '/'
IDENTIFIER = "autoscaling"
SOURCE_LINK = ""  # TODO
SOURCE_LINK_CONFIG = ""

FILE_BUP_NODE = AUTOSCALING_FOLDER + 'backup_node.json'
FILE_BUP_JOBS = AUTOSCALING_FOLDER + 'backup_jobs.json'

FILE_CONFIG_YAML = AUTOSCALING_FOLDER + IDENTIFIER + '_config.yaml'
FILE_PORTAL_AUTH = AUTOSCALING_FOLDER + 'portal_auth.json'
FILE_PID = IDENTIFIER + ".pid"

CLUSTER_PASSWORD_FILE = AUTOSCALING_FOLDER + 'cluster_pw.json'  # {"password":"PASSWORD"}

LOG_FILE = AUTOSCALING_FOLDER + IDENTIFIER + '.log'
LOG_CSV = AUTOSCALING_FOLDER + IDENTIFIER + '.csv'

NORM_HIGH = 1
NORM_LOW = 0.0001
FORCE_LOW = 0.0001

# ----- NODE STATES -----
NODE_ALLOCATED = "ALLOC"
NODE_MIX = "MIX"
NODE_IDLE = "IDLE"
NODE_DRAIN = "DRAIN"
NODE_DRAINING = "DRNG"
NODE_DOWN = "DOWN"
NODE_COMP = "COMP"
NODE_KEEP = False  # keep a high worker (if required from scheduler), set LIMIT_DOWNSCALE to 1
NODE_DUMMY = "bibigrid-worker-autoscaling_dummy"
NODE_DUMMY_REQ = True
WORKER_SCHEDULING = "SCHEDULING"
WORKER_PLANNED = "PLANNED"
WORKER_ERROR = "ERROR"
WORKER_FAILED = "FAILED"
WORKER_PORT_CLOSED = "PORT_CLOSED"
WORKER_ACTIVE = "ACTIVE"

# ----- JOB STATE IDs -----
JOB_FINISHED = 3
JOB_PENDING = 0
JOB_RUNNING = 1

LIMIT_DOWNSCALE = 0  # 1, if scheduler required
WAIT_CLUSTER_SCALING = 10  # wait seconds
FLAVOR_MULTI_STARTS = False

# ----- MODE PARAMETERS -----

LONG_TIME_DATA = True
DATABASE_FILE = AUTOSCALING_FOLDER + IDENTIFIER + '_database.json'
DATA_HISTORY = 2
DATA_LONG_TIME = 30

# ---- rescale cluster ----
HOME = str(Path.home())
PLAYBOOK_DIR = HOME + '/playbook'
PLAYBOOK_VARS_DIR = HOME + '/playbook/vars'
ANSIBLE_HOSTS_FILE = PLAYBOOK_DIR + '/ansible_hosts'
INSTANCES_YML = PLAYBOOK_VARS_DIR + '/instances.yml'
SCHEDULER_YML = PLAYBOOK_VARS_DIR + '/scheduler_config.yml'


@total_ordering
class ScaleState(enum.Enum):
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
    INIT = 0
    CHECK = 1
    NONE = 2


class TerminateProtected:
    """ Protect a piece of code from being killed by SIGINT or SIGTERM.
    It can still be killed by a force kill.

    Example:
        with TerminateProtected():
            run_func_1()
            run_func_2()

    Both functions will be executed even if a sigterm or sigkill has been received.
    """
    killed = False

    def _handler(self, signum, frame):
        logging.error("Received SIGINT or SIGTERM! Finishing this block, then exiting.")
        self.killed = True

    def __enter__(self):
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, type, value, traceback):
        if self.killed:
            sys.exit(0)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)


class SlurmInterface:
    def __init__(self):
        pass

    def scheduler_function_test(self):
        """
        Test if scheduler can retrieve job and node data.
        :return: boolean, success
        """
        # try node data
        try:
            pyslurm.node()
        except ValueError as e:
            logger.error("Error: unable to receive node data \n%s", e)
            return False
        # try job data
        try:
            pyslurm.slurmdb_jobs()
        except ValueError as e:
            logger.error("Error: unable to receive job data \n%s", e)
            return False
        return True

    def fetch_scheduler_node_data(self):
        """
        Read scheduler data from database and return a json object with node data.
        Verify database data.
        Example:
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
        :return
            - json object with node data,
            - on error, return None
        """
        try:
            nodes = pyslurm.node()
            node_dict = nodes.get()

            # make sure db data is up-to-date
            # db problem
            async_db = True
            retry_counter = 4
            while async_db and retry_counter > 0:
                async_db = False
                retry_counter -= 1
                nodes_live = self.node_data_live()
                if node_dict and nodes_live:
                    for key, value in list(nodes_live.items()):
                        if key in node_dict:
                            if not ((NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state'])):
                                if (NODE_ALLOCATED in node_dict[key]['state'] or NODE_MIX in node_dict[key]['state']):
                                    logger.debug("Node data not match %s - %s - %s", key, value['state'],
                                                 node_dict[key]['state'])
                                    async_db = True
                        else:
                            logger.debug("Missing key in db %s", key)
                            async_db = True
                elif not node_dict and nodes_live:
                    async_db = True
                    logger.debug("Node db is empty sinfo %s", nodes_live)
                if async_db and retry_counter > 0:
                    time.sleep(2)
                    node_dict = nodes.get()  # try again
            if async_db:
                logger.debug("Node data is still not in sync, give up")
            return node_dict
        except ValueError as e:
            logger.error("Error: unable to receive node data \n%s", e)
            return None

    def get_json(self, slurm_command):
        """
        Get all slurm data as string from squeue or sinfo
        :param slurm_command:
        :return: json data from slurm output
        """
        process = Popen([slurm_command, '-o', '%all'], stdout=PIPE, stderr=PIPE, shell=False, universal_newlines=True)
        proc_stdout, proc_stderr = process.communicate()
        lines = proc_stdout.split('\n')
        header_line = lines.pop(0)
        header_cols = header_line.split('|')
        entries = []
        # error_lines = []
        for line in lines:
            parts = line.split('|')
            d = {}
            if len(parts) == len(header_cols):
                for i, key in enumerate(header_cols):
                    d[key] = parts[i]
                entries.append(d)

        # print(json.dumps(entries, indent = 2))
        return entries

    def __convert_node_state(self, state):
        """
        Convert sinfo state to detailed database version.
            - not possible to identify MIX+DRAIN
        :param state: node state to convert
        :return:
        """
        state = state.upper()
        if "DRNG" in state:
            return NODE_ALLOCATED + "+" + NODE_DRAIN
        elif "DRAIN" in state:
            return NODE_IDLE + "+" + NODE_DRAIN
        elif "ALLOC" in state:
            return NODE_ALLOCATED
        elif "IDLE" in state:
            return NODE_IDLE
        elif "MIX" in state:
            return NODE_MIX
        elif "DOWN" in state:
            return NODE_DOWN
        # elif state == NODE_COMP:  # completing, new state incoming
        #     return NODE_ALLOCATED
        else:
            return state

    def node_data_live(self):
        """
        Receive node data from sinfo.
        :return: json object
        """

        node_dict = self.get_json('sinfo')
        node_dict_format = {}
        for i in node_dict:
            node_dict_format.update(
                {i['HOSTNAMES']:
                     {'cpus': int(i['CPUS']),
                      'real_memory': int(i['MEMORY']),
                      'state': self.__convert_node_state(i['STATE']),
                      'tmp_disk': int(i['TMP_DISK']),
                      'name': i['HOSTNAMES'],
                      }
                 }
            )
        # print(node_dict_format)
        return node_dict_format

    def job_data_live(self):
        """
        Fetch live data from scheduler without database usage.
        Use squeue output from slurm, should be faster up-to-date
        :return: return current job data (no history)
            - pending job list
            - running job list
        """
        # print("###### squeue_data #######")
        squeue_json = self.get_json('squeue')
        job_live_dict_pending = {}
        job_live_dict_running = {}
        for i in squeue_json:
            mem = re.split('(\d+)', i['MIN_MEMORY'])
            disk = re.split('(\d+)', i['MIN_TMP_DISK'])
            # convert memory to iB, according to slurm db
            if i['STATE'] == 'PENDING':
                job_live_dict_pending.update(
                    {i['JOBID']:
                         {'jobid': int(i['JOBID']),
                          'req_cpus': int(i['MIN_CPUS']),
                          'req_mem': self.memory_size_ib(mem[1], mem[2]),
                          'state': JOB_PENDING,
                          'tmp_disk': self.memory_size(disk[1], disk[2]),
                          'priority': int(i['PRIORITY']),
                          'jobname': i['NAME']
                          }
                     }
                )
            elif i['STATE'] == "RUNNING":
                job_live_dict_running.update(
                    {i['JOBID']:
                         {'jobid': int(i['JOBID']),
                          'req_cpus': int(i['MIN_CPUS']),
                          'req_mem': self.memory_size_ib(mem[1], mem[2]),
                          'state': JOB_RUNNING,
                          'tmp_disk': self.memory_size_ib(disk[1], disk[2]),
                          'priority': int(i['PRIORITY']),
                          'jobname': i['NAME']
                          }
                     }
                )
        # print (pformat(job_live_dict_pending))
        return job_live_dict_pending, job_live_dict_running

    def memory_size_ib(self, num, ending):
        if ending == 'G':
            tmp_disk = convert_gb_to_mib(num)
        elif ending == 'M':
            tmp_disk = int(num)
        elif ending == 'T':
            tmp_disk = convert_tb_to_mib(num)
        else:
            tmp_disk = int(num)
        return tmp_disk

    def memory_size(self, num, ending):
        if ending == 'G':
            tmp_disk = convert_gb_to_mb(num)
        elif ending == 'M':
            tmp_disk = int(num)
        elif ending == 'T':
            tmp_disk = convert_tb_to_mb(num)
        else:
            tmp_disk = int(num)
        return tmp_disk

    def add_jobs_tmp_disk(self, jobs_dict):
        """
        Collect slurm tmp disc data and merge with job dictionary.
        :param jobs_dict: modified jobs_dict
        :return:
        """
        output = Popen(['squeue', '-o "%A %Q %d"'], stdout=PIPE, stderr=PIPE)
        for line in iter(output.stdout.readline, b''):
            words = line.rstrip().decode().replace('"', '')
            x = words.split()
            match = re.match(r"([0-9]+)([A-Z]+)", x[2], re.I)
            if match:
                items = match.groups()
                if len(items) == 2:
                    tmp_disk = self.memory_size(items[0], items[1])
                else:
                    tmp_disk = int(items[0])

                try:
                    if tmp_disk:
                        if jobs_dict.get(int(x[0]), None):
                            jobs_dict[int(x[0])]['tmp_disk'] = tmp_disk
                            # logger.debug('mod job %s disk %s', x[0], tmp_disk)
                        # else:
                        #     logger.debug(" not found in dict, job id %s", x[0])
                except (ValueError, IndexError):
                    logger.error("squeue output unusabe ")
            try:
                # fix for async priority data in database
                if x[1].isdigit():
                    if jobs_dict[int(x[0])]['priority'] != int(x[1]):
                        logger.debug("job priority for id %s - %s to %s - req_cpus: %s req_mem: %s", x[0],
                                     jobs_dict[int(x[0])]['priority'], x[1], jobs_dict[int(x[0])]['req_cpus'],
                                     jobs_dict[int(x[0])]['req_mem'])
                        jobs_dict[int(x[0])]['priority'] = int(x[1])
                    # else:
                    #     logger.debug("match job priority for id %s - %s to %s - req_cpus: %s req_mem: %s", x[0],
                    #                  jobs_dict[int(x[0])]['priority'], x[1], jobs_dict[int(x[0])]['req_cpus'],
                    #                  jobs_dict[int(x[0])]['req_mem'])
                # else:
                #     logger.debug("x[1] is no digit")
            except KeyError:
                logger.debug("KeyError, job id from squeue %s not in db", x[0])
            except (ValueError, IndexError):
                logger.error("squeue output unusabe ")

        for key, entry in jobs_dict.items():
            if not ('tmp_disk' in entry):
                jobs_dict[key]['tmp_disk'] = 0
        return dict(jobs_dict)

    def fetch_scheduler_job_data_by_range(self, start, end):
        """
        Read scheduler data from database and return a json object with job data.
        Define the prerequisites for the json data structure and the required data.

        Job states:
            - 0: PENDING
            - 1: RUNNING
            - 2:
            - 3: COMPLETED
        squeue -o "%A %C %m %Q %N"

        Example:
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

        :param start: start time range
        :param end: end time range
        :return json object with job data, return None on error
        """
        try:
            jobs = pyslurm.slurmdb_jobs()
            jobs_dict = jobs.get(starttime=start.encode("utf-8"), endtime=end.encode("utf-8"))

            # if conf_mode['tmp_disk_check']:
            jobs_dict = self.add_jobs_tmp_disk(jobs_dict)
            # print(pformat(jobs_dict))
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
        start = (datetime.datetime.utcnow() - datetime.timedelta(days=num_days)).strftime("%Y-%m-%dT00:00:00")
        end = (datetime.datetime.utcnow() + datetime.timedelta(days=num_days)).strftime("%Y-%m-%dT00:00:00")
        return self.fetch_scheduler_job_data_by_range(start, end)

    def fetch_scheduler_job_data_in_seconds(self, num_seconds):
        """
        Read job data from database and return a json object with job data.
        :param num_seconds: number of past seconds
        :return: jobs_dict
        """
        start = (datetime.datetime.utcnow() - datetime.timedelta(seconds=num_seconds)).strftime("%Y-%m-%dT00:00:00")
        end = (datetime.datetime.utcnow() + datetime.timedelta(seconds=num_seconds)).strftime("%Y-%m-%dT00:00:00")
        logger.debug("job data range start %s end %s", start, end)
        return self.fetch_scheduler_job_data_by_range(start, end)

    def set_node_to_drain(self, w_key):
        """
        Set scheduler option, node to drain.
            - currently running jobs will keep running
            - no further job will be scheduled on that node
        :param w_key: node name
        :return:
        """
        # replace with slurm ide (currently not supported by this slurm version)
        logger.debug("drain node %s  ", w_key)
        os.system("sudo scontrol update nodename=" + str(w_key) + " state=drain reason=REPLACE")

    def set_node_to_undrain(self, w_key):
        """
        Set scheduler option, undrain required nodes
            - further job will be scheduled on that node
        :param w_key: node name
        :return:
        """
        # replace with slurm ide (currently not supported by this slurm version)
        logger.debug("undrain node %s  ", w_key)
        os.system("sudo scontrol update nodename=" + str(w_key) + " state=resume")


class ScalingDown:

    def __init__(self, password, dummy_worker):
        self.password = password
        self.dummy_worker = dummy_worker
        self.data = self.get_scaling_down_data()
        if self.data is None:
            logger.error("get scaling down data: None")
            return
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
        res = requests.post(url=get_url_info_cluster(),
                            json={"scaling": "scaling_down", "password": self.password, "version": VERSION},
                            )
        if res.status_code == 200:
            data_json = res.json()
            version = data_json["VERSION"]
            if version != VERSION:
                # print(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
                logger.error(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
                # sys.exit(1)
            # version_check(version)
        elif res.status_code == 401:
            # print(get_wrong_password_msg())
            logger.error(get_wrong_password_msg())
            sys.exit(1)
        else:
            logger.error("server error - unable to receive scale down data")
            return None
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
                            # instances_mod['deletedWorkers'].append(worker) # TODO required?
                            pass
                        # rescue real worker data
                        elif not (self.dummy_worker is None):
                            if worker['ip'] != self.dummy_worker['ip']:
                                instances_mod['workers'].append(worker)

                stream.seek(0)
                stream.truncate()
                yaml.dump(instances_mod, stream)
                # logger.debug("removed version:")
                # logger.debug(pformat(instances_mod))
            except yaml.YAMLError as exc:
                logger.error("YAMLError %s", exc)
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
        if self.data is None:
            logger.error("get_scaling_down_data None")
            return
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

        res = requests.post(url=get_url_info_cluster(),
                            json={"scaling": "scaling_up", "password": self.password, "version": VERSION})
        if res.status_code == 200:
            res = res.json()
            version_check(res["VERSION"])
            return res
        elif res.status_code == 401:
            print(get_wrong_password_msg())
            logger.error(get_wrong_password_msg())
            sys.exit(1)
        else:
            logger.error("server error - unable to receive cluster data")
            return None

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
                if not (self.dummy_worker is None):
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
                logger.error("YAMLError %s", exc)
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
                        logger.error("YAMLError %s", exc)
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


def run_ansible_playbook():
    """
    Run ansible playbook with system call.
        - ansible-playbook -v -i ansible_hosts site.yml
    :return: boolean, success
    """
    os.chdir(PLAYBOOK_DIR)
    forks_num = str(os.cpu_count() * 4)
    try:
        __csv_log_with_refresh_data('A', '0', '0')
        subprocess.run(['ansible-playbook', '--forks', forks_num, '-v', '-i', 'ansible_hosts', 'site.yml'],
                       check=True)
        logger.debug("ansible playbook success")
        __csv_log_with_refresh_data('B', '0', '1')
        return True
    except subprocess.CalledProcessError:
        __csv_log_with_refresh_data('B', '0', '2')
        logger.error("subprocess failed running ansible-playbook")
        return False


def get_dummy_worker(flavors_data):
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
    ephemeral_disk = 0
    if len(flavors_data) > 0:
        max_memory = convert_gb_to_mib(flavors_data[0]['flavor']['ram'])
        # __reduce_flavor_memory by ansible
        if 'ephemeral_disk' in flavors_data[0]['flavor']:
            ephemeral_disk = int(flavors_data[0]['flavor']['ephemeral_disk'])

        max_cpu = flavors_data[0]['flavor']['vcpus']

        if ephemeral_disk == 0:
            for fd in flavors_data:
                if 'ephemeral_disk' in fd['flavor']:
                    if int(fd['flavor']['ephemeral_disk']) > 0:
                        ephemeral_disk = int(fd['flavor']['ephemeral_disk'])
                        break
    else:
        logger.error("%s flavors available ", len(flavors_data))
        max_memory = 512
        max_cpu = 0

    dummy_worker = {'cores': max_cpu, 'ephemeral_disk': ephemeral_disk, 'ephemerals': [], 'hostname': NODE_DUMMY,
                    'ip': '0.0.0.4', 'memory': max_memory, 'status': 'ACTIVE'}
    logger.debug("dummy worker data: %s", dummy_worker)
    return dummy_worker


def rescale_init():
    """
    Combine rescaling steps
        - update dummy worker
        - start scale function to generate and update the ansible playbook
        - run the ansible playbook
    :return: boolean, success
    """
    flavors_data = get_usable_flavors(True, False)
    if flavors_data:
        if NODE_DUMMY_REQ:
            dummy = get_dummy_worker(flavors_data)
        else:
            dummy = None
        logger.debug("calculated dummy: %s", pformat(dummy))
        ScalingDown(password=cluster_pw, dummy_worker=dummy)
        ScalingUp(password=cluster_pw, dummy_worker=dummy)
        logger.debug("--- run playbook ---")
        return run_ansible_playbook()
    return False


def get_version():
    """
    Print the current autoscaling version.
    :return:
    """
    print("Version: ", VERSION)


def __update_playbook_scheduler_config(set_config):
    """
    Update playbook scheduler configuration, skip with error message when missing.
    :return:
    """
    # SCHEDULER_YML
    scheduler_config = __read_yaml_file(SCHEDULER_YML)
    if scheduler_config is None:
        logger.error("unable to update scheduler config")
    # logger.debug(pformat(scheduler_config))
    elif scheduler_config['scheduler_config']['priority_settings']:
        scheduler_config['scheduler_config']['priority_settings'] = set_config
        # logger.debug(pformat(scheduler_config['scheduler_config']['priority_settings']))
        ___write_yaml_file(SCHEDULER_YML, scheduler_config)
        logger.debug("scheduler config updated")
    else:
        logger.error("scheduler_config, missing: scheduler_config, scheduler_settings")


def __count_current_cpu_mem_usage(worker_json, jobs_json):
    """
    DEBUG
    For csv log only, temporary logging.
    :param worker_json: worker information as json dictionary object
    :param jobs_json:
    :return:
    """
    cpu_alloc, mem_alloc, mem_alloc_drain, mem_alloc_no_drain = 0, 0, 0, 0
    mem_mix_drain, mem_mix_no_drain = 0, 0

    cpu_idle, mem_idle, mem_idle_drain, mem_idle_no_drain = 0, 0, 0, 0
    jobs_cpu_alloc, jobs_mem_alloc = 0, 0
    jobs_cpu_pending, jobs_mem_pending = 0, 0
    if jobs_json:
        for key, value in jobs_json.items():
            if value['state'] == JOB_PENDING:
                jobs_cpu_pending += value['req_cpus']
                jobs_mem_pending += value['req_mem']
            elif value['state'] == JOB_RUNNING:
                jobs_cpu_alloc += value['req_cpus']
                jobs_mem_alloc += value['req_mem']
    if worker_json:
        for key, value in worker_json.items():
            # logger.debug("key: %s - value: %s", json.dumps(key), json.dumps(value['state']))
            if 'worker' in key:
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    cpu_alloc += value['cpus']
                    mem_alloc += value['real_memory']
                    if NODE_DRAIN in value['state']:
                        if NODE_ALLOCATED in value['state']:
                            mem_alloc_drain += value['real_memory']
                        elif NODE_MIX in value['state']:
                            mem_mix_drain += value['real_memory']
                    else:
                        if NODE_ALLOCATED in value['state']:
                            mem_alloc_no_drain += value['real_memory']
                        elif NODE_MIX in value['state']:
                            mem_mix_no_drain += value['real_memory']
                elif NODE_IDLE in value['state']:
                    cpu_idle += value['cpus']
                    mem_idle += value['real_memory']
                    if NODE_DRAIN in value['state']:
                        mem_idle_drain += value['real_memory']
                    else:
                        mem_idle_no_drain += value['real_memory']

    return cpu_alloc, mem_alloc, cpu_idle, mem_idle, \
           jobs_cpu_alloc, jobs_mem_alloc, jobs_cpu_pending, jobs_mem_pending, \
           mem_alloc_drain, mem_alloc_no_drain, mem_idle_drain, mem_idle_no_drain, \
           mem_mix_drain, mem_mix_no_drain


def upscale_limit_log(marker, data, lvl):
    """
    DEBUG
    Log steps from scale up data.
    :param marker: step identification
    :param data: backup data
    :return:
    """
    if LOG_LEVEL != logging.DEBUG:
        return
    log_csv_upscale = AUTOSCALING_FOLDER + '/autoscaling_upscale_limits.csv'
    time_now = __get_time()
    w_data = [time_now, marker, data, lvl]
    # test if exit and not empty
    if not os.path.exists(log_csv_upscale) or os.path.getsize(log_csv_upscale) == 0:
        logger.debug("missing upscale limit log file, create file and header")
        header = [
            'time_now', 'marker', 'data', 'level'
        ]
        __csv_writer(log_csv_upscale, header)
    __csv_writer(log_csv_upscale, w_data)


def cluster_credit_usage():
    """
    DEBUG
    Summarize the current credit values of the running workers on the cluster.
    Stand-alone function for regular csv log creation including API data query.
    Only for CSV log generation.
    :return: cluster_credit_usage
    """
    flavor_data = get_usable_flavors(True, False)
    cluster_data = get_cluster_data()
    credit_sum = 0
    if not (cluster_data is None):
        for cl in cluster_data:
            credit_sum += cluster_worker_to_credit(flavor_data, cl)
    logger.info("current worker credit %s", credit_sum)
    return credit_sum


def cluster_worker_to_credit(flavors_data, cluster_worker):
    fv = cluster_worker_to_flavor(flavors_data, cluster_worker)
    if fv:
        credit = float(fv['flavor']['credits_per_hour'])
        # logger.debug(credit)
        return credit
    return 0


def cluster_worker_to_flavor(flavors_data, cluster_worker):
    """
    Returns a corresponding flavor to a worker.
    :param flavors_data:
    :param cluster_worker:
    :return: flavor
    """
    # bad cluster data workaround
    tmp_disk_min = int(cluster_worker['ephemeral_disk'])
    tmp_disk_max = int(cluster_worker['ephemeral_disk']) * 1.03
    cores = int(cluster_worker['cores'])
    mem = int(cluster_worker['memory'])
    # bad flavor data workaround
    mem_min = int(cluster_worker['memory']) * 0.87
    for fd in flavors_data:
        flavor_mem = convert_gb_to_mib(fd['flavor']['ram'])
        flavor_disk = int(fd['flavor']['ephemeral_disk'])
        flavor_cpu = int(fd['flavor']['vcpus'])
        # logger.debug("flavor mem %s, disk %s , cpu %s", flavor_mem, flavor_disk, flavor_cpu)
        if mem >= flavor_mem >= mem_min and \
                tmp_disk_min <= flavor_disk <= tmp_disk_max and \
                flavor_cpu == cores:
            # logger.debug(pformat(cluster_worker))
            # logger.debug(pformat(fd))
            return fd
    logger.error("flavor not found for worker")
    return None


def __csv_log_with_refresh_data(scale_ud, worker_change, reason):
    """
    DEBUG
    Log current job and worker data after scale up and down
        - give scheduler some seconds to allocate workers
        - refresh node and worker data
        - write data to csv log
    Only for CSV log generation.
    :param scale_ud:
    :return:
    """
    if LOG_LEVEL != logging.DEBUG:
        return
    logger.debug("---- __csv_log_with_refresh_data %s ----", scale_ud)
    jobs_pending_dict, jobs_running_dict, jobs_pending, jobs_running = receive_job_data()
    worker_json, worker_count, worker_in_use, worker_drain_cnt, _ = receive_node_data_db(True)
    worker_free = (worker_count - worker_in_use)
    __csv_log(jobs_running, jobs_pending, worker_count, worker_in_use, worker_free, scale_ud,
              __count_current_cpu_mem_usage(worker_json, {**jobs_pending_dict, **jobs_running_dict}), worker_drain_cnt,
              worker_change, reason)


def __csv_log(jobs_cnt_running, job_cnt_pending, worker_cnt, worker_cnt_use, worker_cnt_free, scale_ud,
              cpu_mem_sum, worker_drain_cnt, worker_change, reason):
    """
    DEBUG
    Write values to CSV log, if required create csv file with header.
    Only for CSV log generation.
    :param jobs_cnt_running: number of running jobs
    :param job_cnt_pending: number of pending jobs
    :param worker_cnt: number of active workers
    :param worker_cnt_use: number of workers in use
    :param worker_cnt_free: number of idle workers
    :param scale_ud: scaling step marker
    :param cpu_mem_sum: total used memory
    :param worker_drain_cnt: number of drain workers
    :param worker_change: number of changed workers by scaling decision
    :param reason: secondary marker to identify request position
    :return:
    """
    cpu_alloc, mem_alloc, cpu_idle, mem_idle, jobs_cpu_alloc, jobs_mem_alloc, jobs_cpu_pending, jobs_mem_pending, \
    mem_alloc_drain, mem_alloc_no_drain, mem_idle_drain, mem_idle_no_drain, mem_mix_drain, mem_mix_no_drain \
        = cpu_mem_sum
    time_now = __get_time()
    if scale_ud != 'E' or (scale_ud == 'E' and int(reason) in [16, 15, 14, 13]):
        credit_ = cluster_credit_usage()
    else:
        credit_ = 0
    w_data = [
        time_now, str(scale_ud),
        str(worker_cnt), str(worker_cnt_use), str(worker_cnt_free),
        cpu_alloc, cpu_idle,
        mem_alloc, mem_idle,
        str(jobs_cnt_running), str(job_cnt_pending),
        jobs_cpu_alloc, jobs_cpu_pending,
        jobs_mem_alloc, jobs_mem_pending,
        mem_alloc_drain, mem_alloc_no_drain,
        mem_idle_drain, mem_idle_no_drain,
        mem_mix_drain, mem_mix_no_drain,
        worker_drain_cnt,
        worker_change,
        reason, credit_]

    if not os.path.exists(LOG_CSV) or os.path.getsize(LOG_CSV) == 0:
        logger.debug("missing log file, create file and header")
        header = [
            'time', 'scale', 'worker_cnt', 'worker_cnt_use', 'worker_cnt_free', 'cpu_alloc', 'cpu_idle',
            'mem_alloc', 'mem_idle', 'jobs_cnt_running', 'job_cnt_pending', 'jobs_cpu_alloc', 'jobs_cpu_pending',
            'jobs_mem_alloc', 'jobs_mem_pending', 'worker_mem_alloc_drain', 'worker_mem_alloc',
            'worker_mem_idle_drain', 'worker_mem_idle', 'worker_mem_mix_drain', 'worker_mem_mix', 'worker_drain_cnt',
            'w_change', 'reason', 'credit'
        ]
        __csv_writer(LOG_CSV, header)
    __csv_writer(LOG_CSV, w_data)


def __csv_writer(csv_file, log_data):
    """
    Update log file.
    :param csv_file: write to file
    :param log_data: write this new line
    :return:
    """
    with open(csv_file, 'a', newline='') as csvfile:
        fw = csv.writer(csvfile, delimiter=',')
        fw.writerow(log_data)


def __read_yaml_file(file_path):
    """
    Read yaml data from file.
    :param file_path:
    :return:
    """

    try:
        with open(file_path, "r") as stream:
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
    with open(yaml_file_target, 'w+') as target:
        try:
            yaml.dump(yaml_data, target)
        except yaml.YAMLError as exc:
            print(exc)
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
    # logger.info("\n %s\n", pformat(yaml_config))

    # logger.debug("mode adoptive %s", pformat(yaml_config['scaling']['mode']['adoptive']))
    return yaml_config['scaling']


def read_cluster_id():
    """
    Read cluster id from hostname
        - example hostname: bibigrid-master-clusterID
    :return: cluster id
    """

    with open('/etc/hostname', 'r') as file:
        try:
            cluster_id_ = file.read().rstrip().split('-')[2]
        except (ValueError, IndexError):
            logger.error("error: wrong cluster name \nexample: bibigrid-master-clusterID")
            sys.exit(1)
    return cluster_id_


def __get_portal_url_webapp():
    return config_data['portal_link'] + '/portal/webapp/#/virtualmachines/clusterOverview'


def get_wrong_password_msg():
    logger.error(f"The password seems to be wrong. Please verify it again, otherwise you can generate a new one "
                 f"for the cluster on the Cluster Overview ({__get_portal_url_webapp()})")


def __get_portal_url():
    return config_data['portal_link'] + '/portal/public'


def __get_portal_url_scaling():
    return __get_portal_url() + '/clusters_scaling/'


def get_url_scale_up():
    """
    :return: return portal api scale up url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-up/"


def get_url_scale_down():
    """
    :return: return portal api scale down url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-down/"


def get_url_scale_down_specific():
    """
    :return: return portal api scale down specific url
    """
    return __get_portal_url_scaling() + cluster_id + "/scale-down/specific/"


def get_url_info_cluster():
    """
    :return: return portal api info url
    """
    return __get_portal_url() + '/clusters/' + cluster_id + '/'


def get_url_info_flavors():
    """
    :return: return portal api flavor info url
    """
    return __get_portal_url() + '/clusters_scaling/' + cluster_id + '/usable_flavors/'


def __reduce_flavor_memory(mem_gb):
    """
    Receive raw memory in GB and reduce this to a usable value,
    memory reduces to os requirements and other circumstances. Calculation according to BiBiGrid setup.
    :param mem_gb: memory
    :return: memory reduced value (mb)
    """
    # mem_gb = mem/1024
    mem = convert_gb_to_mb(mem_gb)
    mem_min = 512
    mem_max = 4000
    mem_border = 16001
    # {% set mem = worker.memory // 1024 * 1000 %}
    # {% if mem < 16001 %}{{ mem - [ mem // 16, 512] | max }}
    # {% if mem > 16000 %}{{ mem - [mem // 16, 4000] | min }}
    sub = int(mem / 16)
    if mem <= mem_border:
        if sub < mem_min:
            sub = mem_min
    else:
        if sub > mem_max:
            sub = mem_max
    # logger.debug("reduce_flavor_memory: mem_gb: %s , substract: %s , real: %s", mem, sub, mem - sub)
    return int(mem - sub)


def __translate_cpu_mem_to_flavor(cpu, mem, tmp_disk, flavors_data, available_check, quiet):
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
    :param flavors_data: flavor data (json) from cluster api
    :return: matched and available flavor
    """
    cpu = int(cpu)
    mem = int(mem)
    # logger.debug(">>>>>>>>>>>>>> %s >>>>>>>>>>>>>>", inspect.stack()[0][3])
    # logger.debug("searching for cpu %d mem %d", cpu, mem)

    step_over_flavor = conf_mode['step_over_flavor']
    # logger.debug("-> searching for cpu %s mem %s tmp_disk %s", cpu, mem, tmp_disk)
    for fd in reversed(flavors_data):
        # TMP_DISK \\\\\
        if not conf_mode['tmp_disk_check']:
            if fd['flavor']['ephemeral_disk'] == 0:
                ephemeral_disk = False
            else:
                ephemeral_disk = True

            if not ephemeral_disk and conf_mode['flavor_ephemeral']:
                # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", conf_mode['flavor_ephemeral'])
                continue
            elif ephemeral_disk and not conf_mode['flavor_ephemeral']:
                # logger.debug("skip flavor - EPHEMERAL_REQUIRED %s", conf_mode['flavor_ephemeral'])
                continue
        # TMP_DISK  /////

        # logger.debug("flavor: \"%s\" -- ram: %s -- cpu: %s -- available: %sx ephemeral %s id: %s", fd['flavor']['name'],
        #              fd['available_memory'], fd['flavor']['vcpus'], fd['available_count'],
        #              fd['flavor']['ephemeral_disk'], fd['flavor']['id'])
        # TMP_DISK
        if cpu <= int(fd['flavor']['vcpus']) and mem <= int(fd['available_memory']) \
                and ((int(tmp_disk) < int(fd['flavor']['tmp_disk']) and conf_mode['tmp_disk_check'])
                     or not conf_mode['tmp_disk_check']):
            # job tmp disk must be lower than flavor tmp disk
            # ex. flavor with 1TB tmp disk, jobs with 1TB are not scheduled, only jobs with 999M tmp disk

            # available_memory = __reduce_flavor_memory(fd['flavor']['ram']
            # logger.debug("match flavor: \"%s\" -- ram: %s -- cpu: %s -- available: %sx ephemeral %s id: %s",
            #              fd['flavor']['name'],
            #              fd['available_memory'], fd['flavor']['vcpus'], fd['available_count'],
            #              fd['flavor']['ephemeral_disk'], fd['flavor']['id'])
            if not available_check:
                return fd
            if fd['available_count'] > 0:
                logger.debug("-> match found %s for cpu %d mem %d", fd['flavor']['name'], cpu, mem)
                return fd
            elif not step_over_flavor and not conf_mode['drain_high_nodes']:
                # if we have already passed a suitable flavor, try only the next higher flavor
                # incompatible with drain nodes
                step_over_flavor = True
                logger.debug("step_over_flavor %s", step_over_flavor)
            else:
                logger.debug("flavor %s found, but currently not availabe %s", fd['flavor']['name'],
                             fd['available_count'])
                break
    if not quiet:
        logger.warning("unable to find a suitable flavor! - searching for cpu %s mem %s tmp_disk %s", cpu, mem,
                       tmp_disk)

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
    Sort jobs by priority
        - priority should be calculated by scheduler
        - sort memory as secondary condition

    :param jobs_dict: jobs as json dictionary object
    :return: job list sorted by priority, jobs with high priority at the top
    """

    # sort jobs by memory as secondary condition, first priority (scheduler)
    priority_job_list = sorted(jobs_dict.items(), key=lambda k: (k[1]['priority'], k[1]['req_mem']), reverse=True)

    return priority_job_list


def receive_node_data_live():
    """
    Query the job data from scheduler (no database call) and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: number of workers in use
        - worker_drain: number of workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    worker_in_use = 0
    worker_count = 0
    worker_drain = 0
    worker_drain_idle = []
    # worker_short = {}

    node_dict = scheduler_interface.node_data_live()
    # logger.debug("node dict: %s ", pformat(node_dict))
    if node_dict:
        if LOG_LEVEL == logging.DEBUG:
            __save_file(FILE_BUP_NODE, node_dict)

        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            # logger.error("%s found, but dummy mode is not active", NODE_DUMMY)
            sys.exit(1)
        # else:
        #     logger.error("%s not found, but dummy mode is active", NODE_DUMMY)

        for key, value in list(node_dict.items()):
            # TMP_DISK
            if 'tmp_disk' in value:
                tmp_disk = value['tmp_disk']
            else:
                tmp_disk = 0
            logger.debug("key: %s - state: %s cpus %s real_memory %s tmp_disk %s",
                         json.dumps(key), json.dumps(value['state']),
                         json.dumps(value['cpus']), json.dumps(value['real_memory']), tmp_disk)
            # logger.debug(pformat(key))
            # logger.debug(pformat(value))
            if 'worker' in key:
                worker_count += 1
                # worker_short.update({key: {'state': value['state'], 'cpus': value['cpus'],
                #                            'memory': value['real_memory'], 'tmp_disk': value['tmp_disk']}})
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    worker_in_use += 1
                elif NODE_DRAIN in value['state'] and NODE_IDLE in value['state']:  # TODO only drain and idle
                    worker_drain += 1
                    worker_drain_idle.append(key)
                elif NODE_DRAIN in value['state']:
                    worker_drain += 1
                elif (NODE_DOWN in value['state']) and NODE_IDLE not in value['state']:
                    logger.error("workers are in DOWN state")
                else:
                    pass
            else:
                del node_dict[key]
        logger.info("nodes: I found %d worker - %d allocated, %d drain, drain+idle %s", worker_count, worker_in_use,
                    worker_drain, worker_drain_idle)
    # else:
    #     logger.info("No nodes found !")

    return node_dict, worker_count, worker_in_use, worker_drain, worker_drain_idle


def receive_node_data_db(quiet):
    """
    Query the job data from scheduler and return the json object,
    including the number of workers and how many are currently in use.
    :return:
        - worker_json: worker information as json dictionary object
        - worker_count: number of available workers
        - worker_in_use: number of workers in use
        - worker_drain: number of workers in drain status
        - worker_drain_idle: worker list with drain and idle status
    """
    worker_in_use = 0
    worker_count = 0
    worker_drain = 0
    worker_drain_idle = []

    if SIMULATION:
        node_dict = __get_file(FILE_BUP_NODE)
    else:
        node_dict = scheduler_interface.fetch_scheduler_node_data()

    # logger.debug("node dict: %s ", pformat(node_dict))
    if node_dict:
        if LOG_LEVEL == logging.DEBUG:
            __save_file(FILE_BUP_NODE, node_dict)

        if NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            del node_dict[NODE_DUMMY]
        elif not NODE_DUMMY_REQ and NODE_DUMMY in node_dict:
            logger.error("%s found, but dummy mode is not active", NODE_DUMMY)
            sys.exit(1)
        else:
            logger.error("%s not found, but dummy mode is active", NODE_DUMMY)

        for key, value in list(node_dict.items()):
            # TMP_DISK
            if 'tmp_disk' in value:
                tmp_disk = value['tmp_disk']
            else:
                tmp_disk = 0
            if not quiet:
                logger.info("key: %s - state: %s cpus %s real_memory %s tmp_disk %s",
                            json.dumps(key), json.dumps(value['state']),
                            json.dumps(value['cpus']), json.dumps(value['real_memory']), tmp_disk)
            # logger.debug(pformat(key))
            # logger.debug(pformat(value))
            if 'worker' in key:
                worker_count += 1
                if (NODE_ALLOCATED in value['state']) or (NODE_MIX in value['state']):
                    worker_in_use += 1
                elif NODE_DRAIN in value['state'] and NODE_IDLE in value['state']:  # TODO only drain and idle
                    worker_drain += 1
                    worker_drain_idle.append(key)
                elif NODE_DRAIN in value['state']:
                    worker_drain += 1
                elif (NODE_DOWN in value['state']) and NODE_IDLE not in value['state']:
                    logger.error("workers are in DOWN state")
                else:
                    pass
            else:
                del node_dict[key]
        if not quiet:
            logger.info("nodes: I found %d worker - %d allocated, %d drain, drain+idle %s", worker_count, worker_in_use,
                        worker_drain, worker_drain_idle)
    else:
        if not quiet:
            logger.info("No nodes found !")
    return node_dict, worker_count, worker_in_use, worker_drain, worker_drain_idle


def receive_job_data():
    """
    Receive current job data.
    :return:
        - dictionary with pending jobs
        - dictionary with running jobs
        - number of pending jobs
        - number of running jobs
    """
    jobs_pending_dict, jobs_running_dict = scheduler_interface.job_data_live()
    # logger.debug("jobs_pending_dict %s", jobs_pending_dict)
    # logger.debug("jobs_running_dict %s", jobs_running_dict)
    return jobs_pending_dict, jobs_running_dict, len(jobs_pending_dict), len(jobs_running_dict)


def receive_completed_job_data(days):
    """
    Return completed jobs from the last x days.
    :param days: number of days
    :return: jobs dictionary
    """
    jobs_dict = scheduler_interface.fetch_scheduler_job_data(days)
    if jobs_dict:
        for key, value in list(jobs_dict.items()):
            if value['state'] != JOB_FINISHED:
                del jobs_dict[key]
    return jobs_dict


def receive_job_data_db():
    """
    Capture all jobs from the last 24h and return the json object, including the number of pending jobs.
    :return:
        - jobs_dict: jobs as json dictionary object
        - jobs_pending: pending jobs as json dictionary object
        - jobs_running: running jobs as json dictionary object
    """
    jobs_pending = {}
    jobs_running = {}

    jobs_dict = scheduler_interface.fetch_scheduler_job_data(DATA_HISTORY)

    if jobs_dict:
        for key, value in list(jobs_dict.items()):
            if value['state'] == JOB_PENDING:
                logger.debug("JOB PENDING: id: %s - jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s",
                             key, value['jobname'], value['req_mem'],
                             value['req_cpus'], value['priority'], value['tmp_disk'])
                jobs_pending.update({key: value})
            elif value['state'] == JOB_RUNNING:
                logger.debug("JOB RUNNING: id: %s - jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s",
                             key, value['jobname'], value['req_mem'],
                             value['req_cpus'], value['priority'], value['tmp_disk'])
                jobs_running.update({key: value})
            else:
                del jobs_dict[key]
    else:
        logger.info("No job found")
    logger.debug("Found %d pending jobs.", len(jobs_pending))

    return jobs_dict, jobs_pending, jobs_running


def get_cluster_data():
    """
    Receive worker information from portal.
    request example:
    requests:  {'active_worker': [{'ip': '192.168.1.17', 'cores': 4, 'hostname': 'bibigrid-worker-1-1-3zytxfzrsrcl8ku',
     'memory': 4096, 'status': 'ACTIVE', 'ephemerals': []}, {'ip': ...}, 'VERSION': '0.2.0'}
    :return:
        cluster data dictionary
        if api error: None
    """
    try:
        json_data = {"scaling": "scaling_up", "password": cluster_pw, "version": VERSION}
        response = requests.post(url=get_url_info_cluster(),
                                 json=json_data)
        logger.debug("response code %s, send json_data %s", response.status_code, json_data)

        if response.status_code == 200:
            res = response.json()
            version_check(res["VERSION"])
            logger.debug(pformat(res))
            cluster_data = [data for data in res["active_worker"] if data is not None]

            return cluster_data
        elif response.status_code == 401:
            print(get_wrong_password_msg())
            logger.error(get_wrong_password_msg())
            sys.exit(1)
        elif response.status_code == 405:
            automatic_update()
        else:
            logger.error("server error - unable to receive cluster data")
            __csv_log_with_refresh_data('E', 0, '16')
            return None
    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        logger.error("unable to receive cluster data")
        return None
    except OSError as error:
        logger.error(error)
        return None
    except Exception as e:
        logger.error("error by accessing cluster data %s", e)
        return None


def __get_flavor_available_count(flavor_data, flavor_name):
    """
    Return the number of available workers with this flavor.

    :param flavor_data:
    :param flavor_name:
    :return: maximum of new possible workers with this flavor
    """
    flavor_tmp = __get_flavor_by_name(flavor_data, flavor_name)
    if flavor_tmp:
        return int(flavor_tmp['available_count'])
    return 0


def __get_flavor_by_name(flavor_data, flavor_name):
    """
    Return flavor object by flavor name.
    :param flavor_data:
    :param flavor_name:
    :return: flavor data
    """
    for flavor_tmp in flavor_data:
        if flavor_tmp['flavor']['name'] == flavor_name:
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


def convert_tb_to_mb(value):
    """
    Convert TB value to MB.
    :param value:
    :return:
    """
    return int(value) * 1000000


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
    fv_cut = conf_mode['flavor_cut']
    # modify flavor count by current version
    flavors_available = len(flavor_data)
    if 0 < fv_cut < 1:
        flavors_usable = __multiply(flavors_available, fv_cut)
    elif fv_cut >= 1:
        if flavors_available > fv_cut:
            flavors_usable = flavors_available - fv_cut
        else:
            flavors_usable = 1
            logger.error("wrong flavor cut value")
    else:
        flavors_usable = flavors_available
    logger.debug("found %s flavors %s usable ", flavors_available, flavors_usable)
    if flavors_usable == 0 and flavors_available > 0:
        flavors_usable = 1
    return flavors_usable


def reduce_flavor_data(flavor_data):
    reduce_by = len(flavor_data) - usable_flavor_data(flavor_data)
    for x in range(reduce_by):
        flavor_data.pop()
    logger.debug(pformat(flavor_data))


def flavor_mod_gpu(flavors_data):
    flavors_data_mod = []
    removed_flavors = []
    # modify flavors by gpu
    for fd in flavors_data:
        # use only gpu flavors
        if conf_mode['flavor_gpu_only']:
            if fd['flavor']['gpu'] == 0:
                removed_flavors.append(fd['flavor']['name'])
            else:
                flavors_data_mod.append(fd)
        # remove all gpu flavors
        elif fd['flavor']['gpu'] != 0 and conf_mode['flavor_gpu_remove']:
            removed_flavors.append(fd['flavor']['name'])
        else:
            flavors_data_mod.append(fd)

    # if len(removed_flavors) > 0:
    #     logger.debug("unlisted flavors by GPU config option: %s", removed_flavors)
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
        res = requests.post(url=get_url_info_flavors(),
                            json={"password": cluster_pw, "version": VERSION})

        # logger.debug("requests: %s", res)
        if res.status_code == 200:
            flavors_data = res.json()
            # logger.debug("flavorData %s", pformat(flavors_data))

            counter = 0
            flavors_data = flavor_mod_gpu(flavors_data)
            flavors_data_mod = []
            if cut:
                flavors_usable = usable_flavor_data(flavors_data)
            else:
                flavors_usable = len(flavors_data)

            for fd in flavors_data:
                available_memory = __reduce_flavor_memory(int(fd['flavor']['ram']))
                fd['flavor']['tmp_disk'] = convert_gb_to_mb(fd['flavor']['ephemeral_disk'])
                val_tmp = []
                for fd_item in fd.items():
                    val_tmp.append(fd_item)
                val_tmp.append(('cnt', counter))
                val_tmp.append(('available_memory', available_memory))
                if counter < flavors_usable:
                    if not quiet:
                        logger.info(
                            "flavor: %s - %s - ram: %s GB - cpu: %s - available: %sx - ephemeral_disk: %sGB tmpDisk %s - "
                            "real: %s - id %s",
                            counter,
                            fd['flavor']['name'],
                            fd['flavor']['ram'],
                            fd['flavor']['vcpus'],
                            fd['available_count'],
                            fd['flavor']['ephemeral_disk'],
                            fd['flavor']['tmp_disk'],
                            available_memory,
                            fd['flavor']['id']
                        )
                    flavors_data_mod.append(dict(val_tmp))
                else:
                    if not quiet:
                        logger.info(
                            "flavor: X - %s - ram: %s GB - cpu: %s - available: %sx - ephemeral_disk: %sGB tmpDisk %s - "
                            "real: %s - id %s",
                            # counter,
                            fd['flavor']['name'],
                            fd['flavor']['ram'],
                            fd['flavor']['vcpus'],
                            fd['available_count'],
                            fd['flavor']['ephemeral_disk'],
                            fd['flavor']['tmp_disk'],
                            available_memory,
                            fd['flavor']['id']
                        )
                counter += 1
            # print("------flavors_data_cnt----------")
            # print(flavors_data_cnt)
            # print (pformat(list(flavors_data_cnt)))
            # print("------flavors_data----------")
            # print(flavors_data)
            # print(pformat(flavors_data))
            # logger.debug("flavor usable %s",len(flavors_data_cnt))
            # logger.debug("flavor usable %s", pformat(flavors_data_cnt))

            return list(flavors_data_mod)
        elif res.status_code == 401:
            print(get_wrong_password_msg())
            logger.error(get_wrong_password_msg())
            sys.exit(1)
        elif res.status_code == 405:
            logger.error("version missmatch error")
            automatic_update()
        else:
            logger.error("server error - unable to receive flavor data")
            __csv_log_with_refresh_data('E', 0, '15')
            return None

    except requests.exceptions.HTTPError as e:
        logger.error(e.response.text)
        logger.error(e.response.status_code)
        __csv_log_with_refresh_data('E', 0, '14')
        # keep running on error - probably a temporary error, cloud services are currently under maintenance
    except OSError as error:
        logger.error(error)
    except Exception as e:
        logger.error("error by accessing flavor data %s", e)
    return None


def __get_file(read_file):
    """
    Read json content from file and return it.
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
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        logger.error('Decoding JSON data failed from file %s', read_file)
        return None


def __save_file(save_file, content):
    """
    Read json content from file and return it.
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
    Read cluster password from CLUSTER_PASSWORD_FILE, the file should contain: {"password":"CLUSTER_PASSWORD"}

    :return: "CLUSTER_PASSWORD"
    """
    global cluster_pw
    pw_json = __get_file(CLUSTER_PASSWORD_FILE)
    if not (pw_json is None):
        cluster_pw = pw_json['password']
        logger.debug("pw_json: %s", cluster_pw)
    return cluster_pw


def __set_cluster_password():
    """
    Save cluster password to CLUSTER_PASSWORD_FILE, the password can be entered when prompted.
    :return:
    """
    flavor_num = input("enter cluster password (copy&paste): ")

    tmp_pw = {'password': flavor_num}
    # backup cluster password
    __save_file(CLUSTER_PASSWORD_FILE, tmp_pw)


def __get_portal_auth():
    """
    Read the authentication file.
    :return: the authorization name and password
    """
    portal_auth = __get_file(FILE_PORTAL_AUTH)
    logger.debug(pformat(portal_auth))
    return portal_auth['server'], portal_auth['password']


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
        - cluster_data: cluster data from portal
    """
    url_cluster_info = get_url_info_cluster()

    logger.debug("checkWorkers: %s ", url_cluster_info)
    worker_active = []
    worker_down = []
    worker_unknown = []
    worker_error = []

    if not SIMULATION:
        cluster_data = get_cluster_data()
        logger.debug(pformat(cluster_data))
        if not cluster_data:
            return worker_active, worker_unknown, worker_error, worker_down, cluster_data
        if not (cluster_data is None):
            for cl in cluster_data:
                logger.debug("hostname: '%s' status: '%s'", cl['hostname'], cl['status'])
                if NODE_DOWN in cl['status']:
                    worker_down.append(cl['hostname'])
                elif WORKER_ERROR == cl['status']:
                    logger.error("ERROR %s", cl['hostname'])
                    worker_error.append(cl['hostname'])
                elif WORKER_FAILED in cl['status']:
                    logger.error("FAILED workers, not recoverable %s", cl['hostname'])
                    sys.exit(1)
                elif WORKER_ACTIVE == cl['status']:  # SCHEDULING -> BUILD -> (CHECKING_CONNECTION) -> ACTIVE
                    worker_active.append(cl['hostname'])
                else:
                    worker_unknown.append(cl['hostname'])
    return worker_active, worker_unknown, worker_error, worker_down, cluster_data


def check_workers(rescale):
    """
    Fetch cluster data from server and check
        - all workers are active
        - remove broken workers
    :return: cluster_pw
    """
    worker_ready = False
    start_time = time.time()
    max_time = 1200
    no_error = True
    while not worker_ready:
        worker_active, worker_unknown, worker_error, worker_down, cluster_data = __worker_states()

        if cluster_data is None:
            logger.error("unable to receive cluster data ... try again in %s seconds", conf_mode['service_frequency'])
            time.sleep(conf_mode['service_frequency'])
            continue

        logger.info("workers: active %s, error %s, down %s, not ready %s, error list: %s.", len(worker_active),
                    len(worker_error), len(worker_down),
                    len(worker_unknown), worker_error)
        elapsed_time = (time.time() - start_time)

        if elapsed_time > max_time and len(worker_unknown) > 0:
            logger.error("workers are stuck: %s", worker_unknown)
            cluster_scale_down_specific_selfcheck(worker_unknown + worker_error, rescale)
            no_error = False
            __csv_log_with_refresh_data('E', len(worker_unknown), '11')
        elif len(worker_error) > 0 and len(worker_unknown) == 0:
            logger.error("scale down error workers: %s", worker_error)
            cluster_scale_down_specific_selfcheck(worker_error, rescale)
            no_error = False
            __csv_log_with_refresh_data('E', len(worker_error), '12')
        elif elapsed_time > max_time and len(worker_down) > 0:
            logger.error("scale down workers are down: %s", worker_down)
            cluster_scale_down_specific_selfcheck(worker_down, rescale)
            no_error = False
            __csv_log_with_refresh_data('E', len(worker_error), '17')
        elif len(worker_unknown) == 0 and len(worker_error) == 0:
            logger.info("ALL WORKERS READY!!!")
            return worker_ready, no_error
        else:
            logger.info("at least one worker is not 'ACTIVE', wait ... %d seconds",
                        WAIT_CLUSTER_SCALING)
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
    # sort workers by resources, memory 'real_memory' False low mem first - True high first
    # workaround - required, if the most capable worker should remain active
    worker_mem_sort = sorted(worker_data.items(), key=lambda k: (k[1]['real_memory']), reverse=False)
    for key, value in worker_mem_sort:
        logger.debug("for : %d - key: %s - state: %s", count, key, value['state'])
        if 'worker' in key and count > 0:
            # check for broken workers first
            if (NODE_ALLOCATED not in value['state']) and (NODE_MIX not in value['state']) and (
                    NODE_IDLE not in value['state']):
                logger.error("worker is in unknown state: key %s  value %s", key, value['state'])
                worker_remove.append(key)
            elif (NODE_ALLOCATED not in value['state']) and (NODE_MIX not in value['state']) and count > 0:
                if NODE_DRAIN in value['state'] and NODE_IDLE in value['state']:
                    worker_remove.append(key)
                    count -= 1
                elif NODE_IDLE in value['state']:
                    worker_idle.append(key)
                    count -= 1
                else:
                    logger.debug("unable to include %s %s", key, value['state'])

    logger.debug("worker_idle %s", worker_idle)
    logger.debug("worker_remove %s", worker_remove)

    # remove recent used idle (not drain) workers from list
    if conf_mode['scale_frequency'] > 0 and len(worker_idle) > 0:
        worker_idle = __scale_frequency_worker_check(worker_idle, worker_data)
        logger.debug("frequency worker check %s", worker_idle)
    if len(worker_idle) > 0:
        # test if idle workers are capable for pending jobs
        # remove idle worker if still required but current idle without reason we know of
        _, _, _, worker_useless = __current_workers_capable(jobs_dict.items(), worker_data, worker_idle)
        worker_idle = [x for x in worker_idle if x in worker_useless]
        logger.debug("worker_idle not capable %s", worker_idle)
    worker_remove = worker_remove + worker_idle
    logger.debug("worker_remove merged: %s", worker_remove)

    if len(worker_remove) != 0:
        [scheduler_interface.set_node_to_drain(key) for key in worker_remove]
    else:
        logger.debug("unable to generate downscale list, worker may still be required")
    return worker_remove


def __scale_frequency_worker_check(worker_useless, worker_dict):
    """
    Check the elapsed time since the last worker usage according to job history.
    Remove workers from the list if the elapsed time since last usage is lower than the 'scale_frequency'.
    :param worker_useless: worker list
    :param worker_dict: worker dictionary
    :return: worker list
    """
    jobs_history_dict_rev = sorted(
        list(receive_completed_job_data(1).items()), key=lambda k: (k[1]['start'], k[1]['req_mem']), reverse=True)
    time_now = __get_time()
    wait_time = conf_mode['scale_frequency']
    worker_prevent_frequency = []
    for i in worker_useless:
        w_value = worker_dict[i]
        for _, value in jobs_history_dict_rev:
            elapsed_time = float(time_now) - float(value['end'])
            if 'nodes' in value:
                if elapsed_time < wait_time and i in value['nodes']:
                    # if __worker_match_to_job(value, w_value):
                    worker_prevent_frequency.append(i)
                    logger.debug("elapsed_time: %s wait_time %s time_now %s job_end %s fit for key %s",
                                 elapsed_time, wait_time,
                                 time_now, value['end'], i)
                else:  # too old
                    break
            else:
                logger.debug("missing node information, broken job data: %s", value)
            logger.debug("elapsed_time %s %s %s key %s", elapsed_time, time_now, value['end'], i)
        logger.debug("i %s w_value %s job len %s", i, w_value, len(jobs_history_dict_rev))
    logger.debug("worker_frequency_prevent %s", worker_prevent_frequency)
    result = [item for item in worker_useless if item not in worker_prevent_frequency]
    logger.debug("worker_frequency result %s", result)
    return result


def __calculate_scale_down_value(worker_count, worker_free, state):
    """
    Retain number of free workers to delete and respect boundaries like DOWNSCALE_LIMIT and SCALE_FORCE.

    :param worker_count: number of total workers
    :param worker_free: number of idle workers
    :return: number of workers to scale down
    """

    if worker_count > LIMIT_DOWNSCALE:
        if worker_free < worker_count and not state == ScaleState.DOWN_UP:
            return round(worker_free * conf_mode['scale_force'], 0)

        else:
            max_scale_down = worker_free - LIMIT_DOWNSCALE
            if max_scale_down > 0:
                return max_scale_down
            else:
                return 0
    else:
        return 0


def probably(chance):
    return random.random() < chance


def __similar_job_history(jobs_dict, job_pending_name, job_time_max, job_time_min, jobs_pending, job_time_sum,
                          job_count):
    """
    Search for jobs with a similar name in job history and calculate with normalized execution time for this job.

    :param jobs_dict: jobs as json dictionary object
    :param job_pending_name: pending job name as string
    :param job_time_max: longest job time
    :param job_time_min: shortest job time
    :param job_time_sum: start value
    :param job_count: start value
    :return: if similar jobs found, return average time value in seconds for these jobs
             if no similar job found, return -1
             norm, job_time_sum, job_count
    """
    logger.debug("----- __similar_job_history, job_pending_name : %s -----", job_pending_name)
    counter_tmp = 0
    if jobs_dict:
        for key, value in jobs_dict.items():
            # pprint(value)
            if value['state'] == JOB_FINISHED:
                job_name = clear_job_names(value['jobname'])
                diff_match = difflib.SequenceMatcher(None, job_name, job_pending_name).ratio()
                if diff_match >= conf_mode['job_match_value']:
                    # logger.debug("----- __similar_job_history, job %s match : %s -----", job_name, diff_match)
                    job_count += 1
                    counter_tmp += 1
                    job_time_sum += value['elapsed']
                    # logger.debug(
                    #     "id %s name %s - mem %s cpus %s - prio %s node %s - state %s - %s sec",
                    #     value['jobid'], value['jobname'], value['req_mem'], value['req_cpus'],
                    #     value['priority'], value['nodes'],
                    #     value['state_str'],
                    #     value['elapsed'])  # , value['start'], value['end'], value['end']-value['start'])

    norm = __calc_job_time_norm(job_time_sum, job_count, job_time_max, job_time_min)
    logger.debug("__calc_job_time_norm result %s jobs_pending %s counter_tmp %s", norm,
                 jobs_pending, counter_tmp)

    return norm, job_time_sum, job_count


def __current_worker_capable_(job_, worker_json):
    """
    Test if current workers are capable to process given job.
    :param job_:
    :param worker_json:
    :return: boolean, worker capable
    """

    found_match = False
    for w_key, w_value in worker_json.items():
        if 'worker' in w_key:
            if __worker_match_to_job(job_, w_value):
                found_match = True
                logger.debug("capable y - job mem %s worker mem %s, jcpu %s wcpu %s", job_['req_mem'],
                             w_value['real_memory'], job_['req_cpus'], w_value['cpus'])
                return found_match
            # else:
            # logger.debug("capable n - job mem %s worker mem %s, jcpu %s wcpu %s", job_['req_mem'],
            #              w_mem_tmp, job_['req_cpus'], w_value['cpus'])
    return found_match


def __worker_match_to_job(j_value, w_value):
    """
    Test if a worker matches to a job.
    :return: boolean, job data match to worker data
    """
    found_match = False
    # w_mem_tmp = int(w_value['real_memory'])
    # # TMP_DISK
    # if (j_value['req_mem'] <= w_mem_tmp) and (j_value['req_cpus'] <= w_value['cpus']) and \
    #         ((j_value['tmp_disk'] <= w_value['tmp_disk']) or not conf_mode['tmp_disk_check']):
    #     found_match = True

    # logger.debug("j_value['req_mem'] %s w_mem_tmp %s",type(j_value['req_mem']),type(w_mem_tmp))
    # logger.debug("j_value['req_cpus'] %s j_value['tmp_disk'] %s",type(j_value['req_cpus']),type(j_value['tmp_disk']))
    try:
        w_mem_tmp = int(w_value['real_memory'])
        w_cpu = int(w_value['cpus'])
        w_tmp_disk = int(w_value['tmp_disk'])
        # except ValueError:
        #     logger.error("worker value was not an integer, cpus: %s, tmp_disk %s",
        #                  w_value['cpus'], w_value['tmp_disk'])
        # try:
        j_cpu = int(j_value['req_cpus'])
        j_mem = int(j_value['req_mem'])
        j_tmp_disk = int(j_value['tmp_disk'])
        if j_mem <= w_mem_tmp and j_cpu <= w_cpu:
            if not conf_mode['tmp_disk_check'] or j_tmp_disk <= w_tmp_disk:
                found_match = True
    except ValueError:
        logger.error("value was not an integer %s w_value %s", j_value, w_value)
        # logger.error("job value was not an integer, cpu %s %s, mem %s %s, disk %s %s",
        #              j_value['req_cpus'], type(j_value['req_cpus']),
        #              j_value['req_mem'], type(j_value['req_mem']),
        #              j_value['tmp_disk'], type(j_value['tmp_disk']))
        # logger.debug("compare j_mem %s <=w_mem_tmp %s, j_cpu %s<=w_cpu %s, j_tmp_disk %s <= w_tmp_disk %s",
        #              j_mem, w_mem_tmp, j_cpu, w_cpu, j_tmp_disk, w_tmp_disk)

    return found_match


def __current_workers_capable(jobs_pending_dict, worker_json, worker_to_check):
    """
    Check if workers are capable for next job in queue.
    :param jobs_pending_dict: job dictionary with pending jobs
    :param worker_json: worker dictionary
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
    worker_useless = worker_all.copy()  # TODO
    for j_key, j_value in jobs_pending_dict:
        if j_value['state'] == JOB_PENDING:
            found_pending_jobs = True
            found_match = False
            # logger.debug("j_key %s j_value %s",j_key,j_value)
            for w_key, w_value in worker_json.items():
                # logger.debug("w_key %s w_value %s", w_key, w_value)
                if w_key not in worker_all:
                    logger.debug("skip %s", w_key)
                    continue
                if 'worker' in w_key:
                    if __worker_match_to_job(j_value, w_value):
                        if w_key in worker_useless:
                            worker_useless.remove(w_key)
                        found_match = True
                        logger.debug("%s capable y - job %s %s mem %s worker mem %s, jcpu %s wcpu %s", w_key, j_key,
                                     j_value['jobname'],
                                     j_value['req_mem'], w_value['real_memory'], j_value['req_cpus'], w_value['cpus'])
                    else:
                        logger.debug("%s capable n - job %s %s mem %s worker mem %s, jcpu %s wcpu %s", w_key, j_key,
                                     j_value['jobname'],
                                     j_value['req_mem'], w_value['real_memory'], j_value['req_cpus'], w_value['cpus'])
            if not found_match:
                worker_not_capable_counter += 1
                logger.debug("capable n - worker_not_capable_counter %s", worker_not_capable_counter)
        # break  # test only the next job (flavor) in queue
    # logger.debug("capable - found %s jobs without capable worker", worker_not_capable_counter)
    worker_useful = [x for x in worker_all if x not in worker_useless]
    logger.debug("worker_all %s", worker_all)
    logger.debug("worker_useful %s", worker_useful)
    logger.debug("worker_useless %s", worker_useless)

    if found_pending_jobs and worker_not_capable_counter > 0:
        return False, worker_all, worker_useful, worker_useless
    else:
        return True, worker_all, worker_useful, worker_useless


def __worker_flavor_pre_check(job_priority, flavor_data):
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
    flavor_max_depth = conf_mode['flavor_depth']
    depth_limit = len(flavor_data)
    if flavor_max_depth > 0 and flavor_max_depth > depth_limit:
        flavor_max_depth = depth_limit
    if flavor_max_depth == -1 or flavor_max_depth == -3:
        depth_limit = len(flavor_data)
        logger.debug("flavor_depth is set to all %s flavors", depth_limit)
    elif flavor_max_depth == -2 or conf_mode['flavor_default']:
        logger.debug("flavor_depth is set to single flavor")
        flavor_depth.append(__worker_no_flavor_separation(job_priority, flavor_data))
        return flavor_depth
    else:
        depth_limit = flavor_max_depth

    flavor_job_list = []
    flavor_next = None
    flavor_cnt = 0
    counter = 0
    for key, value in job_priority:
        if value['state'] == JOB_PENDING:
            # logger.debug("flavor_tmp_current cpu: %s mem: %s tmp %s c: %s", value['req_cpus'], value['req_mem'],
            #              value['tmp_disk'], counter)
            flavor_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                                       flavor_data, True, False)

            if flavor_tmp is None:
                logger.debug("unavailable flavor for job %s cpu %s mem %s disk %s",
                             value['jobname'], value['req_cpus'], value['req_mem'], value['tmp_disk'])
                # jump to the next job with available flavors
            elif flavor_next is None:
                flavor_next = flavor_tmp
                logger.debug("flavor_tmp_current is None: %s counter: %s", flavor_tmp, counter)
                counter += 1
                flavor_job_list.append((key, value))
            elif flavor_next['flavor']['name'] == flavor_tmp['flavor']['name']:
                counter += 1
                flavor_job_list.append((key, value))
                # logger.debug("flavor_tmp_current is same: %s counter: %s", flavor_tmp, counter)
            elif flavor_next['flavor']['name'] != flavor_tmp['flavor']['name'] and flavor_cnt < depth_limit:
                flavor_cnt += 1
                if counter > 0 and flavor_next:
                    flavor_depth.append((counter, flavor_next, flavor_job_list))
                logger.debug("switch to next flavor, previous flavor %s", flavor_next['flavor']['name'])
                counter = 1
                flavor_next = flavor_tmp
                flavor_job_list = [(key, value)]
            else:
                logger.debug("reached depth limit")
                break
    if len(flavor_job_list) > 0:
        flavor_depth.append((counter, flavor_next, flavor_job_list))

    # logger.debug("flavor_depth %s", pformat(flavor_depth))

    if LOG_LEVEL == logging.DEBUG:
        # print result snapshot for debug
        count_ = 0
        for co, fl, li in flavor_depth:
            logger.debug("\n---------------------------\n")
            if len(li) > 0:
                logger.debug("flavor_tmp_current %s counter: %s priority first: %s last: %s",
                             fl['flavor']['name'], co,
                             pformat(li[0][1]['priority']), pformat(li[-1][1]['priority']))
                for key, value in li:
                    logger.debug("JOB level %s: jobname %s - req_mem %s - req_cpus %s - priority %s - tmp_disk %s",
                                 count_, value['jobname'], value['req_mem'],
                                 value['req_cpus'], value['priority'], value['tmp_disk'])
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
        if value['state'] == JOB_PENDING:
            flavor_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                                       flavor_data, True, False)

            if flavor_tmp is None:
                logger.debug("unavailable flavor for job %s cpu %s mem %s disk %s",
                             value['jobname'], value['req_cpus'], value['req_mem'], value['tmp_disk'])
                continue
            elif flavor_next is None:
                flavor_next = flavor_tmp
            elif flavor_next['flavor']['name'] != flavor_tmp['flavor']['name']:
                if flavor_tmp['cnt'] > flavor_next['cnt']:
                    flavor_next = flavor_tmp
            job_list.append((key, value))
    return len(job_list), flavor_next, job_list


def __get_lower_flavor(flavor_data, flavor_tmp):
    """
    Return the next lower flavor to the provided one.
    :param flavor_data: available flavor data
    :param flavor_tmp: the flavor should be considered as a starting point
    :return: lower flavor
    """
    flavor_cnt = int(flavor_tmp['cnt']) + 1
    for fv_tmp in reversed(flavor_data):
        if flavor_cnt == int(fv_tmp['cnt']):
            return fv_tmp
    return None


def __get_higher_flavor(flavor_data, flavor_tmp):
    """
    Return the next higher flavor to the provided one.
    :param flavor_data: available flavor data
    :param flavor_tmp: the flavor should be considered as a starting point
    :return: higher flavor
    """
    flavor_cnt = int(flavor_tmp['cnt']) - 1
    for fv_tmp in reversed(flavor_data):
        if flavor_cnt == int(fv_tmp['cnt']):
            return fv_tmp
    return None


def __compare_flavor_max(fv_max, fv_min):
    """
    Compare flavor vs flavor values and, test if the given tmp flavor is higher or equal than the minimal flavor.
    :param fv_max: maximal flavor as json object
    :param fv_min: minimal flavor as json object
    :return: boolean
    """
    if fv_max['flavor']['ram'] >= fv_min['flavor']['ram'] and \
            fv_max['flavor']['vcpus'] >= fv_min['flavor']['vcpus'] and \
            (not conf_mode['tmp_disk_check'] or
             fv_max['flavor']['ephemeral_disk'] >= fv_min['flavor']['ephemeral_disk']):
        return True
    return False


def __compare_worker_high_vs_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, test if the worker is too large compared to given flavor.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp['available_memory'])
    w_mem_tmp = int(w_value['real_memory'])
    if fv_mem < w_mem_tmp and \
            fv_tmp['flavor']['vcpus'] < w_value['cpus'] and \
            (not conf_mode['tmp_disk_check'] or (int(fv_tmp['flavor']['tmp_disk']) <= int(w_value['tmp_disk']))):
        return True
    return False


def __compare_worker_meet_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, if the worker meets the minimum flavor data.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp['available_memory'])
    w_mem_tmp = int(w_value['real_memory'])
    if fv_mem <= w_mem_tmp and \
            fv_tmp['flavor']['vcpus'] <= w_value['cpus'] and \
            (not conf_mode['tmp_disk_check'] or (int(fv_tmp['flavor']['tmp_disk']) <= int(w_value['tmp_disk']))):
        return True
    return False


def __compare_worker_match_flavor(fv_tmp, w_value):
    """
    Compare flavor and worker values, if the worker match to flavor data with exact memory match.
    :param fv_tmp: single flavor as json object
    :param w_value: worker values as json object
    :return: boolean
    """
    fv_mem = int(fv_tmp['available_memory'])
    w_mem_tmp = int(w_value['real_memory'])
    if (fv_mem == w_mem_tmp) and (
            int(fv_tmp['flavor']['vcpus']) >= int(w_value['cpus'])) and (
            (not conf_mode['tmp_disk_check'] or (
                    int(fv_tmp['flavor']['tmp_disk']) >= int(w_value['tmp_disk'])))):
        return True
    return False


def set_nodes_to_drain(jobs_dict, worker_json, flavor_data):
    """
    Calculate the largest flavor/worker for pending jobs.
    Check the workers:
        - if they are over the required resources, set them to drain state
        - if they have required resources and are in drain state, remove drain state
    :param jobs_dict:
    :param worker_json:
    :param flavor_data:
    :return: changed worker data
    """
    worker_data_changed = False
    missing_flavors = False
    workers_drain = []
    fv_max = __translate_cpu_mem_to_flavor("1", "1", "0", flavor_data, False, True)  # lowest flavor
    if not fv_max:
        return worker_data_changed

    nodes_undrain = 0
    fv_min = None

    for key, value in jobs_dict.items():

        if value['state'] == JOB_PENDING:
            # logger.debug("set_nodes_to_drain: job %s", int(value['req_mem']))
            fv_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                                   flavor_data, False, False)
            if fv_tmp:
                if __compare_flavor_max(fv_tmp, fv_max):
                    fv_max = fv_tmp
                    # logger.debug(
                    #     "reset ram tmp %s >= max %s - tmp cpu %s >= max cpu %s - tmp disk %s - max disk %s",
                    #     fv_tmp['flavor']['ram'], fv_max['flavor']['ram'],
                    #     fv_tmp['flavor']['vcpus'], fv_max['flavor']['vcpus'],
                    #     fv_tmp['flavor']['tmp_disk'], fv_max['flavor']['tmp_disk'])
                if not fv_min:
                    fv_min = fv_tmp
                elif __compare_flavor_max(fv_min, fv_tmp):
                    fv_min = fv_tmp
            else:
                logger.debug("no suitable flavor available for job %s - maybe after downscale",
                             int(value['req_mem']))
                missing_flavors = True

    logger.debug("set_nodes_to_drain: %s", pformat(fv_max))

    for w_key, w_value in worker_json.items():
        # logger.debug("w_key %s w_value %s",w_key,w_value)
        if 'worker' in w_key:

            # w_mem_tmp = __reduce_flavor_memory(int(w_value['real_memory']) / 1000)
            # w_mem_tmp = int(w_value['real_memory'])
            logger.debug("---")
            # logger.debug("w_mem_tmp %s, flavor max %s", w_mem_tmp, fv_max_mem)
            logger.debug("worker disk %s, flavor disk %s", int(w_value['tmp_disk']),
                         int(fv_max['flavor']['tmp_disk']))
            logger.debug("w_cpus %s, cpus %s", int(w_value['cpus']), int(fv_max['flavor']['vcpus']))
            if missing_flavors and (NODE_DRAIN in w_value['state']):
                logger.debug("missing_flavors: undrain %s - keep worker active!", w_key)
                # scheduler_interface.set_node_to_undrain(w_key)
                # reactivate - if scale down and recheck resources ... ?
            # worker drain, if job do not need this flavor
            # worker resources are over required resources
            elif __compare_worker_high_vs_flavor(fv_max, w_value) and \
                    not missing_flavors and (NODE_DRAIN not in w_value['state']):
                logger.debug("high worker here: set to drain %s", w_key)
                scheduler_interface.set_node_to_drain(w_key)
                worker_data_changed = True
            # if worker in drain and jobs are in queue which require this worker flavor, undrain
            # first test if drained worker are capable
            elif (NODE_DRAIN in w_value['state']) and \
                    __compare_worker_meet_flavor(fv_max, w_value):
                logger.debug("high worker: %s in drain, fit for max required flavor", w_key)
                # undrain if worker with flavor still required for upcoming jobs in queue
                worker_data_changed = True
                if __compare_worker_match_flavor(fv_max, w_value):
                    logger.debug("high worker still required: undrain %s", w_key)
                    scheduler_interface.set_node_to_undrain(w_key)
                    nodes_undrain += 1
                else:
                    logger.debug("high worker: %s in drain, too large", w_key)
                    if NODE_IDLE in w_value['state']:  # if worker already idle, add for removal
                        workers_drain.append(w_value['node_hostname'])
            elif (NODE_DRAIN in w_value['state']) and (NODE_IDLE in w_value['state']):
                logger.debug("worker %s is drain and idle - scale down", w_key)
                workers_drain.append(w_value['node_hostname'])
                worker_data_changed = True

    if nodes_undrain > 0:
        __csv_log_with_refresh_data('X', nodes_undrain, '1')
    if len(workers_drain) > 0:
        cluster_scale_down_specific_selfcheck(workers_drain, Rescale.INIT)
        __csv_log_with_refresh_data('Y', len(workers_drain), '0')
    return worker_data_changed


def __convert_to_high_flavor(job_priority, flavor_data):
    """
    Calculate high flavor for next jobs in squeue.
        - jobs must be sorted by priority
        - only working a mode with high priority with high resources
    :param job_priority: jobs, sorted by resources, high resources first
    :param flavor_data: usable flavors
    :return: calculated flavor
    """
    tmp_cpu = 0
    tmp_mem = 0
    tmp_disk = 0
    flavor_max = None
    counter = 0
    for key, value in job_priority:
        if value['state'] == JOB_PENDING:
            tmp_cpu += int(value['req_cpus'])
            tmp_mem += int(value['req_mem'])
            tmp_disk += int(value['tmp_disk'])
            tmp_fv = __translate_cpu_mem_to_flavor(tmp_cpu, tmp_mem, tmp_disk, flavor_data, True, True)
            if tmp_fv is None:
                break
            counter += 1
            flavor_max = tmp_fv
    if flavor_max:
        logger.debug("convert_to_high_flavor: max %s jobs with flavor %s", counter, flavor_max['flavor']['name'])
    return flavor_max


def generate_hash(file_path):
    """
    Generate a sha256 hash from a file.
    :param file_path: file to process
    :return: hash value
    """
    h = hashlib.sha256()
    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    logger.debug("%s with hash %s", file_path, h.hexdigest())
    return h.hexdigest()


def check_config():
    """
    Print message to log if configuration file changed since program start.
    :return: sha256 hash from current configuration
    """
    new_hash = generate_hash(FILE_CONFIG_YAML)
    if new_hash != config_hash:
        logger.warning("configuration file changed, to apply the changes a manual restart is required \n"
                       "current hash: %s \n"
                       "started hash: %s ",
                       new_hash, config_hash)
    return new_hash


def convert_to_high_flavor(job_priority, flavors_data, pending_cnt, flavor_min):
    """
    Calculate high flavor for next jobs in squeue.
        - jobs must be sorted by priority
        - only working with scheduler mode high priority to high resources
        - consider how many jobs with the same minimal flavor can run on average on this worker
    :param job_priority: jobs, sorted by resources, high resources first
    :param flavors_data: usable flavors
    :param pending_cnt: number of jobs with flavor
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
    # max_cpu = 0
    # max_mem = 0
    # max_disk = 0
    flavor_max = None
    counter = 0
    counter_index = pending_cnt
    for key, value in job_priority:
        if value['state'] == JOB_PENDING and counter_index > 0:
            tmp_cpu = int(value['req_cpus'])
            tmp_mem = int(value['req_mem'])
            tmp_disk = int(value['tmp_disk'])
            counter += 1
            sum_cpu += tmp_cpu
            sum_mem += tmp_mem
            sum_disk += tmp_disk

            # if tmp_cpu > max_cpu:
            #     max_cpu = tmp_cpu
            # if tmp_mem > max_mem:
            #     max_mem = tmp_mem
            # if max_disk > tmp_disk:
            #     max_disk = tmp_disk

    sum_cpu = __division(sum_cpu, counter)
    sum_mem = __division(sum_mem, counter)
    sum_disk = __division(sum_disk, counter)
    counter_index = pending_cnt
    counter = 0

    tmp_cpu = 0
    tmp_mem = 0
    tmp_disk = 0
    logger.debug("convert_to_high_flavor: average cpu %s mem %s disk %s", sum_cpu, sum_mem, sum_disk)
    while counter_index > 0:
        tmp_cpu += sum_cpu
        tmp_mem += sum_mem
        tmp_disk += sum_disk
        # respect maximum resources, only by different minimum flavors
        # if tmp_cpu < max_cpu:
        #     search_cpu = max_cpu
        # else:
        #     search_cpu = tmp_cpu
        # if tmp_mem < max_mem:
        #     search_mem = max_mem
        # else:
        #     search_mem = tmp_mem
        # if tmp_disk < max_disk:
        #     search_disk = max_disk
        # else:
        #     search_disk = tmp_disk
        # tmp_fv = __translate_cpu_mem_to_flavor(search_cpu, search_mem, search_disk, flavors_data, True)
        tmp_fv = __translate_cpu_mem_to_flavor(tmp_cpu, tmp_mem, tmp_disk, flavors_data, True, True)
        if tmp_fv is None:
            break
        counter += 1
        flavor_max = tmp_fv
        counter_index -= 1
    if flavor_max:
        logger.debug("convert_to_high_flavor: max %s jobs with flavor %s", counter, flavor_max['flavor']['name'])

    logger.debug("high flavor found max %s - min %s", flavor_max['flavor']['name'], flavor_min['flavor']['name'])

    # test if flavor meet the specified minimal flavor
    if __compare_flavor_max(flavor_max, flavor_min):
        return flavor_max, counter, sum_cpu, sum_mem, sum_disk
    return None, None, None, None, None


def __get_worker_memory_usage(worker_json):
    """
    Calculate memory usage by all running workers.
    :param worker_json: current workers
    :return: memory usage
    """
    w_memory_sum = 0
    for _, w_value in worker_json.items():
        w_memory_sum += int(w_value['real_memory'])
    return w_memory_sum


def __generate_upscale_limit(current_force, jobs_pending_flavor, worker_count, job_time_norm):
    """
    Generate an up-scale limit from current force, based on current workers, job time and pending jobs on flavor.
    :param current_force:
    :param jobs_pending_flavor: number of pending jobs with current flavor
    :param worker_count: number of running workers
    :param job_time_norm: normalized job time from this flavor
    :return: up-scale limit
    """
    upscale_limit = 0
    if jobs_pending_flavor == 0:
        return upscale_limit

    # reduce starting new workers based on the number of currently existing workers
    if conf_mode['worker_weight'] != 0:
        force = current_force - (conf_mode['worker_weight'] * worker_count)
        logger.debug("WORKER_WEIGHT: %s - worker_count %s - force %s", conf_mode['worker_weight'],
                     worker_count, force)
        if force < FORCE_LOW:  # prevent too low scale force
            logger.debug("scale force too low: %s - reset", force)
            force = FORCE_LOW
    else:
        force = current_force

    # include job count and flavor data (if available)
    if not (job_time_norm is None):
        norm_tmp = job_time_norm
        # if norm_tmp > conf_mode['job_match_similar']:
        #     norm_tmp = conf_mode['job_match_similar']
        norm_cnt = __division(norm_tmp * jobs_pending_flavor, conf_mode['job_match_similar'])
        force_cnt = jobs_pending_flavor * force
        # merge force and norm value, with a double weighting of the flavor norm value
        upscale_limit = __division((force_cnt + norm_cnt * 2), 3)
        # upscale_limit = int(jobs_pending_flavor * (job_time_norm + force) / 2)
        logger.info("jobs_pending_flavor %s job_time_norm %s force %s norm_cnt %s force_cnt %s upscale_limit %s",
                    jobs_pending_flavor, job_time_norm, force,
                    norm_cnt, force_cnt, upscale_limit)
    else:
        upscale_limit = int(jobs_pending_flavor * force)

    logger.info("calculated  up-scale limit: %d", upscale_limit)
    return upscale_limit


def __division(x, y):
    """
    Calculate a division, if divided by zero, return zero.
    :param x: number
    :param y: divisor
    :return: result
    """
    try:
        return int(int(x) / int(y) + 0.5)
    except ZeroDivisionError:
        return 0


def __multiply(x, y):
    """
    Multiply and round the output value
    :param x: number
    :param y: divisor
    :return: result
    """

    return int((float(x) * float(y)) + 0.5)


def __calculate_scale_up_data(flavor_job_list, jobs_pending_flavor, worker_count, worker_json, state,
                              flavors_data, flavor_next, level, worker_memory_usage):
    """
    Create scale-up data for pending jobs with a specific flavor.
    Generates the scaling variable step by step according to user settings and available data from the job database/dictionary.
    A worker variable is generated in the process based on the job data, configuration and the scale force.
    Other user-based options can further reduce the value.
        - scale force  (0 to 1) - strength of scaling - user selection
        - optional:
            - worker weight
            - history data
                - from database or live generated
                - past jobs on the flavor
                - similar job data
                    job_time_norm - value (0 to 1) - identify if previous jobs were time-consuming
                    job times from previous jobs
            - high flavors
            - limits
                - memory
                - worker starts
                - active workers
        - worker capable check
        - limits
            - number of jobs on one flavor
            - available resources

      :param flavor_job_list: workers should be generated with this job list as json dictionary object
      :param jobs_pending_flavor: number of pending jobs
      :param worker_count: number of current workers
      :param worker_json: current workers as json dictionary object
      :param state: current multiscale state
      :param flavors_data: flavor data as json dictionary object
      :param flavor_next: next target flavor
      :param level: level depth
      :return: up_scale_data: scale up data for server
    """

    need_workers = False
    worker_capable, _, _, _ = __current_workers_capable(flavor_job_list, worker_json, None)
    fv_mem = 0

    current_force = conf_mode['scale_force']
    upscale_limit_log('f1', current_force, level)
    # pre-check - how many workers in line, need the same flavor
    upscale_limit_log('u0', int(current_force * jobs_pending_flavor), level)

    if flavor_next is None:
        return None, worker_memory_usage
    logger.info("calc worker for %s; pending %s jobs; worker active %s, capable %s, memory %s, need %s; state %s",
                flavor_next['flavor']['name'], jobs_pending_flavor, worker_count,
                worker_capable, worker_memory_usage, need_workers, state.name)
    # logger.debug("jobs_pending %s jobs_pending_flavor %s", jobs_pending, jobs_pending_flavor)
    if LONG_TIME_DATA and run_as_service:
        jobs_db = None
        dict_db = __get_file(DATABASE_FILE)
        if dict_db is None:
            logger.error("job dictionary is empty!")
            return None, worker_memory_usage
    else:
        jobs_db = receive_completed_job_data(DATA_HISTORY)
    # include previous job time history for flavor
    if (conf_mode['job_time_flavor'] or conf_mode['job_match_search']) and not conf_mode['prefer_high_flavors']:
        if LONG_TIME_DATA and run_as_service and flavor_next['flavor']['name'] in dict_db['flavor_name']:
            job_time_max = dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_max']
            job_time_min = dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_min']
            # logger.debug("flavor name from db: %s, next %s", dict_db['flavor_name'][flavor_next['flavor']['name']],flavor_next['flavor']['name'])
            # if not conf_mode['job_time_range_automatic']:
            #     job_time_norm_fv_tmp = __calc_job_time_norm(
            #         dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_sum'],
            #         dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_cnt'],
            #         conf_mode['job_time_long'], conf_mode['job_time_short'])
            #     logger.debug("flavor range dynamic: %s to static: %s", job_time_norm_fv, job_time_norm_fv_tmp)
            #     job_time_norm_fv = job_time_norm_fv_tmp
            if not conf_mode['job_time_range_automatic']:
                job_time_norm_fv = dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_norm_static']
            else:
                job_time_norm_fv = dict_db['flavor_name'][flavor_next['flavor']['name']]['fv_time_norm']
        else:
            job_time_norm_fv, job_time_max, job_time_min, _, _, _ = \
                __calc_previous_job_time(jobs_db, flavor_next, flavors_data)
    else:
        job_time_max, job_time_min = __calc_job_time_range(jobs_db, flavor_next, flavors_data)
        job_time_norm_fv = None
    upscale_limit_log('w', worker_count, level)
    upscale_limit_log('p', jobs_pending_flavor, level)

    # flavor based calculation, only if similar search is off
    if conf_mode['job_time_flavor'] and not conf_mode['job_match_search']:
        upscale_limit = __generate_upscale_limit(current_force, jobs_pending_flavor, worker_count, job_time_norm_fv)
    else:
        upscale_limit = __generate_upscale_limit(current_force, jobs_pending_flavor, worker_count, None)
    if not conf_mode['job_time_flavor'] or conf_mode['flavor_depth'] == -2:
        # otherwise, maybe required for jobs without history
        # normalized flavor value not available with a single high flavor (flavor_depth = -2)
        job_time_norm_fv = None

    logger.debug("track upscale limit lvl %s: %s norm %s flavor", level, upscale_limit, job_time_norm_fv)
    upscale_limit_log('u1', upscale_limit, level)
    # test at least one worker
    if upscale_limit == 0 and conf_mode['job_match_search']:
        logger.debug("upscale limit is zero, test at least one worker with similar search")
        upscale_limit = 1
        upscale_limit_log('u2', upscale_limit, level)
        logger.debug("track upscale limit lvl %s: %s - at least one", level, upscale_limit)
    # memory limit pre-check
    if conf_mode['limit_memory'] != 0:
        if worker_memory_usage > convert_tb_to_mb(conf_mode['limit_memory']):  # 1048576
            upscale_limit = 0
            logger.debug("memory limit %sTB memory_sum %s reduce upscale_limit %s on level %s",
                         conf_mode['limit_memory'], worker_memory_usage,
                         upscale_limit, level)
            upscale_limit_log('u11', upscale_limit, level)
        else:
            logger.debug("memory limit: %s < %s", worker_memory_usage, convert_tb_to_mb(conf_mode['limit_memory']))
    logger.debug("track upscale limit lvl %s: %s worker_memory_usage %s - memory limit check", level, upscale_limit,
                 worker_memory_usage)
    up_scale_data = {}
    flavor_tmp = flavor_next

    if (state == ScaleState.FORCE_UP and not worker_capable) or worker_count == 0:
        need_workers = True
    if need_workers and upscale_limit == 0:
        # if up scaling triggered and ...
        #   we need a scale up after a scale down with sufficient resources
        #   zero workers available
        #   workers are not capable for next jobs in queue
        logger.debug("scale_up need_workers %s upscale_limit %s", need_workers, upscale_limit)
        logger.debug("worker capable %s", worker_capable)
        upscale_limit = need_workers
        upscale_limit_log('u3', upscale_limit, level)
        logger.debug("track upscale limit lvl %s: %s capable %s - force up", level, upscale_limit, worker_capable)
    if upscale_limit > 0:
        job_time_norm_sum = 0  # use for JOB_TIME_SIMILAR_MERGE

        jobs_pending_flavor_counter = jobs_pending_flavor
        jobs_pending_similar_data_missing = 0  # use for JOB_TIME_SIMILAR_MERGE

        for key, value in flavor_job_list:
            if value['state'] == JOB_PENDING:
                job_name = clear_job_names(value['jobname'])
                logger.debug("+ cpu %s mem %s MB priority %s for jobid %s name %s", value['req_cpus'],
                             value['req_mem'],
                             value['priority'], value['jobid'], value['jobname'])

                # search for similar jobs
                if conf_mode['job_match_search']:
                    # use database with normalized data from similar job history
                    if LONG_TIME_DATA and run_as_service:
                        # the database only makes sense if it is updated regularly when started as a service
                        tmp_job_data = job_data_from_database(dict_db, flavor_next['flavor']['name'], job_name, True)
                        if tmp_job_data:
                            logger.debug("job data found in dictionary name: %s, %s", job_name, tmp_job_data)
                            # job_time_norm = tmp_job_data[0]
                            if not conf_mode['job_time_range_automatic']:
                                job_time_norm = tmp_job_data[3]
                            else:
                                job_time_norm = tmp_job_data[0]

                        else:
                            # new job
                            logger.debug("job data not in dictionary")
                            job_time_norm = None

                    else:
                        logger.debug("generate live + normalized data from job history")
                        # generate live normalized data from similar job history
                        job_time_norm, job_time_sum, job_count = __similar_job_history(jobs_db, job_name,
                                                                                       job_time_max,
                                                                                       job_time_min,
                                                                                       jobs_pending_flavor_counter,
                                                                                       0,
                                                                                       0)
                    # collect data for JOB_TIME_SIMILAR_MERGE
                    if job_time_norm:
                        # sum normalized values from pending jobs
                        # calculate new workers with jobs pending flavor and summarize normalized job time

                        # by a time based normalized job value over JOB_MATCH_SIMILAR, treat as one new worker
                        # JOB_MATCH_SIMILAR is the limit for a new worker from configuration
                        if job_time_norm > conf_mode['job_match_similar']:
                            logger.debug("job_time_norm: sum %s, %s > %s - over max",
                                         job_time_norm_sum, job_time_norm, conf_mode['job_match_similar'])
                            # job_time_norm_sum += conf_mode['job_match_similar']
                        else:
                            logger.debug("job_time_norm: sum %s, %s < %s - less max",
                                         job_time_norm_sum, job_time_norm, conf_mode['job_match_similar'])
                            # job_time_norm_sum += job_time_norm
                        # TODO TEST NO CUT VALUES
                        job_time_norm_sum += job_time_norm
                    else:
                        jobs_pending_similar_data_missing += 1

                jobs_pending_flavor_counter -= 1

        # calculate worker with summarized job time norm
        if conf_mode['job_match_search']:

            # generate new worker cnt jobs
            # summarize normalized worker values, use JOB_MATCH_SIMILAR to predict worker count
            upscale_limit_log('n1', job_time_norm_sum, level)
            similar_job_merge = conf_mode['job_match_similar']  # 1 - JOB_MATCH_SIMILAR TODO
            similar_job_worker_cnt = job_time_norm_sum / similar_job_merge
            upscale_limit_log('j1', similar_job_worker_cnt, level)
            logger.debug("similar_job_worker_cnt %s = %s / %s", similar_job_worker_cnt, job_time_norm_sum,
                         similar_job_merge)
            worker_active_weight = worker_count * conf_mode['worker_weight']

            # reduce worker count by active workers
            similar_job_worker_cnt = int(similar_job_worker_cnt - worker_active_weight)
            upscale_limit_log('j2', similar_job_worker_cnt, level)

            # generate upscale limit for jobs without history data
            # if active, include flavor based calculation
            similar_job_worker_cnt += __generate_upscale_limit(current_force,
                                                               jobs_pending_similar_data_missing,
                                                               worker_count, job_time_norm_fv)
            upscale_limit_log('j3', similar_job_worker_cnt, level)
            logger.debug("upscale_limit %s similar_job_worker_cnt %s similar_job_weight %s", upscale_limit,
                         similar_job_worker_cnt, int(similar_job_worker_cnt - worker_active_weight))

            logger.debug(
                "similar job merge: norm_sum %s , worker_cnt %s , worker_active %s upscale %s",
                job_time_norm_sum, similar_job_worker_cnt, worker_active_weight, upscale_limit)
            # limit to previous generated upscale limit
            if upscale_limit > similar_job_worker_cnt >= 0:
                upscale_limit = similar_job_worker_cnt
                upscale_limit_log('u4', upscale_limit, level)
            logger.debug("track upscale limit lvl %s: %s sum norm %s- job time similar search", level, upscale_limit,
                         job_time_norm_sum)

        if conf_mode['flavor_default']:
            fv_fix = __get_flavor_by_name(flavors_data, conf_mode['flavor_default'])
            if fv_fix:
                if fv_fix['flavor']['ram'] >= flavor_tmp['flavor']['ram'] and \
                        fv_fix['flavor']['tmp_disk'] >= flavor_tmp['flavor']['tmp_disk'] and \
                        fv_fix['flavor']['vcpus'] >= flavor_tmp['flavor']['vcpus']:
                    flavor_tmp = fv_fix
                else:
                    logger.error(
                        "flavor_default is active, but selected flavor not meet the requirements for minimal flavor")
                    return None, worker_memory_usage
            else:
                logger.error("flavor_default is active, selected flavor is not available")
                return None, worker_memory_usage

        # convert to a higher flavor when useful
        # resources for job resources vs flavor comparison
        fv_high, jobs_per_fv_high, average_job_resources_cpu, average_job_resources_memory, average_job_resources_tmp_disk = \
            convert_to_high_flavor(flavor_job_list, flavors_data, jobs_pending_flavor, flavor_next)
        if conf_mode['prefer_high_flavors'] and fv_high:
            logger.debug("high_flavor found %s with %s jobs per flavor, minimum flavor was %s",
                         fv_high['flavor']['name'], jobs_per_fv_high, flavor_tmp['flavor']['name'])
            if fv_high['flavor']['name'] != flavor_tmp['flavor']['name']:
                flavor_tmp = fv_high
                logger.debug("found high flavor for jobs in queue")

        # test if multiple jobs can run on this flavor
        # ---
        # check how many pending jobs can run on average on a flavor by cpu, memory and disk
        # whether the current flavor can process multiple jobs.
        # The lowest value over all types of resources is the maximum possible value.

        jobs_average_cpu = __division(flavor_tmp['flavor']['vcpus'], average_job_resources_cpu)
        jobs_average_memory = __division(__reduce_flavor_memory(flavor_tmp['flavor']['ram']),
                                         average_job_resources_memory)
        if int(flavor_tmp['flavor']['tmp_disk']) != 0 and average_job_resources_tmp_disk != 0:
            logger.debug("flavor_next['flavor']['tmp_disk'] %s", flavor_tmp['flavor']['tmp_disk'])
            jobs_average_tmp_disk = __division(flavor_tmp['flavor']['tmp_disk'], average_job_resources_tmp_disk)
            average_jobs_per_flavor = min(jobs_average_cpu, jobs_average_memory, jobs_average_tmp_disk)
        else:
            jobs_average_tmp_disk = 0
            logger.debug("check average job resources, without disk")
            average_jobs_per_flavor = min(jobs_average_cpu, jobs_average_memory)
        upscale_limit_log('a1', average_jobs_per_flavor, level)
        logger.debug("check average jobs per flavor by cpu %s, mem %s, disk %s, min %s",
                     jobs_average_cpu, jobs_average_memory, jobs_average_tmp_disk, average_jobs_per_flavor)
        # more than one job per flavor
        if average_jobs_per_flavor > 1:
            upscale_limit = __division(upscale_limit, average_jobs_per_flavor)
            upscale_limit_log('u6', upscale_limit, level)
            logger.debug("check average job resources against flavor resources, average max %s, limit %s",
                         average_jobs_per_flavor, upscale_limit)
        logger.debug("track upscale limit lvl %s: %s jobs per flavor %s - average_jobs_per_flavor", level,
                     upscale_limit,
                     average_jobs_per_flavor)
        # ---

        # memory limit check for scale up workers
        fv_mem = convert_gb_to_mb(flavor_tmp['flavor']['ram'])
        if conf_mode['limit_memory'] != 0:
            memory_buffer = convert_tb_to_mb(conf_mode['limit_memory']) - worker_memory_usage  # 1048576
            flavor_possible = int(memory_buffer / fv_mem)
            logger.debug("memory limit: flavor_possible %s, upscale_limit %s,"
                         " free %s, fv_mem %s, mem_sum %s, level %s",
                         flavor_possible, upscale_limit, memory_buffer, fv_mem, worker_memory_usage, level)
            if flavor_possible < upscale_limit:
                upscale_limit = flavor_possible
                upscale_limit_log('u10', upscale_limit, level)

        logger.debug("track upscale limit lvl %s: %s  - memory limit second check", level, upscale_limit)

        # worker limit check
        if conf_mode['limit_workers'] != 0:
            if worker_count >= conf_mode['limit_workers']:
                upscale_limit = 0
            elif (worker_count + upscale_limit) > conf_mode['limit_workers']:
                logger.debug("limit workers: up %s, count %s, limit %s, reduced %s",
                             upscale_limit, worker_count, conf_mode['limit_workers'],
                             conf_mode['limit_workers'] - worker_count)
                upscale_limit = conf_mode['limit_workers'] - worker_count
                upscale_limit_log('u7', upscale_limit, level)
        logger.debug("track upscale limit lvl %s: %s  - limit workers", level, upscale_limit)

        # force scale up, if required
        if need_workers and upscale_limit < 1:
            logger.info("scale force up %s, worker_count %s", state, worker_count)
            upscale_limit = need_workers
            upscale_limit_log('u12', upscale_limit, level)
        logger.debug("track upscale limit lvl %s: %s  - need workers", level, upscale_limit)

        # check with available flavors on cluster
        flavor_count_available = __get_flavor_available_count(flavors_data, flavor_tmp['flavor']['name'])
        if upscale_limit > flavor_count_available:
            logger.debug("track upscale limit lvl %s: %s  - reduce worker, available flavor %s ", level, upscale_limit,
                         flavor_count_available)
            upscale_limit = flavor_count_available
            upscale_limit_log('u8', upscale_limit, level)
        if conf_mode['limit_worker_starts'] != 0 and upscale_limit > conf_mode['limit_worker_starts']:
            logger.debug("limit worker starts - upscale_limit %s = %s LIMIT_STARTS", upscale_limit,
                         conf_mode['limit_worker_starts'])
            upscale_limit = conf_mode['limit_worker_starts']
            upscale_limit_log('u9', upscale_limit, level)
        up_scale_data = {"password": cluster_pw, "worker_flavor_name": flavor_tmp['flavor']['name'],
                         "upscale_count": upscale_limit, 'version': VERSION}
        logger.debug("track upscale limit lvl %s: %s  - start limit", level, upscale_limit)
        logger.debug("scale-up-data: \n%s", pformat(up_scale_data))
    else:
        upscale_limit_log('x', upscale_limit, level)
    if upscale_limit == 0 or not flavor_tmp:
        logger.info("calculated upscale_limit is %d or missing flavor '%s', skip", upscale_limit,
                    flavor_tmp['flavor']['name'])
        upscale_limit_log('z', upscale_limit, level)
        return None, worker_memory_usage
    upscale_limit_log('-', upscale_limit, level)
    logger.debug("track upscale limit lvl %s: %s  - final", level, upscale_limit)
    worker_memory_usage_tmp = worker_memory_usage + fv_mem * upscale_limit
    return up_scale_data, worker_memory_usage_tmp


def __calc_job_time_range(jobs_dict, flavor_next, flavor_data):
    """
    Search for maximal and minimal job-time from job history.
    Check the elapsed value on completed jobs with the same flavor for next jobs in queue.

    :param jobs_dict: jobs as json dictionary object
    :param flavor_next: jobs as json dictionary object
    :param flavor_data: jobs as json dictionary object
    :return:
        - job_time_max: longest job time
        - job_time_min: shortest job time
    """
    if conf_mode['job_time_range_automatic']:

        job_time_min = 1
        job_time_max = 1
        for key, value in jobs_dict.items():
            if value['state'] == JOB_FINISHED:
                fv_ = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                                    flavor_data, False, True)
                if not fv_:
                    logger.debug("skip unavailable flavor for job cpu %s mem %s disk %s", value['req_cpus'],
                                 value['req_mem'], value['tmp_disk'])
                    continue
                if flavor_next['cnt'] == fv_['cnt']:
                    # logger.debug("%s   %s", value['state'], value['elapsed'])
                    if job_time_min == 1 and value['elapsed'] > 0:
                        job_time_min = value['elapsed']
                    elif job_time_min > value['elapsed'] > 0:
                        job_time_min = value['elapsed']
                    #
                    if job_time_max == 1 and value['elapsed'] > 0:
                        job_time_max = value['elapsed']
                    elif job_time_max < value['elapsed']:
                        job_time_max = value['elapsed']

        logger.debug("job_time_max %s job_time_min %s", job_time_max, job_time_min)

        logger.debug("JOB_TIME_RANGE_AUTOMATIC %s", conf_mode['job_time_range_automatic'])

        return job_time_max, job_time_min
    return conf_mode['job_time_long'], conf_mode['job_time_short']


def __calc_job_time_norm(job_time_sum, job_num, job_time_max, job_time_min):
    """
    Generate normalized value between 0 and 1 in relation to maximum and minimum job time for scale up decision.
    Value range between  NORM_LOW <= value <= NORM_HIGH, where 0 < NORM_LOW and NORM_HIGH = 1.
    :param job_time_sum: sum of job times
    :param job_num: job count
    :param job_time_max: longest job time
    :param job_time_min: shortest job time
    :return: normalized value
    """

    # generate normalized value
    try:
        job_time_sum = int(job_time_sum)
        job_num = int(job_num)
        job_time_max = int(job_time_max)
        job_time_min = int(job_time_min)

        if job_num > 0 and job_time_sum > 1:
            job_time_average = __division(job_time_sum, job_num)
            norm = float(job_time_average - job_time_min) / float(job_time_max - job_time_min)
            # logger.debug("job_time_average: %d norm: %s tmin %s tmax %s job_time_sum: %d job_num: %d",
            #              job_time_average, norm, job_time_min, job_time_max, job_time_sum, job_num)

            if NORM_HIGH > norm > NORM_LOW:
                return norm
            # logger.debug("job_time_average: miscalculation")
            if norm <= NORM_LOW:
                return NORM_LOW
            return 1

    except ZeroDivisionError:
        logger.debug('cannot divide by zero, unusable data')
    logger.debug("job_time_average: skip calculation")

    return NORM_LOW


def __calc_previous_job_time(jobs_dict, flavor_next, flavor_data):
    """
    Check for finished jobs and calculate
        - the minimum and maximum job time for a flavor
        - whether average long or short jobs run on this flavor

    Prevent too many new workers when previous jobs are done in a few minutes.
    :param jobs_dict: jobs as json dictionary object
    :param flavor_next: jobs as json dictionary object
    :param flavor_data: jobs as json dictionary object
    :return: normalized value from 0 to 1
    """
    jobs_dict_flavor = []
    job_time_min = conf_mode['job_time_short']
    job_time_max = conf_mode['job_time_long']
    job_time_sum = 0
    counter = 0
    if jobs_dict:
        for key, value in jobs_dict.items():
            if value['state'] == JOB_FINISHED:
                fv_ = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                                    flavor_data, False, True)
                if not fv_:
                    logger.debug("skip unavailable flavor for job cpu %s mem %s disk %s", value['req_cpus'],
                                 value['req_mem'], value['tmp_disk'])
                    continue
                if flavor_next['cnt'] == fv_['cnt'] and value['elapsed'] >= 0:

                    jobs_dict_flavor.append((key, value))
                    counter += 1
                    job_time_sum += value['elapsed']
                    if job_time_min == 1:
                        job_time_min = value['elapsed']
                    elif job_time_min > value['elapsed']:
                        job_time_min = value['elapsed']
                    #
                    if job_time_max == 1:
                        job_time_max = value['elapsed']
                    elif job_time_max < value['elapsed']:
                        job_time_max = value['elapsed']

        logger.debug("job_time_max %s job_time_min %s", job_time_max, job_time_min)
        jobs_dict_flavor = dict(jobs_dict_flavor)
        return __calc_job_time_norm(job_time_sum, counter, job_time_max, job_time_min), \
               job_time_max, job_time_min, jobs_dict_flavor, counter, job_time_sum
    logger.info("No job found")
    return None, job_time_max, job_time_min, None, counter, job_time_sum


def __scale_down_job_frequency(jobs_pending):
    """
    Limit scale down frequency. Compare with job history since last started job time,
    while jobs are pending and being processed, new jobs may be started for the workers.

    If jobs start in a short interval, then it might not be worth deleting workers.
    Wait the span of the frequency variable in seconds before scale down.
    :param jobs_pending: number of pending jobs
    :return:
        True : if scale down allowed
        False: if scale down denied
    """
    wait_time = int(conf_mode['scale_frequency'] / 2)
    if jobs_pending == 0 and wait_time > 0 and not conf_mode['drain_high_nodes']:
        # if pending jobs + scale-down initiated -> worker not suitable
        time_now = __get_time()
        jobs_dict_rev = sorted(
            list(receive_completed_job_data(1).items()), key=lambda k: (k[1]['start'], k[1]['req_mem']), reverse=True)
        for key, value in jobs_dict_rev:
            # logger.debug(pformat(value))
            if value['state'] == JOB_FINISHED:  # if last job is completed
                elapsed_time = float(time_now) - float(value['start'])
                logger.debug("elapsed_time: %s wait_time %s time_now %s job_start %s", elapsed_time, wait_time,
                             time_now,
                             value['start'])
                if elapsed_time < wait_time:
                    logger.info("prevent scale down by frequency")
                    return False
                break
    return True


def __multiscale_scale_down(scale_state, worker_json, worker_down_cnt, jobs_pending_dict):
    """
    Scale down part from multiscale.
    :param scale_state: current scale state
    :param worker_json: worker information as json dictionary object
    :param worker_down_cnt: maximum worker count to scale down
    :param jobs_pending_dict: pending jobs as dictionary
    :return: new scale state
    """

    if __scale_down_job_frequency(len(jobs_pending_dict)):  # TODO
        if scale_state == ScaleState.DELAY:
            scale_state = ScaleState.DOWN
            time.sleep(conf_mode['scale_delay'])  # wait and recheck slurm values after DELAY
        elif scale_state == ScaleState.DOWN:
            cluster_scale_down_specific(worker_json, worker_down_cnt, Rescale.CHECK, jobs_pending_dict)
            scale_state = ScaleState.DONE
        else:
            scale_state = ScaleState.SKIP
            logger.info("---- SCALE DOWN - condition changed - skip ----")
    else:
        scale_state = ScaleState.SKIP
        logger.info("---- SCALE DOWN - job frequency ----")
    return scale_state


def __compare_custer_node_workers(cluster_data, worker_json):
    """
    Compare cluster nodes with scheduler.
    If a cluster worker is active, but not added to scheduler - possible broken worker.
    :param cluster_data: cluster data as json
    :param worker_json: worker data as json
    :return: worker error list
    """
    # verify cluster workers with node data
    worker_err_list = []
    cluster_workers = []
    worker_missing = []
    if not (cluster_data is None):
        for cl in cluster_data:
            cluster_workers.append(cl['hostname'])
            if not (cl['hostname'] in worker_json):  # TODO remove
                worker_err_list.append(cl['hostname'])
        worker_missing = list(set(cluster_workers).difference(list(worker_json.keys())))
        if len(worker_err_list) > 0:
            logger.debug("found missing workers in node data %s", worker_err_list)
        if len(worker_missing) > 0:
            logger.debug("found missing workers in node data %s", worker_missing)

    return worker_missing


def __scale_down_error_workers(worker_err_list):
    """
    Scale down workers by a given error worker list and print error message.
    Skip, if the error worker list is empty.
    :param worker_err_list: workers in state error as list
    :return:
    """
    if len(worker_err_list) > 0:
        logger.error("found %s broken workers on cluster, no state change %s in %s", len(worker_err_list),
                     worker_err_list, conf_mode['scale_delay'])
        time.sleep(WAIT_CLUSTER_SCALING)
        cluster_scale_down_specific_selfcheck(worker_err_list, Rescale.CHECK)
        __csv_log_with_refresh_data('E', '0', '0')


def __verify_cluster_workers(cluster_data_old, worker_json, manual_run):
    """
    Check via cluster api if workers are broken and compare workers from first and second call.
        - delete workers if states on cluster are stuck at error, down, planned or drain
        - server side (check worker)
    :param cluster_data_old: cluster data from first call
    :param worker_json: worker information as json dictionary object
    :param manual_run: skip compare cluster data
    :return: cluster_data
    """
    cluster_data_new = get_cluster_data()
    # logger.debug(pformat(cluster_data_new))
    if cluster_data_new is None:
        return None
    if not cluster_data_new:
        logger.debug("cluster data is empty")
        worker_err_list = __compare_custer_node_workers(cluster_data_new, worker_json)
        __scale_down_error_workers(worker_err_list)
        return False
    if manual_run:
        cluster_data_old = cluster_data_new
    if cluster_data_old is None:
        worker_err_list = __compare_custer_node_workers(cluster_data_new, worker_json)
        __scale_down_error_workers(worker_err_list)
        return cluster_data_new
    worker_err_list = []
    cluster_data_copy_tmp = []
    error_states = [WORKER_ERROR, WORKER_PLANNED, WORKER_PORT_CLOSED, WORKER_PLANNED, WORKER_SCHEDULING]
    for cl in cluster_data_old:
        logger.debug("hostname: '%s' status: '%s'", cl['hostname'], cl['status'])
        for clt in cluster_data_new:
            if cl['hostname'] == clt['hostname'] and cl['status'] == clt['status']:
                # no change
                cluster_data_copy_tmp.append(cl)
                if any(ele in cl['status'] for ele in error_states):
                    worker_err_list.append(cl['hostname'])
    __scale_down_error_workers(worker_err_list)
    return cluster_data_copy_tmp


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
            if 'worker' in key:
                worker_copy[key] = value
        return worker_copy

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
            elif NODE_IDLE in value['state'] and NODE_DRAIN in value['state']:
                worker_copy_tmp[key] = value
                logger.debug("%s state: %s vs %s - state recently changed, exception worker idle & drain!",
                             key, value['state'], worker_copy[key]['state'])
            else:
                logger.debug("%s state: %s vs %s - state recently changed or keep alive", key, value['state'],
                             worker_copy[key]['state'])
    return worker_copy_tmp


def multiscale(flavor_data):
    """
    Make a scaling decision in advance by scaling state classification.
    """
    logger.debug("================= %s =================", inspect.stack()[0][3])
    __csv_log_with_refresh_data('M', '0', '0')
    state = ScaleState.DELAY

    # create worker copy, only use delete workers after a delay, give scheduler and network time to activate
    worker_copy = None
    cluster_worker = None
    if not flavor_data:
        return

    while state != ScaleState.SKIP and state != ScaleState.DONE:
        jobs_pending_dict, _, jobs_pending, jobs_running = receive_job_data()
        worker_json, worker_count, worker_in_use, _, _ = receive_node_data_db(True)
        worker_free = (worker_count - worker_in_use)

        if conf_mode['drain_high_nodes'] and state == ScaleState.DELAY and jobs_pending > 0 and flavor_data:
            if set_nodes_to_drain(jobs_pending_dict, worker_json, flavor_data):
                # update data after drain + scale down
                jobs_pending_dict, _, jobs_pending, jobs_running = receive_job_data()
                worker_json, worker_count, worker_in_use, _, _ = receive_node_data_db(True)

        # check for broken workers at cluster first
        cluster_worker = __verify_cluster_workers(cluster_worker, worker_json, False)
        if cluster_worker is None:
            state = ScaleState.SKIP
            logger.error("unable to receive cluster workers")
            continue

        # create worker copy, only delete workers after a delay, give scheduler and network time to activate
        worker_copy = __worker_same_states(worker_json, worker_copy)

        # check workers'
        # - server side (check worker) - __verify_cluster_workers
        # - scheduler side (node information) - at scale down
        # save workers in list during delay , recheck if same states - __worker_same_states
        # - use only workers with no state change at scale down

        logger.info("worker_count: %s worker_in_use: %s worker_free: %s jobs_pending: %s", worker_count,
                    worker_in_use, worker_free, jobs_pending)
        if worker_count == 0 and jobs_pending > 0 and not state == ScaleState.FORCE_UP:
            state = ScaleState.FORCE_UP
            logger.debug("zero worker and jobs pending, force up")
        # SCALE DOWN  # TODO merge
        elif worker_count > LIMIT_DOWNSCALE and worker_in_use == 0 and jobs_pending == 0:
            # generateDownScaleList(worker_json, worker_free)
            # workers are not in use and not reached DOWNSCALE_LIMIT
            # nothing to do here, just scale down any worker

            logger.info("---- SCALE DOWN - DELETE: workers are not in use")
            state = __multiscale_scale_down(state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state),
                                            jobs_pending_dict)
        elif worker_count > LIMIT_DOWNSCALE and worker_free >= 1 and worker_in_use > 0 and jobs_pending == 0:
            logger.info("---- SCALE DOWN - DELETE: workers are free and no jobs pending")
            state = __multiscale_scale_down(state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state),
                                            jobs_pending_dict)
        elif worker_count > LIMIT_DOWNSCALE and worker_free > round(conf_mode['scale_force'] * worker_count,
                                                                    0) and jobs_pending == 0:
            # if medium or high CPU usage, but multiple workers are idle
            # more workers are idle than the force allows
            # __generate_downscale_list(worker_json, round(SCALE_FORCE * worker_count, 0))

            logger.info("---- SCALE DOWN - DELETE: more workers %s are idle than the force %s allows", worker_free,
                        round(conf_mode['scale_force'] * worker_count, 0))
            state = __multiscale_scale_down(state, worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state),
                                            jobs_pending_dict)
        # SCALE DOWN and UP
        elif worker_free > 0 and jobs_pending >= 1 and not state == ScaleState.FORCE_UP:
            # required CPU/MEM usage from next pending job may over current worker resources ?
            # jobs pending, but workers are idle, possible low resources ?
            # -> if workers are free, but jobs are pending and CPU/MEM usage may not sufficient
            # -> SCALE DOWN - delete free worker with too low resources
            # start at least one worker, plus additional ones if a lot of jobs pending
            # - force upscale after downscale
            logger.info("---- SCALE DOWN_UP - worker_free and jobs_pending ----")
            if state == ScaleState.DOWN_UP:
                logger.info("---- SCALE DOWN_UP - cluster_scale_down_specific ----")
                cluster_scale_down_specific(worker_copy,
                                            __calculate_scale_down_value(worker_count, worker_free, state),
                                            Rescale.NONE, jobs_pending_dict)
                # skip one reconfigure at FORCE_UP
                state = ScaleState.FORCE_UP
            elif state == ScaleState.DELAY:
                logger.info("---- SCALE DOWN_UP - SCALE_DELAY ----")
                state = ScaleState.DOWN_UP
                time.sleep(conf_mode['scale_delay'])
            else:
                state = ScaleState.SKIP
                logger.info("---- SCALE DOWN_UP - condition changed - skip ----")
        # SCALE UP
        elif (worker_count == worker_in_use and jobs_pending >= 1) or state == ScaleState.FORCE_UP:
            # -> if all workers are in use and jobs pending and jobs require more time
            # calculate
            #    -> SCALE_FORCE*JOBS_WAITING - check resources for every job + worker
            #        -> initiate scale up
            logger.info("---- SCALE UP - all workers are in use with pending jobs or force up ----")
            if not flavor_data:
                logger.error("missing flavor_data")
                return
            logger.debug("flavor data count %s", len(flavor_data))
            if state == ScaleState.DELAY:
                state = ScaleState.UP
                time.sleep(conf_mode['scale_delay'])
            elif state == ScaleState.UP and len(flavor_data) > 0:
                cluster_scale_up(jobs_pending_dict, worker_count, worker_json, state, flavor_data)
                state = ScaleState.DONE
            elif state == ScaleState.FORCE_UP and len(flavor_data) > 0:
                logger.debug("---- SCALE UP - force scale up ----")
                cluster_scale_up(jobs_pending_dict, worker_count, worker_json, state, flavor_data)
                state = ScaleState.DONE
            else:
                logger.info("---- SCALE UP - condition changed - skip ---- %s", state.name)
                state = ScaleState.SKIP
        else:
            if state == ScaleState.UP or state == ScaleState.DOWN:
                logger.info("---- SCALE - condition changed - skip ----")
            else:
                logger.debug("<<<< skip scaling >>>> ")
            state = ScaleState.SKIP

        logger.info("-----------#### scaling loop state: %s ####-----------", state.name)


def __cloud_api_(portal_url_scale, worker_data):
    global cluster_pw

    if PORTAL_AUTH:
        auth_name, auth_pw = __get_portal_auth()
        response = requests.post(url=portal_url_scale, json=worker_data,
                                 auth=(auth_name, auth_pw))
    else:
        response = requests.post(url=portal_url_scale, json=worker_data)
    response.raise_for_status()
    logger.info("response code: %s, message: %s", response.status_code, response.text)

    cluster_pw = json.loads(response.text)['password']  # response.text.split(':')[1]
    # logger.debug("new cluster password is: %s", cluster_pw)

    # backup cluster password
    with open(CLUSTER_PASSWORD_FILE, 'w') as f:
        f.write(response.text)
    # extract response text
    # logger.debug(f"portal response is:{response.text}")
    return cluster_pw


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
    if not SIMULATION:
        try:
            with TerminateProtected():
                return __cloud_api_(portal_url_scale, worker_data)
        except requests.exceptions.HTTPError as e:
            logger.error(e.response.text)
            logger.error(e.response.status_code)
            if e.response.status_code == 405:
                logger.error("version missmatch error")
                automatic_update()
            # keep running on error - probably a temporary error, cloud services are currently under maintenance
            __csv_log_with_refresh_data('E', 0, '13')
            return None
        except OSError as error:
            logger.error(error)
            return None
        except Exception as e:
            logger.error("error by accessing cloud api %s", e)
            return None
    else:
        logger.debug("%s: JUST A DRY RUN!!", inspect.stack()[0][3])
        return ""


def cluster_scale_up(jobs_dict, worker_count, worker_json, state, flavor_data):
    """
    scale up and rescale cluster with data generation
    skip if scale up data return None

    :param flavor_data:
    :param worker_json: worker node information as json dictionary object
    :param state: current scaling state
    :param worker_count: number of total workers
    :param jobs_dict: jobs as json dictionary object
    :return:
    """
    job_priority = __sort_job_priority(jobs_dict)
    flavor_depth = __worker_flavor_pre_check(job_priority, flavor_data)
    if not flavor_depth:
        return
    flavor_col_len = len(flavor_depth)
    if flavor_col_len == 0:
        logger.debug("pre-check failed, skip")
        return
    data_list = []
    upscale_cnt = 0
    flavor_index_cnt = 0
    started_ = False
    flavors_started_cnt = 0
    multi_start = FLAVOR_MULTI_STARTS
    if conf_mode['flavor_depth'] == -1:
        multi_start = True
    # if no worker generated for next pending jobs with next flavor
    # perhaps it is desirable to look further ahead to calculate the flavor after next ... for the pending jobs
    # may break job order! but speed up the process
    # logger.debug("flavor_depth len %s - %s", len(flavor_depth), pformat(flavor_depth))
    worker_memory_usage = __get_worker_memory_usage(worker_json)

    while flavor_col_len > flavor_index_cnt:  # and not data
        jobs_pending_flavor, flavor_next, flavor_job_list = flavor_depth[flavor_index_cnt]

        data_tmp, worker_memory_usage_tmp = __calculate_scale_up_data(flavor_job_list, jobs_pending_flavor,
                                                                      worker_count, worker_json,
                                                                      state, flavor_data, flavor_next, flavor_index_cnt,
                                                                      worker_memory_usage)
        # multi-start
        if data_tmp:
            data_list.append(data_tmp)
            if not started_ or multi_start:
                if cloud_api(get_url_scale_up(), data_tmp):
                    worker_memory_usage = worker_memory_usage_tmp
                    upscale_cnt += data_tmp['upscale_count']
                    flavors_started_cnt += 1
                    started_ = True
                    time.sleep(WAIT_CLUSTER_SCALING)
        logger.debug("flavor data generation for future jobs level: %s data: %s", flavor_index_cnt, data_tmp)
        flavor_index_cnt += 1
    time_now = __get_time()

    if upscale_cnt > 0:
        rescale_cluster()
        if state == ScaleState.FORCE_UP:
            __csv_log_with_refresh_data('U', upscale_cnt, '8')
        else:
            __csv_log_with_refresh_data('U', upscale_cnt, '5')
        rescale_time = __get_time() - time_now
        logger.debug("scale up started %s worker with %s different flavor in %s seconds",
                     upscale_cnt, flavors_started_cnt, rescale_time)

        __configuration_watch(upscale_cnt, rescale_time)
    elif state == ScaleState.FORCE_UP:
        rescale_cluster()  # rescale once from scale-down (combined rescale DOWN_UP)
        __csv_log_with_refresh_data('U', 0, '7')


def cluster_scale_down_specific_hostnames(hostnames, rescale):
    """
    scale down with specific hostnames
    :param hostnames: hostname list as string "worker1,worker2,worker..."
    :param rescale: if rescale (with worker check) is desired
    :return: new cluster password
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])

    worker_hostnames = '[' + hostnames + ']'
    logger.debug(hostnames)
    cluster_scale_down_specific_hostnames_list(worker_hostnames, rescale)


def cluster_scale_down_specific_selfcheck(worker_hostnames, rescale):
    """
    scale down with specific hostnames
    include a final worker check with most up-to-date worker data before scale down
        exclude allocated and mix worker
    :param worker_hostnames: hostname list
    :param rescale: if rescale (with worker check) is desired
    :return: success
    """
    # wait for state update
    time.sleep(WAIT_CLUSTER_SCALING)
    worker_json, _, _, _, worker_drain_idle = receive_node_data_db(False)
    scale_down_list = []

    for wb in worker_hostnames:
        if wb in worker_json:
            if wb not in worker_drain_idle:
                if NODE_ALLOCATED in worker_json[wb]['state'] or NODE_MIX in worker_json[wb]['state']:
                    logger.debug("safety check: worker allocated, rescue worker %s, state %s ! ", wb,
                                 worker_json[wb]['state'])
                else:
                    logger.debug("worker %s added with state  %s", wb, worker_json[wb]['state'])
                    scale_down_list.append(wb)
            else:
                logger.debug("worker marked as idle and drain %s", wb)
                scale_down_list.append(wb)
        else:
            logger.debug("not in scheduler worker list %s", wb)
            scale_down_list.append(wb)

    logger.debug("last scale down check: available:\n%s\nidle %s:\n%s\nreceived %s:\n%s\nresult %s:\n%s, rescale %s",
                 worker_json.keys(), len(worker_drain_idle), worker_drain_idle, len(worker_hostnames), worker_hostnames,
                 len(scale_down_list), scale_down_list, rescale)
    if len(scale_down_list) > 0:
        return cluster_scale_down_specific_hostnames_list(scale_down_list, rescale)
    return False


def cluster_scale_down_specific_hostnames_list(worker_hostnames, rescale):
    """
    scale down with specific hostnames
    :param worker_hostnames: hostname list
    :param rescale: if rescale (with worker check) is desired
    :return: success
    """
    # logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    logger.debug("scale down %s worker, list: %s", len(worker_hostnames), worker_hostnames)
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostnames, "version": VERSION}
    logger.debug("portal_url_scaledown_specific: %s", get_url_scale_down_specific())
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    result_ = True
    if not (cloud_api(get_url_scale_down_specific(), data) is None):
        if rescale == Rescale.CHECK:
            result_ = rescale_cluster()
        elif rescale == Rescale.INIT:
            result_ = rescale_init()
        __csv_log_with_refresh_data('D', len(worker_hostnames), '3')
    else:
        __csv_log_with_refresh_data('D', len(worker_hostnames), '4')
        result_ = False
    return result_


def cluster_scale_down_specific(worker_json, worker_num, rescale, jobs_dict):
    """
    scale down a specific number of workers, downscale list is self generated
    :param jobs_dict: jobs dictionary with pending jobs
    :param worker_json: worker information as json dictionary object
    :param worker_num: number of worker to delete
    :param rescale: rescale cluster with worker check
    :return: boolean, success
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    if worker_num > 0:
        worker_list = __generate_downscale_list(worker_json, worker_num, jobs_dict)
        result_ = True

        # skip when failed
        if len(worker_list) > 0:
            result_ = cluster_scale_down_specific_selfcheck(worker_list, rescale)
        return result_


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
    cluster_data = get_cluster_data()
    # logger.debug(pformat(cluster_data))
    worker_list = []
    if not (cluster_data is None):
        for cl in cluster_data:
            if 'worker' in cl['hostname']:
                logger.debug("delete worker %s", cl['hostname'])
                worker_list.append(cl['hostname'])
        if len(worker_list) > 0:
            logger.debug("scale down %s", worker_list)
            time.sleep(WAIT_CLUSTER_SCALING)
            cluster_scale_down_specific_hostnames_list(worker_list, Rescale.CHECK)


def rescale_cluster():
    """
    apply the new worker configuration
    execute scaling, modify and run ansible playbook
    wait for workers, all should be ACTIVE (check_workers)
    :return: boolean, success
    """
    logger.debug("--------------- %s -----------------", inspect.stack()[0][3])
    logger.debug("initiate Scaling wait %s seconds", WAIT_CLUSTER_SCALING)
    time.sleep(WAIT_CLUSTER_SCALING)
    logger.debug("...")

    worker_ready, no_error_scale = check_workers(Rescale.NONE)
    rescale_success = rescale_init()
    if no_error_scale and rescale_success:
        return True
    return False


def __cluster_scale_up_test(upscale_limit):
    """
    scale up cluster by number
    :param upscale_limit: scale up number of workers
    :return:
    """
    logger.info("----- test upscaling, start %d new worker nodes with %s -----", upscale_limit,
                conf_mode['flavor_default'])
    up_scale_data = {"password": cluster_pw, "worker_flavor_name": conf_mode['flavor_default'],
                     "upscale_count": upscale_limit, "version": VERSION}
    cloud_api(get_url_scale_up(), up_scale_data)
    rescale_cluster()


def __cluster_scale_up_specific(flavor, upscale_limit):
    """
    scale up cluster by flavor
    :param flavor: flavor name as string
    :param upscale_limit: scale up number of workers
    :return: boolean, success
    """
    logger.info("----- upscaling, start %d new worker nodes with %s -----", upscale_limit, flavor)
    up_scale_data = {"password": cluster_pw, "worker_flavor_name": flavor,
                     "upscale_count": upscale_limit, "version": VERSION}
    if not cloud_api(get_url_scale_up(), up_scale_data):
        return False
    if not rescale_cluster():
        return False
    return True


def __cluster_scale_down_idle():
    """
    scale down all idle worker
    :return:
    """
    worker_json, worker_count, worker_in_use, _, _ = receive_node_data_db(False)
    worker_cnt = worker_count - worker_in_use
    cluster_scale_down_specific(worker_json, worker_cnt, Rescale.CHECK, None)


def __cluster_scale_down_workers_test(worker_hostname):
    """
    scale down cluster by hostnames
    :param worker_hostname: "[host_1, host_2, ...]"
    :return:
    """
    data = {"password": cluster_pw,
            "worker_hostnames": worker_hostname, "version": VERSION}
    logger.debug("cluster_scaledown_workers_test: %s", get_url_scale_down_specific())
    logger.debug("\nparams send: \n%s", json.dumps(data, indent=4, sort_keys=True) + "\n")
    cloud_api(get_url_scale_down_specific(), data)
    rescale_cluster()


def __print_help():
    print(textwrap.dedent("""\
        Option      Long option         Argument    Meaning
        -v          -version                        print the current version number
        -h          -help                           print this help information
        -fv         -flavors                        print available flavors
        -m          -mode               mode        scaling with a high adaptation
        -m          -mode               ...         select mode name from yaml file
        -p          -password                       set cluster password
        -rsc        -rescale                        run scaling with ansible playbook 
        -s          -service                        run as service (mode: default)
        -s          -service             mode       run as service
        -sdc        -scaledownchoice                scale down (worker id) - interactive mode
        -sdb        -scaledownbatch                 scale down (batch id)  - interactive mode
        -suc        -scaleupchoice                  scale up - interactive mode
        -sdi        -scaledownidle                  scale down all workers (idle + worker check)
        -csd        -clustershutdown                delete all workers from cluster (api)
                    -reset                          re-download autoscaling and reset configuration
        _                                           no argument - mode: default (yaml config)
    """))


def __print_help_debug():
    if LOG_LEVEL == logging.DEBUG:
        print(textwrap.dedent("""\
        ---------------------------- < debug options > ----------------------------
        Option      Long option         Argument    Meaning
        -cw         -checkworker                    check for broken worker
        -nd         -node                           receive node data
        -l          -log                            create csv log entry
        -c          -clusterdata                    print cluster data
        -su         -scaleup                        scale up cluster by one worker (default flavor)
        -su         -scaleup            2           scale up cluster by a given number of workers (default flavor)
        -sus        -scaleupspecific    "flavor"    scale up one worker by given flavorname (check with -fv)
        -sd         -scaledown                      scale down cluster, all idle workers
        -sds        -scaledownspecific  wk1,wk2,... scale down cluster by given workers
        -cdb        -clusterbroken                  delete all broken workers from cluster (api vs scheduler data)
        -t          -test                           test run
        """))


def setup_logger(log_file):
    """
    setup logger
    :param log_file: log file location
    :return: logger
    """
    logger_ = logging.getLogger('')
    logger_.setLevel(LOG_LEVEL)
    file_handler = logging.FileHandler(log_file)
    stream_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
                                  datefmt='%a, %d %b %Y %H:%M:%S')
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
        logger.critical("pid file already exists %s, exit\n", pidfile_)
        sys.exit(1)
    open(pidfile_, 'w').write(pid)
    return pidfile_


def __settings_check(settings_list, settings_received):
    values_available = True
    dict_keys = settings_received.keys()
    for i in settings_list:
        if i not in dict_keys:
            logger.error("setting %s is missing", i)
            values_available = False
    return values_available


def __setting_overwrite(mode_setting):
    """
    load config from yaml file
    test if all values are available
    """
    scaling_settings = ['portal_link', 'scheduler', 'default_mode', 'automatic_update', 'mode', 'database_reset']
    mode_settings = ['service_frequency', 'limit_memory', 'limit_worker_starts', 'limit_workers', 'scale_force',
                     'scale_delay', 'scale_frequency', 'worker_weight',
                     'job_time_long', 'job_time_short', 'job_time_range_automatic', 'job_time_flavor',
                     'job_match_value', 'job_match_similar',
                     'job_match_remove_numbers', 'job_match_remove_num_brackets', 'job_match_remove_pattern',
                     'job_match_remove_text_within_parentheses',
                     'job_match_search',
                     'flavor_cut', 'flavor_default', 'flavor_ephemeral', 'tmp_disk_check',
                     'flavor_gpu_remove', 'flavor_gpu_only', 'flavor_depth',
                     'prefer_high_flavors', 'step_over_flavor', 'drain_high_nodes',
                     'scheduler_settings']

    yaml_config = read_config_yaml()
    if read_config_yaml is None:
        logger.error("missing configuration data")
        sys.exit(1)
    if not __settings_check(scaling_settings, yaml_config):
        logger.error("missing scaling settings")
        sys.exit(1)
    if mode_setting is None:
        mode_setting = yaml_config['default_mode']
    logger.debug("loading mode settings ... %s", mode_setting)
    if mode_setting in yaml_config['mode']:
        sconf = yaml_config['mode'][mode_setting]
        if not __settings_check(mode_settings, sconf):
            logger.error("missing settings in mode: %s", mode_setting)
            sys.exit(1)
        __update_playbook_scheduler_config(sconf['scheduler_settings'])
    else:
        logger.error("unknown mode: %s", mode_setting)
        sys.exit(1)
    return yaml_config['mode'][mode_setting], yaml_config


def __cluster_scale_up_choice():
    """
    scale up cluster
    * flavor number and scale up value can be specified after the call
    * interactive flavor selection
    :return:
    """
    flavors_data = get_usable_flavors(False, False)
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
                    time.sleep(WAIT_CLUSTER_SCALING)
                    __cluster_scale_up_specific(flavor_name, upscale_num)
                    logger.debug("scale up by choice success: %s, %s x", flavor_name, upscale_num)
                else:
                    logger.error("wrong scale up number")
            else:
                logger.error("wrong flavor number")
        except (ValueError, IndexError):
            logger.error("wrong values")
            sys.exit(1)


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
            if 'worker' in key:
                batch_id = key.split('-')[2]
                logger.info("batch id: %s - key: %s - state: %s cpus %s real_memory %s",
                            batch_id, json.dumps(key), json.dumps(value['state']),
                            json.dumps(value['cpus']), json.dumps(value['real_memory']))
        worker_num = input("worker batch: ")
        if worker_num.isnumeric():
            search_batch = "worker-" + worker_num + "-"
    if search_batch:
        for index, (key, value) in enumerate(worker_dict.items()):
            if search_batch in key:
                logger.info("index %s key: %s - state: %s cpus %s real_memory %s",
                            index, json.dumps(key), json.dumps(value['state']),
                            json.dumps(value['cpus']), json.dumps(value['real_memory']))
                scale_down_list.append(key)

        logger.info("generated scale_down_list: %s", scale_down_list)
        # scale_down_string = ",".join([str(x) for x in scale_down_list])
        # logger.info("generated list: %s", scale_down_string)
        time.sleep(WAIT_CLUSTER_SCALING)

        if len(scale_down_list) > 0:
            cluster_scale_down_specific_hostnames_list(scale_down_list, Rescale.CHECK)


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
            if 'worker' in key:
                logger.info("index %s key: %s - state: %s cpus %s real_memory %s",
                            index, json.dumps(key), json.dumps(value['state']),
                            json.dumps(value['cpus']), json.dumps(value['real_memory']))
        while True:
            worker_num = input("worker index: ")
            if not worker_num.isnumeric():
                break
            ids.append(int(worker_num))

        # logger.debug("generated ids: %s", ids)
        for index, (key, value) in enumerate(worker_dict.items()):
            # logger.debug("key: %s", key)
            if 'worker' in key:
                if index in ids:
                    scale_down_list.append(key)
        logger.info("generated scale_down_list: %s", scale_down_list)
        # scale_down_string = ",".join([str(x) for x in scale_down_list])
        # logger.info("generated list: %s", scale_down_string)
        time.sleep(WAIT_CLUSTER_SCALING)

        if len(scale_down_list) > 0:
            cluster_scale_down_specific_hostnames_list(scale_down_list, Rescale.CHECK)


def __run_as_service():
    """
    Start autoscaling as a service with
        - cluster rescaling
        - worker resource watcher process
        - logging process for graphic analysis
        - start service
    :return:
    """
    rescale_cluster()

    if LOG_LEVEL == logging.DEBUG:
        watcher = Process(target=__worker_watch, args=())
        watcher.start()
    logger.debug("start service!!!")
    __service()


def __service():
    """
    Start autoscaling as a service.
        - rescale cluster
        - run multiscale in a loop
        - sleep for a time defined in service_frequency
    :return:
    """
    while True:
        logger.debug("=== INIT ===")
        check_config()
        flavor_data = get_usable_flavors(True, True)
        if flavor_data:
            if LONG_TIME_DATA:
                update_database(flavor_data)
            multiscale(flavor_data)
        logger.debug("=== SLEEP ===")
        time.sleep(conf_mode['service_frequency'])


def __get_time():
    """
    :return: current time in seconds
    """
    return int(str((datetime.datetime.now().replace(microsecond=0).timestamp())).split('.')[0])


def job_data_from_database(dict_db, flavor_name, job_name, multi_flavor):
    """
    return data from given job name
    - flavor based version
    :param multi_flavor: search on multiple flavors
    :param dict_db: job database
    :param flavor_name: current flavor
    :param job_name: search for this job
    :return: dict_db
    """
    if flavor_name in dict_db['flavor_name']:
        for current_job in dict_db['flavor_name'][flavor_name]['similar_data']:
            diff_match = difflib.SequenceMatcher(None, job_name, current_job).ratio()
            if diff_match > conf_mode['job_match_value']:
                # found
                return dict_db['flavor_name'][flavor_name]['similar_data'][current_job]

    if multi_flavor:
        # logger.debug("dict_db['flavor_name'] %s", dict_db['flavor_name'])
        for flavor_tmp in dict_db['flavor_name']:
            if flavor_tmp == flavor_name:
                continue
            for current_job in dict_db['flavor_name'][flavor_tmp]['similar_data']:
                diff_match = difflib.SequenceMatcher(None, job_name, current_job).ratio()
                if diff_match > conf_mode['job_match_value']:
                    return dict_db['flavor_name'][flavor_tmp]['similar_data'][current_job]
    return None


def create_database_():
    """
    :return: dict_db
    """
    flavors_data = get_usable_flavors(True, True)
    if flavors_data is None:
        logger.error("unable to receive flavor data")
        return None
    time_start = __get_time() - DATA_LONG_TIME * 86400

    dict_db = {'VERSION': VERSION, 'config_hash': config_hash, 'update_time': time_start, 'flavor_name': {}}
    __save_file(DATABASE_FILE, dict_db)
    return dict_db


def update_database(flavors_data):
    """
    update database file
    integrate job data since "update_time" from database file
    - flavor based version
    :return: dict_db
    """
    dict_db = None
    # database file exist
    if os.path.exists(DATABASE_FILE):
        dict_db = __get_file(DATABASE_FILE)
        # create new database if config file changed to avoid incompatible settings
        if dict_db and (dict_db['config_hash'] != config_hash or dict_db['VERSION'] != VERSION):
            logger.info("config file changed, recreate database")
            dict_db = None
    # if nothing there
    if dict_db is None:
        dict_db = create_database_()

    if flavors_data is None:
        logger.error("unable to receive flavor data")
        return dict_db
    current_time = __get_time()
    last_update_time = int(dict_db['update_time'])
    logger.debug("get jobs from %s to %s", int(dict_db['update_time']), current_time)
    jobs_dict_new = receive_completed_job_data(1 + __division(current_time - last_update_time, 86400))

    if not jobs_dict_new:
        return dict_db
    dict_db['update_time'] = current_time

    # add new sum and cnt values from jobs to dict
    for key, value in jobs_dict_new.items():
        if value['state'] != JOB_FINISHED or int(value['end']) < last_update_time or value['elapsed'] < 0:
            continue
        job_name = clear_job_names(value['jobname'])
        # logger.debug("update database job ... %s, cpu %s, mem %s, disk %s elapsed %s s %s e %s", job_name,
        #              value['req_cpus'],
        #              value['req_mem'], value['tmp_disk'], value['elapsed'], value['start'], value['end'])
        v_tmp = __translate_cpu_mem_to_flavor(value['req_cpus'], value['req_mem'], value['tmp_disk'],
                                              flavors_data, False, True)
        if not (v_tmp is None):
            found = False
            # logger.debug("add job %s, flavor %s", job_name, v_tmp['flavor']['name'])
            # elapsed = value['end'] - value['start']

            # logger.debug("new job value['elapsed'] %s elapsed %s", value['elapsed'], elapsed)
            # flavor related - global normalized value
            if not v_tmp['flavor']['name'] in dict_db['flavor_name']:
                # if not exist init flavor
                logger.debug("init flavor in database %s", v_tmp['flavor']['name'])
                dict_db['flavor_name'].update(
                    {v_tmp['flavor']['name']: {'fv_time_min': conf_mode['job_time_short'],
                                               'fv_time_max': conf_mode['job_time_long'],
                                               'fv_time_cnt': 0,
                                               'fv_time_sum': 0,
                                               'fv_time_norm': NORM_LOW,
                                               'fv_time_norm_static': NORM_LOW,
                                               'similar_data': {}}})

            if value['elapsed'] < conf_mode['job_time_short'] and \
                    dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_min'] > value['elapsed']:
                dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_min'] = value['elapsed']
            if value['elapsed'] > conf_mode['job_time_long'] and \
                    dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_max'] < value['elapsed']:
                dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_max'] = value['elapsed']
            # update flavor related data
            dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_cnt'] += 1
            dict_db['flavor_name'][v_tmp['flavor']['name']]['fv_time_sum'] += value['elapsed']

            # update job related similar_data
            for current_job in dict_db['flavor_name'][v_tmp['flavor']['name']]['similar_data']:
                # logger.debug("update database job %s for %s flavor ...", current_job, v_tmp['flavor']['name'])

                diff_match = difflib.SequenceMatcher(None, job_name, current_job).ratio()
                if diff_match > conf_mode['job_match_value']:
                    found = True
                    # job related time sum + job related count
                    dict_db['flavor_name'][v_tmp['flavor']['name']]['similar_data'][current_job] = (
                        NORM_LOW,
                        dict_db['flavor_name'][v_tmp['flavor']['name']]['similar_data'][current_job][1] + \
                        value['elapsed'],
                        dict_db['flavor_name'][v_tmp['flavor']['name']]['similar_data'][current_job][2] + 1,
                        NORM_LOW)

            if not found:
                logger.debug("init job %s in database, elapsed %s", job_name, value['elapsed'])
                dict_db['flavor_name'][v_tmp['flavor']['name']]['similar_data'].update(
                    {job_name: (NORM_LOW, value['elapsed'], 1, NORM_LOW)})
        else:
            logger.error("missing flavor %s for job classification!", v_tmp['flavor']['name'])

    # update all dict norm values at once
    for current_flavor in dict_db['flavor_name']:
        logger.debug("current_flavor %s, %s", current_flavor, dict_db['flavor_name'][current_flavor])

        # update normalized flavor data
        dict_db['flavor_name'][current_flavor]['fv_time_norm'] = \
            __calc_job_time_norm(
                dict_db['flavor_name'][current_flavor]['fv_time_sum'],
                dict_db['flavor_name'][current_flavor]['fv_time_cnt'],
                dict_db['flavor_name'][current_flavor]['fv_time_max'],
                dict_db['flavor_name'][current_flavor]['fv_time_min'])
        dict_db['flavor_name'][current_flavor]['fv_time_norm_static'] = \
            __calc_job_time_norm(
                dict_db['flavor_name'][current_flavor]['fv_time_sum'],
                dict_db['flavor_name'][current_flavor]['fv_time_cnt'],
                conf_mode['job_time_long'],
                conf_mode['job_time_short'])
        dict_db['flavor_name'][current_flavor]['fv_time_avg'] = \
            __division(dict_db['flavor_name'][current_flavor]['fv_time_sum'],
                       dict_db['flavor_name'][current_flavor]['fv_time_cnt'])
        # dict_db['flavor_name'][current_flavor]['fv_time_avg_norm'] = \
        #     __calc_job_time_norm(
        #         dict_db['flavor_name'][current_flavor]['fv_time_sum'],
        #         dict_db['flavor_name'][current_flavor]['fv_time_cnt'],
        #         __multiply(__division(dict_db['flavor_name'][current_flavor]['fv_time_sum'],
        #                               dict_db['flavor_name'][current_flavor]['fv_time_cnt']), 0.5),
        #         __multiply(__division(dict_db['flavor_name'][current_flavor]['fv_time_sum'],
        #                               dict_db['flavor_name'][current_flavor]['fv_time_cnt']), 1.5)
        #     )
        # update normalized job data
        for job_name in dict_db['flavor_name'][current_flavor]['similar_data']:
            dict_db['flavor_name'][current_flavor]['similar_data'].update(
                {job_name: (
                    # job time norm dynamic
                    __calc_job_time_norm(
                        dict_db['flavor_name'][current_flavor]['similar_data'][job_name][1],
                        dict_db['flavor_name'][current_flavor]['similar_data'][job_name][2],
                        dict_db['flavor_name'][current_flavor]['fv_time_max'],
                        dict_db['flavor_name'][current_flavor]['fv_time_min']),
                    dict_db['flavor_name'][current_flavor]['similar_data'][job_name][1],
                    dict_db['flavor_name'][current_flavor]['similar_data'][job_name][2],
                    # job time norm static
                    __calc_job_time_norm(
                        dict_db['flavor_name'][current_flavor]['similar_data'][job_name][1],
                        dict_db['flavor_name'][current_flavor]['similar_data'][job_name][2],
                        conf_mode['job_time_long'],
                        conf_mode['job_time_short']),
                    # average job time
                    __division(dict_db['flavor_name'][current_flavor]['similar_data'][job_name][1],
                               dict_db['flavor_name'][current_flavor]['similar_data'][job_name][2]),
                )})

    __save_file(DATABASE_FILE, dict_db)
    return dict_db


def clear_job_names(job_name):
    """
    With similar job search, we certainly don't want to compare raw job names.
    Numbers in brackets are usually an indication just a counter for the same or similar job.
    :param job_name:
    :return: modified job name as string
    """
    if conf_mode['job_match_remove_num_brackets']:
        job_name = re.sub(r'\(\d+\)', '', job_name)
    if conf_mode['job_match_remove_numbers']:
        job_name = re.sub(r'\d+', '', job_name)
    if conf_mode['job_match_remove_pattern']:
        job_name = re.sub(conf_mode['job_match_remove_pattern'], '', job_name)
    if conf_mode['job_match_remove_text_within_parentheses']:
        job_name = re.sub(r'\([^()]*\)', '', job_name)

    return job_name


def version_check(version):
    """
    Compare passed version and with own version data and initiate an update in case of mismatch.
    If the program does not run via systemd, the user must carry out the update himself.
    :param version: current version from cloud api
    :return:
    """
    if version != VERSION:
        print(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
        # logger.error(OUTDATED_SCRIPT_MSG.format(SCRIPT_VERSION=VERSION, LATEST_VERSION=version))
        automatic_update()


def automatic_update():
    """
    Download the program from sources if automatic update is active.
    Initiate update if started with systemd option otherwise just download the updated version.
    :return:
    """
    logger.error("wrong autoscaling version detected, I try to upgrade!")
    if config_data['automatic_update']:
        if not systemd_start:
            download_autoscaling()
            sys.exit(1)
        update_autoscaling()
    else:
        sys.exit(1)


def update_autoscaling():
    """
    Download current autoscaling version and restart systemd autoscaling service.
    :return:
    """
    download_autoscaling()
    restart_systemd_service()


def restart_systemd_service():
    """
    Restart systemd service
    :return:
    """
    logger.debug("try restart systemd service")
    os.system("sudo service autoscaling restart")


def reset_autoscaling():
    """
    Reset settings and update to current autoscaling version.
    :return:
    """
    logger.info("re-download autoscaling configuration")
    # download_autoscaling()
    # download_autoscaling_config() TODO
    update_file_source("config")
    update_file_source("program")
    delete_database()
    if not os.path.exists(CLUSTER_PASSWORD_FILE):
        logger.info("cluster password file missing")
        __set_cluster_password()


def delete_database():
    """
    Clear job dictionary to avoid possible incompatibilities from new versions.
    :return:
    """

    if os.path.exists(DATABASE_FILE):
        logger.warning("delete old job dictionary in %s seconds press CTRL+C to cancel",
                       WAIT_CLUSTER_SCALING)
        time.sleep(WAIT_CLUSTER_SCALING)
        os.remove(DATABASE_FILE)
    else:
        logger.debug("no database file available for removal")


def download_autoscaling_config():
    """
    Download autoscaling configuration.
    This function will replace user settings to the latest default version.
    :return:
    """
    logger.warning("replacing autoscaling configuration in %s seconds press CTRL+C to cancel"
                   "\nmanual backup may be required before reset",
                   WAIT_CLUSTER_SCALING)
    time.sleep(WAIT_CLUSTER_SCALING)
    url = SOURCE_LINK_CONFIG

    try:
        logger.debug("download new autoscaling config")
        res = requests.get(url, allow_redirects=True)
        if res.status_code == 200:
            open(AUTOSCALING_FOLDER + 'autoscaling_config.yaml', 'wb').write(res.content)
        else:
            logger.error("unable to update autoscaling config from %s, status: %s", url, res.status_code)
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def update_file(filename, http_link, file_id, file_pw):
    headers = {
        'X-Requested-With': 'XMLHttpRequest',
    }
    try:
        response = requests.get(http_link, headers=headers, auth=(file_id, file_pw))
        if response.status_code == 200:
            open(AUTOSCALING_FOLDER + filename, 'wb').write(response.content)
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def update_file_source(source_file):
    """
    debug function
    TODO remove
    :return:
    """
    # sources_=__read_yaml_file(AUTOSCALING_FOLDER+'autoscaling_sources.yaml')['source']
    # update_file(sources_[source_file]['name'], sources_['http_link'], sources_[source_file]['id'], sources_[source_file]['pw'])


def download_autoscaling():
    """
    Download current version of the program.
    Use autoscaling url from configuration file.
    :return:
    """
    url = SOURCE_LINK

    try:
        logger.debug("download new autoscaling version")
        res = requests.get(url, allow_redirects=True)
        if res.status_code == 200:
            open(AUTOSCALING_FOLDER + 'autoscaling.py', 'wb').write(res.content)
        else:
            logger.error("unable to update autoscaling from %s, status: %s", url, res.status_code)
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


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

    if get_cluster_data() is None:
        logger.error("unable to receive cluster data")
        result_ = False

    fv = get_usable_flavors(False, True)
    if not (fv is None):
        logger.debug("test scale up %s", fv[-1])
        if not __cluster_scale_up_specific(fv[-1]['flavor']['name'], 1):
            logger.error("unable to receive flavor data")
            result_ = False
        else:
            if not __cluster_scale_down_complete():
                result_ = False
    else:
        result_ = False

    logger.info("function test result: %s", result_)

    return result_


def __configuration_watch(worker_build, build_time):
    """ DEBUG
    Backup worker build time with created workers to ansible times CSV."""
    if LOG_LEVEL != logging.DEBUG:
        return
    log_csv_watch = AUTOSCALING_FOLDER + '/autoscaling_ansible_times.csv'
    time_now = __get_time()
    weight = round(build_time / 10 * worker_build)  # only every 10 seconds
    w_data = [time_now, worker_build, build_time, weight]
    if not os.path.exists(log_csv_watch) or os.path.getsize(log_csv_watch) == 0:
        logger.debug("missing watch file, create file and header")
        header = [
            'time_now', 'worker_build', 'build_time', 'weight'
        ]
        __csv_writer(log_csv_watch, header)
    __csv_writer(log_csv_watch, w_data)


def __worker_watch():
    """DEBUG
    Watch worker data every 10 seconds and backup values to the watch CSV file."""
    log_csv_watch = AUTOSCALING_FOLDER + '/autoscaling_watch.csv'
    # logger_watch = setup_logger(AUTOSCALING_FOLDER + IDENTIFIER + '_csv_watch.log')
    print("start watcher")
    while True:
        time.sleep(10)

        worker_data, worker_cnt, worker_in_use, worker_drain, worker_drain_idle = receive_node_data_db(True)
        worker_idle = worker_cnt - worker_in_use

        # logger_watch.debug("worker_idle %s", worker_idle)

        time_now = __get_time()
        w_data = [time_now, worker_idle, worker_cnt, worker_in_use, worker_drain, len(worker_drain_idle)]
        # test if exit and not empty
        if not os.path.exists(log_csv_watch) or os.path.getsize(log_csv_watch) == 0:
            # logger_watch.debug("missing watch file, create file and header")
            header = [
                'time_now', 'worker_idle', 'worker_cnt', 'worker_in_use', 'worker_drain', 'worker_drain_idle'
            ]
            __csv_writer(log_csv_watch, header)
        __csv_writer(log_csv_watch, w_data)
        sys.stdout.flush()


if __name__ == '__main__':
    logger = None
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
            logger = setup_logger(AUTOSCALING_FOLDER + IDENTIFIER + '_csv.log')

    if not logger:
        logger = setup_logger(LOG_FILE)

    if len(sys.argv) == 2:
        if sys.argv[1] in ["-reset", "--reset"]:
            reset_autoscaling()
            sys.exit(0)
    if len(sys.argv) == 3:
        if sys.argv[2] in ["-m", "--m", "-mode", "--mode"]:
            logger.warning("mode parameter: %s", sys.argv[2])
            conf_mode, config_data = __setting_overwrite(sys.argv[2])
            logger.warning("settings overwrite successful")
        else:
            conf_mode, config_data = __setting_overwrite(None)
    else:
        conf_mode, config_data = __setting_overwrite(None)
    config_hash = generate_hash(FILE_CONFIG_YAML)
    scheduler = config_data['scheduler']

    # load scheduler interface
    if scheduler == "slurm":
        logger.info("scheduler selected: %s", scheduler)
        import pyslurm

        scheduler_interface = SlurmInterface()
    else:
        logger.error("unknown scheduler selected: %s", scheduler)
        sys.exit(1)
    cluster_id = read_cluster_id()
    cluster_pw = __get_cluster_password()

    # parameter with logging, config, scheduler and cluster data
    # temp csv log
    if len(sys.argv) == 2:
        if sys.argv[1] in ["-l", "--l", "-log", "--log"]:
            __csv_log_with_refresh_data('C', '0', '0')
            sys.exit(0)
        elif sys.argv[1] in ["-nd", "--nd", "-node", "--node"]:
            receive_node_data_live()
            receive_node_data_db(False)
            sys.exit(0)
        elif sys.argv[1] in ["-j", "--j", "-jobdata", "--jobdata"]:
            pprint(receive_job_data())
            sys.exit(0)
        elif sys.argv[1] in ["-fv", "--fv", "-flavors", "--flavors"]:
            print('flavors available:')
            get_dummy_worker(get_usable_flavors(False, True))
            sys.exit(0)
        elif sys.argv[1] in ["-c", "--c", "-clusterdata", "--clusterdata"]:
            pprint(get_cluster_data())
            sys.exit(0)
    # create PID file
    pidfile = create_pid_file()

    try:

        logger.info('\n#############################################'
                    '\n########### START SCALING PROGRAM ###########'
                    '\n#############################################')

        run_as_service = False
        systemd_start = False

        # watcher values
        worker_blocked = 0
        worker_idle_times = 0

        if len(sys.argv) > 1:
            arg = sys.argv[1]
            logger.debug('autoscaling with %s: ', ' '.join(sys.argv))
            if len(sys.argv) == 2:
                if arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    __cluster_scale_up_test(1)
                elif arg in ["-suc", "--suc", "-scaleupchoice", "--scaleupchoice"]:
                    __cluster_scale_up_choice()
                elif arg in ["-sdc", "--sdc", "-scaledownchoice", "--scaledownchoice"]:
                    __cluster_scale_down_choice()
                elif arg in ["-sdb", "--sdb", "-scaledownbatch", "--scaledownbatch"]:
                    __cluster_scale_down_choice_batch()
                elif arg in ["-sd", "--sd", "-scaledown", "--scaledown"]:
                    print('scale-down')
                    __cluster_scale_down_idle()
                elif arg in ["-sdi", "--sdi", "-scaledowncomplete", "--scaledowncomplete"]:
                    logger.warning("warning: complete scale down in %s sec ...", WAIT_CLUSTER_SCALING)
                    time.sleep(WAIT_CLUSTER_SCALING)
                    __cluster_scale_down_complete()
                elif arg in ["-csd", "--csd", "-clustershutdown", "--clustershutdown"]:
                    logger.warning("warning: delete all worker from cluster in %s sec ...", WAIT_CLUSTER_SCALING)
                    time.sleep(WAIT_CLUSTER_SCALING)
                    __cluster_shut_down()
                elif arg in ["-cdb", "--cdb", "-clusterbroken", "--clusterbroken"]:
                    logger.warning("warning: delete all broken worker from cluster in %s sec ...",
                                   WAIT_CLUSTER_SCALING)
                    time.sleep(WAIT_CLUSTER_SCALING)
                    worker_j, _, _, _, _ = receive_node_data_db(False)
                    __verify_cluster_workers(None, worker_j, True)
                elif arg in ["-rsc", "--rsc", "-rescale", "--rescale"]:
                    print('rescale cluster configuration')
                    rescale_cluster()
                elif arg in ["-cw", "--cw", "-checkworker", "--checkworker"]:
                    check_workers(Rescale.INIT)
                elif arg in ["-s", "-service", "--service"]:
                    run_as_service = True
                    __run_as_service()
                elif arg in ["-systemd", "--systemd"]:
                    systemd_start = True
                    run_as_service = True
                    logger.debug("... start autoscaling with systemd")
                    __run_as_service()
                elif arg in ["-t", "-test", "--test"]:
                    if not function_test():
                        sys.exit(1)
                else:
                    print("No usage found for param: ", arg)
                sys.exit(0)
            elif len(sys.argv) == 3:
                arg2 = sys.argv[2]
                if arg in ["-su", "--su", "-scaleup", "--scaleup"]:
                    print('scale-up')
                    if arg2.isnumeric():
                        __cluster_scale_up_test(int(arg2))
                    else:
                        logger.error("we need a number, for example: -su 2")
                        sys.exit(1)
                elif arg in ["-sus", "--sus", "-scaleupspecific", "--scaleupspecific"]:
                    print('scale-up')
                    __cluster_scale_up_specific(arg2, 1)
                elif arg in ["-sds", "--sds", "-scaledownspecific", "--scaledownspecific"]:
                    print('scale-down-specific')
                    if "worker" in arg2:
                        cluster_scale_down_specific_hostnames(arg2, Rescale.CHECK)
                    else:
                        logger.error("hostname parameter without 'worker'")
                        sys.exit(1)
                elif arg in ["-s", "-service", "--service"]:
                    conf_mode, config_data = __setting_overwrite(arg2)
                    run_as_service = True
                    __run_as_service()
                else:
                    print("No usage found for param: ", arg, arg2)
                    sys.exit(1)
                sys.exit(0)
        else:
            print('\npass a mode via parameter, use -h for help')
            multiscale(get_usable_flavors(True, True))
    finally:
        os.unlink(pidfile)

    sys.exit(0)
