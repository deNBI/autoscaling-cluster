import datetime
import os
import re
from subprocess import PIPE, Popen

import pyslurm
from classes.job_info import JobInfo
from constants import (
    JOB_FINISHED,
    JOB_PENDING,
    JOB_RUNNING,
    NODE_ALLOCATED,
    NODE_DOWN,
    NODE_DRAIN,
    NODE_IDLE,
    NODE_MIX,
)
from logger import setup_custom_logger
from scheduler_interface import SchedulerInterface
from utils import (
    convert_gb_to_mb,
    convert_gb_to_mib,
    convert_mib_to_gb,
    convert_tb_to_mb,
    convert_tb_to_mib,
)

logger = setup_custom_logger(__name__)


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

    def fetch_scheduler_node_data(self) -> list[pyslurm.Node]:
        """
        Read scheduler data from database and return a json object with node data.
        Verify database data.
        :return:
            - json object with node data
            - on error, return None
        """
        try:
            return pyslurm.Nodes.load()
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

    def job_data_live(self) -> list[JobInfo]:
        """
        Fetch live data from scheduler without database usage.
        Use squeue output from slurm, should be faster and up-to-date
        :return: List of JobInfo (no history)
        """
        squeue_json = self.get_json("squeue")
        job_info_list = []

        for i in squeue_json:
            mem = re.split(r"(\d+)", i["MIN_MEMORY"])
            disk = re.split(r"(\d+)", i["MIN_TMP_DISK"])
            comment = None if i["COMMENT"] == "(null)" else i["COMMENT"]
            state_id = self.__convert_job_state(i["STATE"])

            if state_id >= 0:
                job_info = JobInfo(
                    job_id=int(i["JOBID"]),
                    req_cpus=int(i["MIN_CPUS"]),
                    req_mem=self.memory_size_ib(mem[1], mem[2]),
                    state=state_id,
                    state_str=i["STATE"],
                    temporary_disk=self.memory_size(disk[1], disk[2]),
                    priority=int(i["PRIORITY"]),
                    name=i["NAME"],
                    req_gres=i["TRES_PER_NODE"],
                    nodes=i["NODELIST"],
                    elapsed_time=self.__time_to_seconds(i["TIME"]),
                    comment=comment,
                )
                job_info_list.append(job_info)
        return job_info_list

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


    def fetch_scheduler_job_data_by_range(self, start_time, end_time) -> list[JobInfo]:
        """
        Read scheduler data from database and return a json object with job data.
        Define the prerequisites for the json data structure and the required data.

        :param start_time: start time range
        :param end_time: end time range
        :return json object with job data, return None on error
        """
        try:
            db_filter = pyslurm.db.JobFilter(
                start_time=start_time.encode("utf-8"), end_time=end_time.encode("utf-8")
            )
            #TODO FIX
            jobs_list= [JobInfo(job_id=job.id,req_cpus=job.cpus,req_mem=job.memory,state=,state_str=job.state,temporary_disk=job.temporary_disk if hasattr(job,"temporary_disk") else 0,priority=job.priority,name=job.nam,req_gres=,nodes=job.nodelist,elapsed_time=elapsed_time,comment=job.comment) for job i pyslurm.db.Jobs.load(db_filter).values()n]

            return jobs_list
        except ValueError as e:
            logger.debug("Error: unable to receive job data \n%s", e)
            return None

    def fetch_scheduler_job_data(self, from_last_days: int) -> list[JobInfo]:
        """
                Read job data from database and return a json object with job data.
                :param     def fetch_scheduler_job_data(self, from_last_days:int):
        : number of past days
                :return: jobs_dict
        """
        start_time = (
            datetime.datetime.utcnow() - datetime.timedelta(days=from_last_days)
        ).strftime("%Y-%m-%dT00:00:00")
        end_time = (
            datetime.datetime.utcnow() + datetime.timedelta(days=from_last_days)
        ).strftime("%Y-%m-%dT00:00:00")
        return self.fetch_scheduler_job_data_by_range(
            start_time=start_time, end_time=end_time
        )

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
