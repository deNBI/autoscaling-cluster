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
