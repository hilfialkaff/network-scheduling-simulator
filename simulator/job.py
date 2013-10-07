"""
Represents a job in map-reduce like environment.
"""
class Job:
    NOT_EXECUTED = 0
    EXECUTING = 1

    def __init__(self, job_id, submit_time, inter_job_diff, map_size, shuffle_size,
        reduce_size, cpu_usage=0, mem_usage=0, net_usage=0):
        self.state = Job.NOT_EXECUTED
        self.job_id = job_id
        self.submit_time = submit_time
        self.inter_job_diff = inter_job_diff
        self.map_size = map_size
        self.shuffle_size = shuffle_size
        self.reduce_size = reduce_size
        self.cpu_usage = cpu_usage
        self.mem_usage = mem_usage
        self.net_usage = net_usage
        self.data_left = self.shuffle_size
        self.last_update = 0

    def get_state(self):
        return self.state

    def set_state(self, state):
        self.state = state

    def get_id(self):
        return self.job_id

    def get_submit_time(self):
        return self.submit_time

    def get_inter_job_diff(self):
        return self.inter_job_diff

    def get_map_size(self):
        return self.map_size

    def get_shuffle_size(self):
        return self.shuffle_size

    def get_reduce_size(self):
        return self.reduce_size
    def get_data_left(self):
        return self.data_left

    def update_data_left(self, util):
        if self.data_left > util:
            self.data_left -= util
        else:
            self.data_left = 0

        return self.data_left

    def get_last_update(self):
        return self.last_update

    def set_last_update(self, last_update):
        self.last_update = last_update

    def get_cpu_usage(self):
        return self.cpu_usage

    def set_cpu_usage(self, cpu_usage):
        self.cpu_usage = cpu_usage

    def get_mem_usage(self):
        return self.mem_usage

    def set_mem_usage(self, mem_usage):
        self.mem_usage = mem_usage

    def get_net_usage(self):
        return self.net_usage

    def set_net_usage(self, net_usage):
        self.net_usage = net_usage
