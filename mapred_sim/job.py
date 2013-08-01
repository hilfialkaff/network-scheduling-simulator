"""
Represents a job in map-reduce like environment.
"""
class Job:
    NOT_EXECUTED = 0
    EXECUTING = 1
    FINISHED = 2

    def __init__(self, line):
        tmp = line.split('\t')
        self.state = Job.NOT_EXECUTED
        self.job_id = tmp[0]
        self.submit_time = int(tmp[1])
        self.inter_job_diff = int(tmp[2])
        self.map_size = long(tmp[3])
        self.shuffle_size = long(tmp[4])
        self.reduce_size = long(tmp[5])

    def get_state(self):
        return self.state

    def get_job_id(self):
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
