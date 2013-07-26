class Job:
    def __init__(self, line):
        tmp = line.split('\t')
        self.job_id = tmp[0]
        self.submit_time = tmp[1]
        self.inter_job_diff = tmp[2]
        self.map_size = tmp[3]
        self.shuffle_size = tmp[4]
        self.reduce_size = tmp[5]

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
