from job import Job

class ParsePlacementWorkload:
    def __init__(self, fname, num_jobs):
        self.fname = fname
        self.num_jobs = num_jobs # Max # of jobs to be simulated

    def parse(self):
        f = open(self.fname)
        jobs = []
        i = 0

        for line in f:
            tmp = line.split('\t')
            job_id = int(tmp[0].strip("job"))
            submit_time = int(tmp[1])
            inter_job_diff = int(tmp[2])
            map_size = long(tmp[3])
            shuffle_size = long(tmp[4])
            reduce_size = long(tmp[5])

            job = Job(job_id, submit_time, inter_job_diff, map_size, shuffle_size, reduce_size)
            jobs.append(job)

            i += 1
            if i == self.num_jobs:
                break

        return jobs

class ParseDRFWorkload:
    def __init__(self, fname, num_jobs, net_usage):
        self.fname = fname
        self.num_jobs = num_jobs # Max # of jobs to be simulated
        self.net_usage = net_usage

    def parse(self):
        f = open(self.fname)
        jobs = []
        i = 0

        for line in f:
            tmp = line.split('\t')
            job_id = int(tmp[0].strip("job"))
            submit_time = int(tmp[1])
            inter_job_diff = int(tmp[2])
            map_size = long(tmp[3])
            shuffle_size = long(tmp[4])
            reduce_size = long(tmp[5])
            cpu_usage = float(tmp[6])
            mem_usage = float(tmp[7])
            net_usage = self.net_usage # TODO: Need to think about job with different bandwidth requirement

            job = Job(job_id, submit_time, inter_job_diff, map_size, shuffle_size, reduce_size,
                cpu_usage, mem_usage, net_usage)
            jobs.append(job)

            i += 1
            if i == self.num_jobs:
                break

        return jobs
