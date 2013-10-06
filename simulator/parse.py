from job import Job

class Parse:
    def __init__(self, fname):
        self.fname = fname

class ParsePlacementWorkload(Parse):
    def parse(self):
        f = open(self.fname)
        jobs = []

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

        return jobs

class ParseDRFWorkload(Parse):
    def parse(self):
        f = open(self.fname)
        jobs = []

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

            job = Job(job_id, submit_time, inter_job_diff, map_size, shuffle_size, reduce_size,
                cpu_usage, mem_usage)
            jobs.append(job)

        return jobs
