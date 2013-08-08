from random import shuffle, randrange, sample, seed
from routing import OptimalRouting, AnnealingRouting
from pprint import pprint
from copy import deepcopy
from job import Job
from node import *
from topology import *
from utils import *
import math
import timeit

"""
Manager knows everything about the topology (available mappers, reducers)
and receives workload information.

TODO:
- Number of mappers/reducers not necessarily static
- Bandwidth requirement per job might be different
- Take into account link failures
"""
class Manager:
    def __init__(self, graph, workload, num_mappers, num_reducers):
        self.seed = randrange(100)
        self.graph = graph
        self.workload = workload
        self.jobs = []
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.routing = AnnealingRouting(self.graph, num_mappers, num_reducers, 2, 10, 0.5)

    def jobs_finished(self):
        return len(self.jobs) == 0

    def enqueue_job(self, line):
        job = Job(line)
        self.jobs.append(job)

    def dequeue_job(self, cur_time):
        dequeued_jobs = []
        for job in self.jobs:
            if job.get_submit_time() <= cur_time and job.get_state() == Job.NOT_EXECUTED:
                dequeued_jobs.append(job)
            if job.get_submit_time() > cur_time: # This job and the following jobs are for future time
                break
        return dequeued_jobs

    def execute_job(self, job):
        return self.routing.execute_job(job)

    def update_jobs_progress(self, t):
        for job in self.jobs:
            if job.get_state() == Job.EXECUTING:
                job_id = job.get_id()
                util = self.routing.jobs_config[job_id].get_util()

                job.update_data_left(util)
                if job.get_data_left() <= 0:
                    print "Job", job_id, "done at", t
                    self.routing.delete_job_config(job_id)
                    self.jobs.remove(job)

                    for host in self.graph.get_hosts():
                        if host.get_job_id_executed() == job_id:
                            host.set_free()

                    self.routing.update_jobs_utilization()

    def run(self):
        f = open(self.workload)
        t = 0 # Virtual time in the datacenter
        i = 1 # XXX

        def loop():
            jobs = self.dequeue_job(t)

            for job in jobs:
                job_config = self.execute_job(job)

                # The job is actually executed
                if job_config.get_util() > 0:
                    job_id = job.get_id()

                    print "Executing job %d submitted at %d at %d" % (job_id, job.get_submit_time(), t)

                    job.set_state(Job.EXECUTING)
                    self.routing.add_job_config(job_id, job_config)
                    self.routing.update_nodes_status(job_id, job_config)
                    self.routing.update_jobs_utilization()

                    self.print_jobs_utilization()

        for line in f:
            self.enqueue_job(line)
            i += 1
            if i == 50:
                break

        # While there are jobs that are being executed in the system
        while not self.jobs_finished():
            time_taken = timeit.timeit(loop)
            t += time_taken

            self.update_jobs_progress(t)

        f.close()

    def clean_up(self):
        del self.graph

    def print_jobs_utilization(self):
        for job in self.jobs:
            if job.get_state() != Job.NOT_EXECUTED:
                job_id = job.get_id()
                print "Job", job_id, "has utilization:", self.routing.get_job_config(job_id).get_util()
