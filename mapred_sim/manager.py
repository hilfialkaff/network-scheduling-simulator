from random import shuffle, randrange, sample, seed
from routing import OptimalRouting, AnnealingRouting
from pprint import pprint
from copy import deepcopy
from job import Job
from node import *
from topology import *
from utils import *
import math

"""
Manager knows everything about the topology (available mappers, reducers)
and receives workload information.

TODO:
- Make it asynchronous
- mark_job_done()
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
            if job.get_submit_time() == cur_time and job.get_state() == Job.NOT_EXECUTED:
                dequeued_jobs.append(job)
            if job.get_submit_time() > cur_time: # This job and the following jobs are for future time
                break
        return dequeued_jobs

    def execute_job(self, job):
        return self.routing.execute_job(job)

    def mark_job_done(self):
        pass

    def run(self):
        f = open(self.workload)
        t = 0 # Virtual time in the datacenter
        i = 1 # XXX

        for line in f:
            self.enqueue_job(line)

        # While there are jobs that are being executed in the system
        while not self.jobs_finished():
            jobs = self.dequeue_job(t)
            for job in jobs:
                max_util = self.execute_job(job)

                print "Job", i, "utilization: ", max_util
                i += 1
                return # XXX

            self.mark_job_done()
            t += 1

        f.close()

    def clean_up(self):
        del self.graph
        # if self.allocStrategy:
        #     self.allocStrategy.clean_up()
