from random import shuffle, randrange, sample, seed
from routing import OptimalRouting, AnnealingRouting
from pprint import pprint
from copy import deepcopy
from job import Job
from node import *
from topology import *
from utils import *

import time # clock
import sys # stdout
import os # fsync

"""
Manager knows everything about the topology (available mappers, reducers)
and receives workload information.

TODO:
- Number of mappers/reducers not necessarily static
- Bandwidth requirement per job might be different
- Take into account link failures
"""
class Manager:
    LOG_NAME = "./log"

    def __init__(self, topo, routing, num_host, workload, num_mappers, num_reducers):
        self.seed = randrange(100)
        self.graph = topo.generate_graph()
        self.workload = workload
        self.jobs = []
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.routing = routing(self.graph, num_mappers, num_reducers, 2, 10, 0.5)
        self.f = open(Manager.LOG_NAME, 'a')

        self._write("%s %s %d %d\n" % (topo.get_name(), routing.get_name(), num_host, num_mappers))

    def _write(self, s):
        self.f.write(s)
        self.f.flush()
        os.fsync(self.f.fileno())

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
            if job.get_state() == Job.EXECUTING and t >= (job.get_last_update() + 1):
                job_id = job.get_id()
                util = self.routing.jobs_config[job_id].get_util()

                job.set_last_update(t)
                job.update_data_left(util)
                if job.get_data_left() <= 0:
                    self._write("Job %d done at %d\n" % (job_id, t))
                    self.routing.delete_job_config(job_id)
                    self.jobs.remove(job)

                    for host in self.graph.get_hosts():
                        if host.get_job_id_executed() == job_id:
                            host.set_free()

                    self.routing.update_jobs_utilization()

    def loop(self, t):
        # print "Beginning of loop"
        jobs = self.dequeue_job(t)

        for job in jobs:
            job_config = self.execute_job(job)

            # The job is actually executed
            if job_config.get_util() > 0:
                job_id = job.get_id()
                # print "job", job.get_id(), "executed"

                self._write("Executing job %d submitted at %d at %d\n" \
                    % (job_id, job.get_submit_time(), t))

                job.set_state(Job.EXECUTING)
                job.set_last_update(t)
                self.routing.add_job_config(job_id, job_config)
                self.routing.update_nodes_status(job_id, job_config)
                self.routing.update_jobs_utilization()

                self.print_jobs_utilization()
            else:
                pass
                # print "job", job.get_id(), "cannot be executed"

        self.update_jobs_progress(t)

    def accelerate(self, t):
        jobs = self.dequeue_job(t)

        # XXX: If all machines are used up or there is no new job, keep looping
        while len(self.jobs) and (not len(jobs) or self.routing.is_full()):
            self.update_jobs_progress(t)
            t += 1
            jobs = self.dequeue_job(t)

        return t

    def run(self):
        f = open(self.workload)
        t = float(0) # Virtual time in the datacenter
        i = 1 # XXX

        for line in f:
            self.enqueue_job(line)
            i += 1
            if i == 30:
                break
        f.close()

        # While there are jobs that are being executed in the system
        while not self.jobs_finished():
            start = time.clock()
            self.loop(t)
            diff = time.clock() - start

            if diff < 1:
                time.sleep(1 - diff)
                t += (1 - diff)
            t += diff

            t = self.accelerate(t)

    def clean_up(self):
        self._write("\n")
        self.f.close()
        del self.graph

    def print_jobs_utilization(self):
        for job in self.jobs:
            if job.get_state() != Job.NOT_EXECUTED:
                job_id = job.get_id()
                self._write("Job %d has utilization: %d\n" % \
                    (job_id, self.routing.get_job_config(job_id).get_util()))
