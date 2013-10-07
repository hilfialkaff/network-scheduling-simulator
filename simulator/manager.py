from random import randrange
from algorithm import *
from job import Job
from drf import DRF
from node import *
from topology import *
from utils import *
from resource import Resource

import logging
import time
import os

"""
Manager knows everything about the topology (available mappers, reducers)
and receives workload information.

TODO:
- Number of mappers/reducers not necessarily static
- Take into account link failures
- Apps might have min. bandwidth requirement (check for bandwidth/10)
- Split jobs to have jobs that are not run and those that have
"""
class Manager:
    LOG_NAME = "./logs/simulator.log"

    def __init__(self, topo, algorithm, routing_algo, num_host, jobs, \
        num_mappers, num_reducers, cpu=0, mem=0, with_drf=0):
        self.seed = randrange(100)
        self.graph = topo.generate_graph()
        self.algorithm = algorithm(self.graph, routing_algo, num_mappers, num_reducers, 2, 10, 0.5)
        self.jobs = jobs
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.cpu = cpu
        self.mem = mem
        self.with_drf = with_drf

        self.f = open(Manager.LOG_NAME, 'a')
        self.t = float(0) # Virtual time in the datacenter

        self._write("%s %s %d %d\n" % (topo.get_name(), algorithm.get_name(), num_host, num_mappers))

        if self.with_drf == DRF.INC_DRF:
            self.drf = DRF([net, cpu, mem])

    def _write(self, s):
        self.f.write(s)
        self.f.flush()
        os.fsync(self.f.fileno())

    def jobs_finished(self):
        return len(self.jobs) == 0

    def dequeue_job(self):
        dequeued_jobs = []

        for job in self.jobs:
            if job.get_submit_time() <= self.t and job.get_state() == Job.NOT_EXECUTED:
                dequeued_jobs.append(job)
                break # XXX: Need to be able to schedule multiple jobs at once

            if job.get_submit_time() > self.t: # This job and the following jobs are for future time
                break

        return dequeued_jobs

    def execute_job(self, job):
        ret = None

        if self.with_drf:
            rsrc = self.drf.get_resource_alloc(job.get_id())
            print rsrc
            ret = self.algorithm.execute_job(job, rsrc)
        else:
            ret = self.algorithm.execute_job(job)

        return ret

    def update_jobs_progress(self):
        for job in self.jobs:
            last_update = job.get_last_update()
            if job.get_state() == Job.EXECUTING and self.t >= (last_update + 1):
                job_id = job.get_id()
                util = self.algorithm.jobs_config[job_id].get_util()

                job.update_data_left(util * (self.t - last_update))
                job.set_last_update(self.t)

                if job.get_data_left() <= 0:
                    self._write("Job %d done at %d\n" % (job_id, self.t))
                    self.algorithm.delete_job_config(job_id)
                    self.jobs.remove(job)

                    for host in self.graph.get_hosts():
                        if host.get_job_id_executed() == job_id:
                            host.set_free()

                    self.algorithm.update_jobs_utilization()

    def run_drf(self, new_jobs):
        jobs_to_consider = new_jobs
        for job in self.jobs:
            if job.get_state() == Job.EXECUTING:
                jobs_to_consider.append(job)

        if len(jobs_to_consider) > 0:
            num_hosts = len(self.graph.get_nodes())
            total_cpu = self.cpu * num_hosts
            total_mem = self.mem * num_hosts
            total_net = self.num_mappers * self.graph.get_bandwidth()
            total_rsrc = Resource(total_net, total_cpu, total_mem)

            print "Jobs in DRF:", jobs_to_consider
            self.drf = DRF(total_rsrc, jobs_to_consider)
            self.drf.run()

    def run_new_jobs(self, jobs):
        for job in jobs:
            job_config = self.execute_job(job)

            # The job is actually executed
            if job_config.get_util() > 0:
                job_id = job.get_id()

                self._write("Executing job %d submitted at %d at %d\n" \
                    % (job_id, job.get_submit_time(), self.t))

                job.set_state(Job.EXECUTING)
                job.set_last_update(self.t)
                self.algorithm.add_job_config(job_id, job_config)
                self.algorithm.update_nodes_status(job_id, job_config)
                self.algorithm.update_jobs_utilization()

                self.print_jobs_utilization()

    def accelerate(self):
        jobs = self.dequeue_job()

        # XXX: If all machines are used up or there is no new job, keep looping
        # Might need to be modified on adaptive machine.
        while len(self.jobs) and (not len(jobs) or self.algorithm.is_full()):
            self.update_jobs_progress()
            self.t += 1
            jobs = self.dequeue_job()

    def run(self):
        # While there are jobs that are being executed in the system
        while not self.jobs_finished():
            start = time.clock()

            new_jobs = self.dequeue_job()
            if self.with_drf:
                self.run_drf(new_jobs)
            self.run_new_jobs(new_jobs)
            self.update_jobs_progress()
            diff = time.clock() - start

            self._write("Algorithm: %f\n" % diff)
            # XXX
            # if diff < 1:
            #     time.sleep(1 - diff)
            #     t += (1 - diff)
            # t += diff
            self.t += 1

            self.accelerate()

    def clean_up(self):
        self._write("\n")
        self.f.close()
        del self.graph

    def print_jobs_utilization(self):
        for job in self.jobs:
            if job.get_state() != Job.NOT_EXECUTED:
                job_id = job.get_id()
                self._write("Job %d has utilization: %d\n" % \
                    (job_id, self.algorithm.get_job_config(job_id).get_util()))