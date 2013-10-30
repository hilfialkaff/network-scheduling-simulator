import time
import os

from algorithm import * # pyflakes_bypass
from topology import * # pyflakes_bypass
from utils import * # pyflakes_bypass
from random import randrange
from job import Job

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

    def __init__(self, topo, algorithm, routing_algo, jobs, \
        num_mappers, num_reducers, num_path=2, cpu=0, mem=0):
        self.seed = randrange(100)
        self.graph = topo.generate_graph()
        self.algorithm = algorithm(self.graph, routing_algo, num_mappers, num_reducers, num_path, 10, 0.5)
        self.jobs = jobs
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.cpu = cpu
        self.mem = mem

        self.f = open(Manager.LOG_NAME, 'a')
        self.t = float(0) # Virtual time in the datacenter

        self._write("%s %s %d %d\n" % (topo.get_name() + '_' + str(num_path), algorithm.get_name(), \
                    len(self.graph.get_hosts()), num_mappers))

    def _write(self, s):
        self.f.write(s)
        self.f.flush()
        os.fsync(self.f.fileno())

    def count_running_jobs(self):
        return len([job for job in self.jobs if job.get_state() == Job.EXECUTING])

    def jobs_finished(self):
        return len(self.jobs) == 0

    def dequeue_job(self):
        dequeued_jobs = []

        for job in self.jobs:
            if job.get_submit_time() <= self.t and job.get_state() == Job.NOT_EXECUTED:
                dequeued_jobs.append(job)
                break # TODO: Need to be able to schedule multiple jobs at once

            if job.get_submit_time() > self.t: # This job and the following jobs are for future time
                break

        return dequeued_jobs

    def execute_job(self, job):
        ret = None

        ret = self.algorithm.execute_job(job)

        return ret

    def mark_job_done(self, job):
        job_id = job.get_id()

        self._write("Job %d done at %d\n" % (job_id, self.t))
        self.algorithm.delete_job_config(job_id)
        self.jobs.remove(job)

        for host in self.graph.get_hosts():
            if host.get_job_id_executed() == job_id:
                host.set_free()

        self.algorithm.update_jobs_utilization()


    """
    Return: # of jobs finished
    """
    def update_jobs_progress(self):
        ret = 0

        for job in self.jobs:
            last_update = job.get_last_update()
            if job.get_state() == Job.EXECUTING and self.t >= (last_update + 1):
                job_id = job.get_id()
                util = self.algorithm.jobs_config[job_id].get_util()

                if job.get_shuffle_size() > 0:
                    job.update_data_left(util * (self.t - last_update))
                job.set_last_update(self.t)

                if job.get_data_left() <= 0:
                    self.mark_job_done(job)
                    ret += 1

        return ret

    """
    This function runs jobs that are available.

    Return: True if new jobs can be run, false if none of them can be run.
    """
    def run_new_jobs(self, jobs):
        ret = False

        for job in jobs:
            job_config = self.execute_job(job)

            # The job is actually executed
            if job_config.get_util() > 0:
                ret = True
                job_id = job.get_id()

                self._write("Executing job %d submitted at %d at %d\n" \
                    % (job_id, job.get_submit_time(), self.t))

                job.set_state(Job.EXECUTING)
                job.set_last_update(self.t)
                self.algorithm.add_job_config(job_id, job_config)
                self.algorithm.update_nodes_status(job_id, job_config)
                self.algorithm.update_jobs_utilization()

                self.print_jobs_utilization()

        return ret

    def accelerate(self, is_run):
        jobs = self.dequeue_job()

        if not is_run:
            jobs_done = 0
            while jobs_done == 0 and self.count_running_jobs() != 0:
                jobs_done = self.update_jobs_progress()
                self.t += 1

        # If all machines are used up or there is no new job, keep looping to
        # speed up
        while not self.jobs_finished() and (not len(jobs) or self.algorithm.is_full()):
            self.update_jobs_progress()
            self.t += 1
            jobs = self.dequeue_job()

    def run(self):
        # TODO: CLEANUP!!!!
        while not self.jobs_finished():
            start = time.clock()
            is_run = True
            new_jobs = self.dequeue_job()

            if new_jobs:
                is_run = self.run_new_jobs(new_jobs)

            diff = time.clock() - start
            self._write("Algorithm: %f\n" % diff)

            # TODO
            # if diff < 1:
            #     time.sleep(1 - diff)
            #     t += (1 - diff)
            # t += diff
            self.t += 1

            num_finished_jobs = self.update_jobs_progress()
            if num_finished_jobs > 0:
                is_run = True

            self.accelerate(is_run)

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
