import sys
from random import random

from resource import Resource

class DRF:
    STATIC_DRF = 1 # Vanilla DRF
    INC_DRF = 2 # Incremental DRF

    def __init__(self, rsrc, jobs=None):
        self.R = rsrc # Total resource capabilities
        self.C = Resource() # Consumed resources, initially 0
        self.s = {} # Each job's dominant resource shares, initially 0
        self.U = {} # Resources given to job i, initially 0
        self.D = {} # Demand vector of of job i
        self.jobs_to_consider = {} # List of jobs to be considered

        for job in jobs:
            job_id = job.get_id()

            self.U[job_id] = Resource()
            self.D[job_id] = Resource(job.get_net_usage(), job.get_cpu_usage(), job.get_mem_usage())
            self.s[job_id] = 0
            self.jobs_to_consider[job_id] = job

        # print "Demand vector:", self.D

    """ Get dominant resource of a particular job """
    def get_dominant_rsrc(self, job_id):
        ret = 0
        rsrc = 0

        rsrc = self.U[job_id].get_net()/self.R.get_net()
        if  rsrc > ret:
            ret = rsrc

        rsrc = self.U[job_id].get_cpu()/self.R.get_cpu()
        if  rsrc > ret:
            ret = rsrc

        rsrc = self.U[job_id].get_mem()/self.R.get_mem()
        if  rsrc > ret:
            ret = rsrc

        return ret

    """ Main DRF algorithm """
    def _run(self):
        min_dom_rsrc = sys.maxint
        min_id = 0
        is_full = False # Set to true if no more resource can be allocated to the cluster

        for k in self.jobs_to_consider.keys():
            if self.s[k] < min_dom_rsrc:
                min_dom_rsrc = self.s[k]
                min_id = k

                if self.s[k] == 0:
                    break

        # If any of the resources is greater than the cluster resource, marks
        # cluster as full.
        if (self.D[min_id] + self.C) > self.R:
            is_full = True

        if not is_full:
            self.C += self.D[min_id]
            self.U[min_id] += self.D[min_id]
            self.s[min_id] = self.get_dominant_rsrc(min_id)

        # print "Consumed resources:", self.C
        # print "Resources given:", self.U

        # not full -> DRF hasn't converged
        return not is_full

    def run(self):
        change = True
        while change:
            change = self._run()

    """
    Reset cluster allocation for some jobs.
    @percent: Percentage of current cluster consumption to be rescheduled
    """
    def reset(self, percent):
        jobs = self.s.keys()

        for i in range(int(percent * len(self.s))):
            index = int(random() * (len(jobs) - 1))
            job_id = jobs[index]

            self.C -= self.U[job_id]
            self.U[job_id] = Resource()
            self.s[job_id] = 0

            del jobs[index]

    def add_jobs(self, jobs):
        for job in jobs:
            job_id = job.get_id()

            self.U[job_id] = Resource()
            self.D[job_id] = Resource(job.get_shuffle_size(), job.get_cpu_usage(), job.get_mem_usage())
            self.s[job_id] = 0
            self.jobs_to_consider[job_id] = job

    def get_resource_alloc(self, job_id):
        return self.U[job_id]
