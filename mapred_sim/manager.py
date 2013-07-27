from random import shuffle, randrange, sample, seed
from routing import KShortestPathBWAllocation
from pprint import pprint
from copy import deepcopy
from job import Job
from node import *
from topology import *
from utils import *
import math
from guppy import hpy

"""
Manager knows everything about the topology (available mappers, reducers)
and receives workload information.
"""
class Manager:
    def __init__(self, topology, workload, num_mappers, num_reducers):
        self.seed = randrange(100)
        self.topology = topology
        self.graph = topology.get_graph()
        self.nextTenantIdSeq = 1
        self.h = hpy()
        self.workload = workload
        self.jobs = []
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.allocStrategy = KShortestPathBWAllocation(2, self.graph, topology.get_bandwidth(), num_mappers=self.num_mappers, num_reducers=self.num_reducers)

    def run(self):
        f = open(self.workload)
        i = 1

        for line in f:
            self.jobs.append(Job(line))
            max_util = self.allocStrategy.place_mappers_reducers()

            print "Job ", i, " utilization: ", max_util

            # total_mappers = self.graph.get_total_mappers()
            # total_reducers = self.graph.get_total_reducers()
            break

        f.close()

    def clean_up(self):
        del self.graph
        if self.allocStrategy:
            self.allocStrategy.clean_up()
