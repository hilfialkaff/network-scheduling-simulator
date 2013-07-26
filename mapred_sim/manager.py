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
        self.allocStrategy = None
        self.h = hpy()
        self.workload = workload
        self.jobs = []
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def construct_flows(self, nodes):
        ids = [node.get_id() for node in nodes]
        mappers = ids[:len(ids)/2]
        reducers = ids[len(ids)/2:]

        flows = []
        bw = self.topology.get_bandwidth()

        for m in mappers:
            for r in reducers:
                flow = (m, r, bw / 10)
                flows.append(flow)

        return flows

    def permute(self, index, num_chosen, hosts, num_mr):
        ret = []

        if index == len(hosts) or num_chosen == num_mr:
            return ret

        ret.append([hosts[index]])

        _ret1 = self.permute(index + 1, num_chosen + 1, hosts, num_mr)
        _ret2 = self.permute(index + 1, num_chosen, hosts, num_mr)

        for _ in _ret1:
            if type(_) == list:
                ret.append([hosts[index]] + _)
            else:
                ret.append([hosts[index], _])
        for _ in _ret2:
            ret.append(_)

        return ret

    def generate_permutations(self, hosts, num_mr):
        # print "hosts: ", hosts
        p = self.permute(0, 0, hosts, num_mr)
        p = filter(lambda x: len(x) == num_mr, p)
        # print "permutations: ", p
        return p
        
    def place_mappers_reducers(self):
        flows = []
        hosts = self.graph.get_hosts()
        bandwidth = self.topology.get_bandwidth()
        max_util = 0
        i = 0 # XXX

        # TODO: Check for availability
        # TODO: Select only a subset of the nodes?
        for p in self.generate_permutations(hosts, self.num_mappers + self.num_reducers):
            flows = self.construct_flows(p)
            self.graph.set_comm_pattern(flows)
            max_util = max(max_util, self.compute_route())

            # XXX
            i += 1
            if i == 10:
                break

        return max_util
    
    def compute_route(self):
        bandwidth = self.topology.get_bandwidth()

        graph = self.graph.clone()

        self.allocStrategy = KShortestPathBWAllocation(2)
        return self.allocStrategy.compute(graph, bandwidth)

    def run(self):
        f = open(self.workload)
        i = 1

        for line in f:
            self.jobs.append(Job(line))
            max_util = self.place_mappers_reducers()
            print "Job ", i, " utilization: ", max_util

            # total_mappers = self.graph.get_total_mappers()
            # total_reducers = self.graph.get_total_reducers()

        f.close()

    def clean_up(self):
        del self.graph
        if self.allocStrategy:
            self.allocStrategy.clean_up()
