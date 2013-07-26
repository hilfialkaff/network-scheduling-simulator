from collections import deque
from random import choice, sample
from sys import maxint
from copy import deepcopy
from pprint import pprint
from time import time
from utils import *
import flow
import link
from graph import *
from guppy import hpy

class KShortestPathBWAllocation:
    def __init__(self, k = 1, numAltPaths = 2, maxIntersections = 0.5):
        self.k = k # Parameter for k-shortest path
        self.numAltPaths = 10 # Number of alternative paths to cache
        self.maxIntersections = 0.5 # Fraction of intersections tolerable between the paths

        self.graph = None
        self.valid_paths = {}
        self.chosen_graphs = []
        self.chosen_paths = []
        self._chosen_graphs = []
        self.h = hpy()

    def clean_up(self):
        del self.graph
        del self.valid_paths

        for _ in self.chosen_graphs:
            del _
        for _ in self.chosen_paths:
            del _
        for _ in self._chosen_graphs:
            del _

    def getNextAvailableHostId(self, hostList, curHostId):
        "Return a host ID whose edge switch is not down"
        hostId = curHostId

        if hostId == "":
            hostId = sample(hostList, 1)[0]
            hostList.remove(hostId)

        while len(hostList) > 0:
            link = self.graph.getEdgeLinkForHost(hostId)

            if (link is not None) and link.isActive():
                return hostId

            hostId = sample(hostList, 1)[0]
            hostList.remove(hostId)

        return None # Should not hit here...

    def compute(self, g, b):
        self.graph = g
        self.flows = g.get_comm_pattern()
        self.bandwidth = b

        self.build_paths()
        permutations = self.generate_permutations()
        permutations = self.prunePermutations(permutations)
        self.generate_graphs(permutations)
        return self.selectOptimalGraph()

    def k_path(self, k, src, dst, desired_bw):
        # Find k shortest paths between src and dst which have sufficient bandwidth

        pathsFound = []
        path = [src]
        q = PriorityQueue()
        q.push(0, path, desired_bw)

        flatGraph = self.graph.get_flat_graph()

        # Uniform Cost Search
        while (q.isEmpty() == False) and (len(pathsFound) < k):
            path_len, path, bw = q.pop()

            # If last node on path is the destination
            if path[-1] == dst:
                pathsFound.append((bw, path))
                continue

            # Add next neighbors to paths to explore
            for neighbor, neighbor_bw in flatGraph[path[-1]].items():
                if neighbor not in path and bw >= desired_bw:
                    new_bw = min(bw, neighbor_bw)
                    new_path = path + [neighbor]
                    new_length = path_len + 1
                    q.push(new_length, new_path, new_bw)

        # print pathsFound
        return pathsFound

    def build_paths(self):
        # Build path for all the communication pattern
        for c in self.flows:
            possible = self.k_path(self.k, c[0], c[1], c[2])

            for v in possible:
                src_dst_pair = (v[1][0], v[1][-1])
                if src_dst_pair not in self.valid_paths:
                    self.valid_paths[src_dst_pair] = []
                self.valid_paths[src_dst_pair].append(v)

        # print "valid paths: ", self.valid_paths
        return

    def permute(self, index):
        ret = []
        if index == len(self.valid_paths.items()):
            return []

        # Recursively call permute, advancing the items to look at every call.
        for v in self.valid_paths.values()[index]:
            h = self.permute(index + 1)
            if len(h) == 0:
                ret.append(v)
            else:
                for w in h:
                    if type(w) == list:
                        ret.append([v] + w)
                    else:
                        ret.append([v, w])

        return ret

    def generate_permutations(self):
        # print "permutations: ", permute(0)
        return self.permute(0)

    def prunePermutations(self, permutations):
        new_permutations = []

        for permute in permutations:
            intersections = []
            numIntersection = 0
            total_path_lengths = sum([len(p[1]) for p in permute])
            valid = True

            for p in permute:
                links = p[1]
                for l in range(len(links) - 1):
                    if (links[l], links[l + 1]) not in intersections:
                        intersections.append((links[l], links[l + 1]))
                    else:
                        numIntersection += 1

                    if numIntersection > (total_path_lengths * self.maxIntersections):
                        valid = False
                        break

                if not valid:
                    break

            if valid:
                new_permutations.append(permute)

        return new_permutations

    def generate_graphs(self, permutations):
        # print "permutations: ", permutations

        i = 0
        for permute in permutations:
            new_graph = deepcopy(self.graph.get_flat_graph())

            valid = True
            paths_used = []

            for p in permute:
                bw, links = p[0], p[1]
                for l in range(len(links) - 1):
                    if new_graph[links[l]][links[l + 1]] < bw:
                        valid = False
                        break
                    new_graph[links[l]][links[l + 1]] -= bw
                    
                if not valid:
                    break
                else:
                    paths_used.append(p)

            if valid:
                self.chosen_graphs.append(new_graph)
                self.chosen_paths.append(paths_used)
                i+=1

            # TODO
            if i > self.numAltPaths:
                break

        for g, p in zip(self.chosen_graphs, self.chosen_paths):
            graph = Graph(g, self.flows)
            graph.set_flow(p)
            self._chosen_graphs.append(graph)

    def selectOptimalGraph(self):
        # Create Link Objects from Graph
        bestGraph = None
        maxUtil = -float('inf')

        for g in self._chosen_graphs:
            util = g.compute_utilization()
            if util > maxUtil:
                maxUtil = util
                bestGraph = g

        # print "Utilization: ", maxUtil
        # return bestGraph
        return maxUtil
