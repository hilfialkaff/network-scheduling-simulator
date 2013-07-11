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

# TODO: VERY MESSY
class BWAllocation(object):
    def compute(self, g, b):
        pass


class KShortestPathBWAllocation(BWAllocation):
    def __init__(self, k = 1, numAltPaths = 10, maxIntersections = 0.5):
        self.k = k # Parameter for k-shortest path
        self.numAltPaths = 10 # Number of alternative paths to cache
        self.maxIntersections = 0.5 # Fraction of intersections tolerable between the paths

        self.graph = None
        self.valid_paths = {}
        self.chosen_graphs = []
        self.chosen_paths = []
        self._chosen_graphs = []
        self.h = hpy()

    def cleanUp(self):
        del self.graph
        del self.valid_paths

        for _ in self.chosen_graphs:
            del _
        for _ in self.chosen_paths:
            del _
        for _ in self._chosen_graphs:
            del _

    # TODO: What if it's brought up
    def takeRandomLinkDown(self):
        link = self.graph.takeRandomLinkDown()

        # print "Took link down: ", link
        for g in self._chosen_graphs:
            f = g.getFlowAllocation(link)
            if f:
                self._chosen_graphs.remove(g)

    def simulatePercentageLinkFailures(self, p = 0.1):
        if p > 1:
            return  # Invalid

        print "Simulating %d%% link failures" % (p * 100)
        links = self.graph.getLinks()
        numOfLinks = int(len(links) * p)

        for i in range(numOfLinks):
            self.takeRandomLinkDown()

        # self.graph.build()

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

    def recoverBrokenFlows(self):
        flows = self.graph.getFlows()
        brokenFlows = [f for f in flows.values() if not f.isActive()]

        if len(brokenFlows) == 0:
            print "No flows are affected."
            return

        hostSet = set([h.getId() for h in self.graph.getHosts()])

        for f in flows.keys():
            hostSet.discard(f[0])
            hostSet.discard(f[1])

        # print "Broken flows: ", [f.getEndPoints() for f in brokenFlows]

        # Build up comm pattern connectivity dictionary
        while len(brokenFlows) > 0:
            availableHosts = list(hostSet)
            fl = brokenFlows[0]
            endPoints = fl.getEndPoints()
            srcHostId = endPoints[0]
            dstHostId = endPoints[1]
            bw = fl.getRequestedBandwidth()

            pathFound = False
            path = None

            while len(availableHosts) > 0:
                srcHostId = self.getNextAvailableHostId(availableHosts, srcHostId)
                dstHostId = self.getNextAvailableHostId(availableHosts, dstHostId)

                path = self.k_path(1, srcHostId, dstHostId, bw)
                if len(path):
                    pathFound = True
                    break
                else:
                    srcHostId = ""
                    dstHostId = ""

            if pathFound:
                hostSet.discard(srcHostId)
                hostSet.discard(dstHostId)

                newFlow = self.graph.addFlow(srcHostId, dstHostId, bw, path[0][1])
                newEndPoints = newFlow.getEndPoints()

                # if newEndPoints == endPoints:
                #     print "Fixed flow", endPoints, "with a new path"
                # else:
                #     print "Migrated flow", endPoints, "to", newEndPoints

            brokenFlows.remove(fl)
            self.graph.removeFlow(fl)

        print "Fixed Graph Utilization: ", self.graph.computeUtilization()        


    def compute(self, g, b):
        self.graph = g
        self.flows = g.getCommPattern()
        self.bandwidth = b

        # stopwatch = StopWatch()
        brokenCP = []

        # stopwatch.Start()
        self.build_paths(brokenCP)

        # while not self.build_paths(brokenCP):
        #     self.recoverBrokenCommunicationPatterns(brokenCP)

        # stopwatch.Log('Build paths')

        # stopwatch.Start()
        permutations = self.generate_permutations()
        permutations = self.prunePermutations(permutations)
        # stopwatch.Log('Generate Permutations')

        # stopwatch.Start()
        self.generate_graphs(permutations)
        # stopwatch.Log('Generate Graphs')

        # stopwatch.Start()
        self.graph = self.selectOptimalGraph()
        # stopwatch.Log('Total time')

    def k_path(self, k, src, dst, desired_bw):
        # Find k shortest paths between src and dst which have sufficient bandwidth

        pathsFound = []
        path = [src]
        q = PriorityQueue()
        q.push(0, path, desired_bw)

        flatGraph = self.graph.getFlatGraph()

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
        del q
        return pathsFound

    def build_paths(self, brokenFlows=[]):
        brokenFlows[:] = []

        # Build path for all the communication pattern
        for c in self.flows:
            possible = self.k_path(self.k, c[0], c[1], c[2])

            if len(possible) == 0:
                brokenFlows.append(c)
                continue

            for v in possible:
                src_dst_pair = (v[1][0], v[1][-1])
                if src_dst_pair not in self.valid_paths:
                    self.valid_paths[src_dst_pair] = []
                self.valid_paths[src_dst_pair].append(v)

        # print "valid paths: ", self.valid_paths
        return (len(brokenFlows) == 0)

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
            new_graph = deepcopy(self.graph.getFlatGraph())

            valid = True
            paths_used = []

            for p in permute:
                bw, links = p[0], p[1]
                for l in range(len(links) - 1):
                    # print "link: ", links[l], " ", links[l + 1], " remaining: ", new_graph[links[l]][links[l + 1]], " bandwidth: ", bw
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
            graph.setFlow(p)
            self._chosen_graphs.append(graph)

    def selectOptimalGraph(self):
        # Create Link Objects from Graph
        bestGraph = None
        maxUtil = -float('inf')

        for g in self._chosen_graphs:
            util = g.computeUtilization()
            if util > maxUtil:
                maxUtil = util
                bestGraph = g

        print "Max Graph Utilization: ", maxUtil
        return bestGraph

    def getAverageUtilization(self, links):
        if len(links) == 0:
            return 0.0

        aggUtil = float(sum([l.getUtilization() for l in links])/len(links))
        return aggUtil
