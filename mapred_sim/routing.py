from collections import deque
from random import choice, sample
from sys import maxint
from copy import deepcopy
from pprint import pprint
from time import time
from utils import *
from graph import Graph
import flow
import link

"""
Computes optimal routing for the given flows in the datacenters

TODO:
- Take into consideration the number of mappers and reducers
"""
class OptimalRouting:
    def __init__(self, graph, num_mappers, num_reducers, *args):
        self.graph = graph
        self.bandwidth = self.graph.get_bandwidth()
        self.comm_pattern = None
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.k = args[0] # For k-shortest path algorithm
        self.num_alt_paths = args[1] # Number of alternative paths to cache
        self.max_intersections = args[2] # Fraction of intersections tolerable between the paths

        self.valid_paths = {}
        self.used_paths = []
        self.used_links = []

    def clean_up(self):
        del self.valid_paths
        del self.used_paths
        del self.used_links

        self.valid_paths = {}
        self.used_paths = []
        self.used_links = []

    def construct_flows(self, nodes):
        ids = [node.get_id() for node in nodes]
        mappers = ids[:len(ids)/2]
        reducers = ids[len(ids)/2:]

        flows = []
        bw = self.bandwidth

        for m in mappers:
            for r in reducers:
                flow = (m, r, bw / 10)
                flows.append(flow)

        return flows

    def _permute(self, index, num_chosen, hosts, num_mr):
        ret = []

        if index == len(hosts) or num_chosen == num_mr:
            return ret

        ret.append([hosts[index]])

        _ret1 = self._permute(index + 1, num_chosen + 1, hosts, num_mr)
        _ret2 = self._permute(index + 1, num_chosen, hosts, num_mr)

        for _ in _ret1:
            if type(_) == list:
                ret.append([hosts[index]] + _)
            else:
                ret.append([hosts[index], _])
        for _ in _ret2:
            ret.append(_)

        return ret

    def _generate_permutations(self, hosts, num_mr):
        p = self._permute(0, 0, hosts, num_mr)
        p = filter(lambda x: len(x) == num_mr, p)
        # print "_permutations: ", p
        return p
 
    def execute_job(self, job):
        flows = []
        hosts = self.graph.get_hosts()
        bandwidth = self.bandwidth
        max_util = 0
        i = 0 # XXX

        # TODO: Check for availability
        # TODO: Select only a subset of the nodes?
        for p in self._generate_permutations(hosts, self.num_mappers + self.num_reducers):
            flows = self.construct_flows(p)
            self.comm_pattern = flows
            self.graph.set_comm_pattern(flows)
            max_util = max(max_util, self.compute_route())

            # XXX
            i += 1
            if i == 10:
                break

            self.clean_up()

        return max_util

    def compute_route(self):
        self.build_paths()
        permutations = self.generate_permutations()
        permutations = self.prune_permutations(permutations)
        self.generate_graphs(permutations)

        return self.select_optimal_graph()

    def k_path(self, src, dst, desired_bw):
        # Find k shortest paths between src and dst which have sufficient bandwidth
        paths_found = []
        path = [src]
        q = PriorityQueue()
        q.push(0, path, desired_bw)

        # Uniform Cost Search
        while (q.is_empty() == False) and (len(paths_found) < self.k):
            path_len, path, bw = q.pop()

            # If last node on path is the destination
            if path[-1] == dst:
                paths_found.append((bw, path))
                continue

            node = self.graph.get_node(path[-1])

            # Add next neighbors to paths to explore
            for link in node.get_links():
                end_point = link.get_end_points()
                neighbor = end_point[0] if node.get_id() != end_point[0] else end_point[1]
                neighbor_bw = link.get_bandwidth()

                if neighbor not in path and bw >= desired_bw:
                    new_bw = min(bw, neighbor_bw)
                    new_path = path + [neighbor]
                    new_length = path_len + 1
                    q.push(new_length, new_path, new_bw)

            if len(paths_found) > self.num_alt_paths:
                break

        # print paths_found
        return paths_found

    def build_paths(self):
        # Build path for all the communication pattern
        for c in self.comm_pattern:
            possible = self.k_path(c[0], c[1], c[2])

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
        permutations = self.permute(0)
        # print "permutations: ", permutations
        return permutations

    def prune_permutations(self, permutations):
        new_permutations = []

        for permute in permutations:
            intersections = []
            num_intersections = 0
            total_path_lengths = sum([len(p[1]) for p in permute])
            valid = True

            for p in permute:
                links = p[1]
                for l in range(len(links) - 1):
                    if (links[l], links[l + 1]) not in intersections:
                        intersections.append((links[l], links[l + 1]))
                    else:
                        num_intersections += 1

                    if num_intersections > (total_path_lengths * self.max_intersections):
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
            cloned_links = self.graph.clone_links()
            valid = True
            paths_used = []

            for p in permute:
                bw, links = p[0], p[1]
                for l in range(len(links) - 1):
                    node1 = self.graph.get_node(links[l])
                    node2 = self.graph.get_node(links[l + 1])
                    link_id = self.graph.get_link(node1, node2).get_end_points()
                    link = cloned_links[link_id]
                    link_bandwidth = link.get_bandwidth()

                    # print "link bandwidth: ", link_bandwidth

                    if link_bandwidth < bw:
                        valid = False
                        break

                    link.set_bandwidth(link_bandwidth - bw)
                    
                if not valid:
                    break
                else:
                    paths_used.append(p)

            if valid:
                self.used_links.append(cloned_links)
                self.used_paths.append(paths_used)
                i+=1

            # TODO
            if i > self.num_alt_paths:
                break

    def select_optimal_graph(self):
        best_graph = None
        max_util = -float('inf')

        for links, path in zip(self.used_links, self.used_paths):
            self.graph.set_links(links)
            self.graph.set_flow(path)

            util = self.graph.compute_utilization()
            if util > max_util:
                max_util = util
                best_links = links
                best_flows = path
            
            # self.graph.print_links()
            self.graph.reset_links()
            self.graph.reset_flows()

        self.graph.reset()

        print "max_util: ", max_util
        # return bestGraph
        return max_util
