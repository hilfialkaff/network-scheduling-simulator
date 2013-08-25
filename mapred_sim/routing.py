from collections import deque
from random import choice, random
from sys import maxint
from copy import deepcopy
from pprint import pprint
from time import time, clock

from simulated_annealing import SimulatedAnnealing
from job_config import JobConfig
from node import Node
from flow import Flow
from utils import *
from graph import Graph
import flow
import link

"""
Computes optimal routing for the given flows in the datacenters

TODO:
- Might need to reschedule the algorithm again?
- Multiple jobs
- Simulated annealing utilization should be based on all jobs, not just one
"""
class Routing(object):
    def __init__(self, graph, num_mappers, num_reducers, *args):
        self.tmp = 1
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

        self.jobs_config = {}
        self.k_paths = {}
        self.build_k_paths()

    def is_full(self):
        ret = False
        num_free = 0

        for h in self.graph.get_hosts():
            if h.is_free():
                num_free += 1

        if num_free <= (self.num_mappers + self.num_reducers):
            ret = True

        return ret

    def clean_up(self):
        del self.valid_paths
        del self.used_paths
        del self.used_links

        self.valid_paths = {}
        self.used_paths = []
        self.used_links = []

    def build_k_paths(self):
        hosts_id = [node.get_id() for node in self.graph.get_hosts()]

        start = clock()

        for src in hosts_id:
            for dst in hosts_id:
                if src == dst:
                    continue

                if src not in self.k_paths:
                    self.k_paths[src] = {}
                if dst not in self.k_paths[src]:
                    self.k_paths[src][dst] = []

                if dst in self.k_paths and src in self.k_paths[dst]:
                    self.k_paths[src][dst] = self.k_paths[dst][src] # Symmetry
                else:
                    self.k_paths[src][dst] = self.k_path(src, dst, self.bandwidth/10) # XXX

        diff = clock() - start
        print "K-paths construction:", diff

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

                # If the path is valid according to heuristic per network topology
                # if self.graph.k_path_validity(path + [neighbor]):
                if neighbor not in path and bw >= desired_bw:
                    new_bw = min(bw, neighbor_bw)
                    new_path = path + [neighbor]
                    new_length = path_len + 1
                    q.push(new_length, new_path, new_bw)
                    # q.push(new_length + self.graph.k_path_heuristic(new_path), new_path, new_bw)

            if len(paths_found) > self.num_alt_paths:
                break

        # print "paths: ", paths_found
        return paths_found

    def build_paths(self):
        ret = True

        # Build path for all the communication pattern
        for c in self.comm_pattern:
            possible = self.k_paths[c[0]][c[1]]
            if not possible:
                ret = False
                break

            for v in possible:
                src_dst_pair = (v[1][0], v[1][-1])
                if src_dst_pair not in self.valid_paths:
                    self.valid_paths[src_dst_pair] = []
                self.valid_paths[src_dst_pair].append(v)

        # print "valid paths: ", self.valid_paths
        return ret

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

    @staticmethod
    def get_name():
        raise NotImplementedError("Method unimplemented in abstract class...")

"""
Routing algorithm using simulated annealing for both placement and routing + Floyd-Warshall
"""
class FWRouting(Routing):
    def __init__(self, *args):
        super(FWRouting, self).__init__(*args)
        self.distances = {}
        self.next_nodes = {}

        self.k_floyd_warshall()

    def build_paths(self):
        ret = True

        """ Build path for all the communication pattern """
        for c in self.comm_pattern:
            possible = self.floyd_warshall_path(c[0], c[1], c[2])
            if not possible:
                ret = False
                break

            for v in possible:
                src_dst_pair = (v[1][0], v[1][-1])
                if src_dst_pair not in self.valid_paths:
                    self.valid_paths[src_dst_pair] = []
                self.valid_paths[src_dst_pair].append(v)

        # print "valid paths: ", self.valid_paths
        return ret

    def _helper(self, src, dst, bandwidth):
        paths = []

        if self.distances[src][dst] == float("inf"):
            raise Exception("! Floyd-Warshall path")

        middle = self.next_nodes[src][dst]
        if dst in middle:
            paths.append([src, dst])
        else:
            for _middle in middle:
                paths_src_middle = self._helper(src, _middle, bandwidth)
                paths_middle_dst = self._helper(_middle, dst, bandwidth)

                for p1 in paths_src_middle:
                    for p2 in paths_middle_dst:
                        paths.append(p1[:-1] + p2)

        return paths

    def floyd_warshall_path(self, src, dst, bandwidth):
        paths_found = []
        start = clock()

        path = self._helper(src, dst, bandwidth)

        for p in path:
            paths_found.append((bandwidth, p))

        return paths_found

    """ Modified floyd-warshall algorithm to find top k-paths instead of only the shortest path """
    def k_floyd_warshall(self):
        start = clock()
        nodes_id = [node.get_id() for node in self.graph.get_nodes().values()]
        links = self.graph.get_links().keys()

        for src in nodes_id:
            for dst in nodes_id:
                if src not in self.distances:
                    self.distances[src] = {}
                    self.next_nodes[src] = {}
                if dst not in self.next_nodes[src]:
                    self.next_nodes[src][dst] = []

                if src == dst:
                    self.distances[src][dst] = 0
                else:
                    self.distances[src][dst] = float("inf")

        for link in links:
            [v1, v2] = link
            self.distances[v1][v2] = 1
            self.distances[v2][v1] = 1
            self.next_nodes[v1][v2] = [v2]
            self.next_nodes[v2][v1] = [v1]

        for i in nodes_id:
            for j in nodes_id:
                for k in nodes_id:
                    if i == j or i == k or j == k:
                        continue

                    if self.distances[j][k] == float("inf") or \
                        self.distances[j][i] + self.distances[i][k] < self.distances[j][k]:

                        self.distances[j][k] = self.distances[j][i] + self.distances[i][k]
                        self.next_nodes[j][k] = []
                        self.next_nodes[j][k].append(i)
                    elif self.distances[j][i] + self.distances[i][k] == self.distances[j][k]:
                        self.next_nodes[j][k].append(i)

        diff = clock() - start
        print "Floyd-Warshall construction:", diff

class OptimalRouting(Routing):
    @staticmethod
    def get_name():
        return "OR"

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
        i = 0

        for p in self._generate_permutations(hosts, self.num_mappers + self.num_reducers):
            flows = self.construct_flows(p)
            self.comm_pattern = flows
            self.graph.set_comm_pattern(flows)
            max_util = max(max_util, self.compute_route())

            ########################################################
            # XXX
            if max_util != 0:
                i += 1

            if i == 10:
                break
            ########################################################

            self.clean_up()

        return max_util

    def compute_route(self):
        ret = 0
        if self.build_paths():
            permutations = self.generate_permutations()
            permutations = self.prune_permutations(permutations)
            self.generate_graphs(permutations)
            ret = self.select_optimal_graph()

        return ret

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

            # XXX
            if i > self.num_alt_paths:
                break

    def select_optimal_graph(self):
        best_graph = None
        max_util = 0

        for links, path in zip(self.used_links, self.used_paths):
            self.graph.set_links(links)
            self.graph.set_flow(path)

            util = self.graph.compute_utilization()
            if util > max_util:
                max_util = util
                best_links = links
                best_flows = path

            # self.graph.print_links()
            self.reset()

        # print "max_util: ", max_util
        # return bestGraph
        return max_util

"""
Routing algorithm using simulated annealing for both placement and routing
"""
class AnnealingRouting(Routing):
    def __init__(self, *args):
        super(AnnealingRouting, self).__init__(*args)
        self.max_step = 100 # XXX

    @staticmethod
    def get_name():
        return "AR"

    def placement_init_state(self):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]

        return available_hosts[:self.num_mappers + self.num_reducers]

    def placement_generate_neighbor(self, state):
        hosts = self.graph.get_hosts()
        num_hosts = len(hosts)
        state_length = len(state)

        if random() > 0.5:
            host1 = choice(range(state_length/2))
            host2 = choice(range(state_length/2, state_length))
            hosts[host1], hosts[host2] = hosts[host2], hosts[host1]
        else:
            # Only take free hosts into consideration
            available_hosts = [h for h in self.graph.get_hosts() if h.is_free() and h not in state]

            host_to_remove = choice(range(len(state)))
            host_to_add = choice(available_hosts)

            state[host_to_remove] = host_to_add

        return state

    def set_placement(self, state):
        flows = self.construct_flows(state)
        self.comm_pattern = flows
        # print "comm pattern: ", self.comm_pattern
        self.graph.set_comm_pattern(flows)

    def placement_compute_util(self, state):
        self.set_placement(state)
        util = self.compute_route()
        util.set_placements(state[:])
        self.clean_up()

        return util

    def execute_job(self, job):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]
        util = JobConfig(0, None, None)

        # There are enough nodes to run the job
        if len(available_hosts) > (self.num_mappers + self.num_reducers):
            max_util = self.num_mappers * self.num_reducers * self.bandwidth
            max_step = self.max_step

            # Executing simulated annealing for map-reduce placement
            start = clock()
            simulated_annealing = SimulatedAnnealing(max_util, \
                                                     max_step, \
                                                     self.placement_init_state, \
                                                     self.placement_generate_neighbor, \
                                                     self.placement_compute_util)
            end = clock()

            util = simulated_annealing.run()

        return util

    def compute_route(self):
        util = JobConfig(0, None, None)
        max_step = self.max_step
        max_util = self.num_mappers * self.num_reducers * self.bandwidth

        if self.build_paths():
            # Executing simulated annealing for map-reduce routing
            simulated_annealing = SimulatedAnnealing(max_util, \
                                                     max_step, \
                                                     self.routing_init_state, \
                                                     self.routing_generate_neighbor, \
                                                     self.routing_compute_util)

            util = simulated_annealing.run()

        # print "util: ", util.get_util()
        return util

    def routing_init_state(self):
        return [path[0] for path in self.valid_paths.values()]

    def routing_generate_neighbor(self, state):
        state_length = len(state)
        ret = True

        # XXX: Will infinite loop if there is only one state
        path_to_change = choice(range(state_length))
        possible_paths = self.valid_paths.values()[path_to_change]
        while len(possible_paths) == 1:
            path_to_change = choice(range(state_length))
            possible_paths = self.valid_paths.values()[path_to_change]

        new_path = choice(range(len(possible_paths)))
        while possible_paths[new_path] == state[path_to_change]:
            new_path = choice(range(len(possible_paths)))

        state[path_to_change] = possible_paths[new_path]

        return state

    def routing_compute_util(self, state):
        self.add_previous_jobs()
        cloned_links = self.graph.clone_links()
        valid = True
        paths_used = []
        util = 0
        job_config = JobConfig(0, None, None)

        for p in state:
            bw, links = p[0], p[1]
            for l in range(len(links) - 1):
                link_id = Link.get_id(links[l], links[l + 1])
                link = cloned_links[link_id]
                link_bandwidth = link.get_bandwidth()

                if link_bandwidth < bw:
                    valid = False
                    break

                link.set_bandwidth(link_bandwidth - bw)

            if not valid:
                break
            else:
                paths_used.append(p)

        if valid:
            self.graph.set_links(cloned_links)
            all_paths_used = deepcopy(paths_used)
            for job in self.jobs_config.values():
                all_paths_used.extend(job.get_used_paths())
            self.graph.set_flow(all_paths_used)

            util = 0
            for p in paths_used:
                flow = self.graph.get_flow(Flow.get_id(p[1][0], p[1][-1]))
                util += (flow.get_requested_bandwidth() + flow.get_effective_bandwidth())

            job_config = JobConfig(util, copy_links(cloned_links), paths_used)
            self.reset()

        return job_config

    def get_job_config(self, job_num):
        return self.jobs_config[job_num]

    def add_job_config(self, job_num, config):
        self.jobs_config[job_num] = config

    def delete_job_config(self, job_num):
        if job_num not in self.jobs_config:
            raise Exception("Invalid job number...")
        else:
            job = self.jobs_config[job_num]
            links = job.get_links()
            placements = job.get_placements()
            paths = job.get_used_paths()

            del links
            del placements
            del paths
            del self.jobs_config[job_num]

    def add_previous_jobs(self):
        for job in self.jobs_config.values():
            flows = self.construct_flows(job.get_placements())
            self.comm_pattern.extend(flows)
            self.graph.merge_paths(job.get_used_paths())

    # Mark nodes executing the job as busy
    def update_nodes_status(self, i, job_config):
        for p in job_config.get_used_paths():
            host1 = self.graph.get_node(p[1][0])
            host2 = self.graph.get_node(p[1][-1])
            host1.set_job_id_executed(i)
            host2.set_job_id_executed(i)

    # Update per job utilization
    def update_jobs_utilization(self):
        used_paths = []

        self.add_previous_jobs()

        for job in self.jobs_config.values():
            used_paths.extend(job.get_used_paths())
        self.graph.set_flow(used_paths)

        for job in self.jobs_config.values():
            util = 0
            for p in job.get_used_paths():
                flow = self.graph.get_flow(Flow.get_id(p[1][0], p[1][-1]))
                util += (flow.get_requested_bandwidth() + flow.get_effective_bandwidth())

            job.set_util(util)

        self.reset()

    def reset(self):
        self.graph.reset()

"""
Routing algorithm using simulated annealing for only routing
"""
class HalfAnnealingRouting(AnnealingRouting):
    @staticmethod
    def get_name():
        return "HAR"

    def execute_job(self, job):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]
        hosts = []
        util = JobConfig(0, None, None)

        # There are enough nodes to run the job
        if len(available_hosts) > (self.num_mappers + self.num_reducers):
            for i in range(self.num_mappers + self.num_reducers):
                host_to_add = choice(available_hosts)

                while host_to_add in hosts:
                    host_to_add = choice(available_hosts)
                hosts.append(host_to_add)

            util = self.placement_compute_util(hosts)

        return util

"""
Routing algorithm using simulated annealing for only placement
"""
class HalfAnnealingRouting2(AnnealingRouting):
    @staticmethod
    def get_name():
        return "HAR2"

    def compute_route(self):
        util = JobConfig(0, None, None)
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util

"""
Routing algorithm with random placement and routing
"""
class RandomRouting(HalfAnnealingRouting):
    @staticmethod
    def get_name():
        return "RR"

    def compute_route(self):
        util = JobConfig(0, None, None)
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util
