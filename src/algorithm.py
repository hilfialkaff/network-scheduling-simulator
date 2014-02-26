from random import choice, random
from copy import deepcopy
import logging # pyflakes_bypass

from simulated_annealing import SimulatedAnnealing
from job_config import JobConfig
from flow import Flow
from utils import * # pyflakes_bypass

"""
Computes optimal routing for the given flows in the datacenters

TODO:
- Might need to reschedule the algorithm again?
- Multiple jobs
- Algorithm utilization should be based on all jobs, not just one
- Job with minimum bandwidth requirement
"""
class Algorithm(object):
    K_PATH = 1
    FLOYD_WARSHALL = 2

    def __init__(self, graph, algo, num_mappers, num_reducers, paths):
        self.tmp = 1
        self.graph = graph
        self.bandwidth = self.graph.get_bandwidth()
        self.comm_pattern = None
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

        self.valid_paths = {}
        self.used_paths = []
        self.used_links = []

        self.jobs_config = {}

        if algo == Algorithm.K_PATH:
            self.k_paths = paths.get_k_paths()
            self.build_paths = self.k_build_paths
        elif algo == Algorithm.FLOYD_WARSHALL:
            self.distances = paths.get_distances()
            self.next_nodes = paths.get_next_nodes()
            self.build_paths = self.fw_build_paths

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

    def k_build_paths(self):
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

    def fw_build_paths(self):
        ret = True

        """ Build path for all the communication pattern """
        for c in self.comm_pattern:
            possible = self.fw_paths(c[0], c[1], c[2])
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

    def _fw_paths(self, src, dst, bandwidth):
        paths = []

        if self.distances[src][dst] == float("inf"):
            raise Exception("! Floyd-Warshall path")

        middle = self.next_nodes[src][dst]
        if dst in middle:
            paths.append([src, dst])
        else:
            for _middle in middle:
                paths_src_middle = self._fw_paths(src, _middle, bandwidth)
                paths_middle_dst = self._fw_paths(_middle, dst, bandwidth)

                for p1 in paths_src_middle:
                    for p2 in paths_middle_dst:
                        paths.append(p1[:-1] + p2)

        return paths

    def fw_paths(self, src, dst, bandwidth):
        paths_found = []

        path = self._fw_paths(src, dst, bandwidth)

        for p in path:
            paths_found.append((bandwidth, p))

        return paths_found

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
Simulated annealing algorithm using simulated annealing for both placement and routing
"""
class AnnealingAlgorithm(Algorithm):
    def __init__(self, *args):
        super(AnnealingAlgorithm, self).__init__(*args)
        self.max_step = 100 # TODO

    @staticmethod
    def get_name():
        return "AA"

    def placement_init_state(self):
        hosts = [h for h in self.graph.get_hosts() if h.is_free()]
        hosts_index = range(len(hosts))
        state = []

        for i in range(self.num_mappers + self.num_reducers):
            index = choice(hosts_index)
            state.append(hosts[index])
            hosts_index.remove(index)

        return state

    def placement_generate_neighbor(self, state):
        hosts = self.graph.get_hosts()
        state_length = len(state)

        if random() > 0.5:
            # Swap placement of one mapper and one reducer
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

        util = JobConfig()

        # There are enough nodes to run the job
        if len(available_hosts) > (self.num_mappers + self.num_reducers):
            max_util = self.num_mappers * self.num_reducers * self.bandwidth
            max_step = self.max_step

            # Executing simulated annealing for map-reduce placement
            simulated_annealing = SimulatedAnnealing(max_util, \
                                                     max_step, \
                                                     self.placement_init_state, \
                                                     self.placement_generate_neighbor, \
                                                     self.placement_compute_util)

            util = simulated_annealing.run()

        return util

    def compute_route(self):
        util = JobConfig()
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
        state = []

        for path in self.valid_paths.values():
            state.append(choice(path))

        return state

    def routing_generate_neighbor(self, state):
        state_length = len(state)

        # TODO: Will infinite loop if there is only one state
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
        cloned_links = self.graph.copy_links()
        valid = True
        paths_used = []
        util = 0
        job_config = JobConfig()

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
                # logging.debug(str(state) + " cannot be built")
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
            total_util = 0
            for p in paths_used:
                flow = self.graph.get_flow(Flow.get_id(p[1][0], p[1][-1]))
                util += (flow.get_requested_bandwidth() + flow.get_effective_bandwidth())

            for p in all_paths_used:
                flow = self.graph.get_flow(Flow.get_id(p[1][0], p[1][-1]))
                total_util += (flow.get_requested_bandwidth() + flow.get_effective_bandwidth())

            job_config = JobConfig(util, total_util, copy_links(cloned_links), paths_used)
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
        nodes = []

        for p in job_config.get_used_paths():
            node = self.graph.get_node(p[1][0])
            if node not in nodes:
                nodes.append(node)

            node = self.graph.get_node(p[1][-1])
            if node not in nodes:
                nodes.append(node)

        for node in nodes:
            node.set_job_id_executed(i)

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
Simulated annealing algorithm using simulated annealing for only routing
"""
class HalfAnnealingAlgorithm(AnnealingAlgorithm):
    @staticmethod
    def get_name():
        return "HAA"

    def execute_job(self, job):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]
        hosts = []
        util = JobConfig()

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
Simulated annealing algorithm using simulated annealing for only placement
"""
class HalfAnnealingAlgorithm2(AnnealingAlgorithm):
    @staticmethod
    def get_name():
        return "HAA2"

    def compute_route(self):
        util = JobConfig()
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util

"""
Simulated annealing algorithm with random placement and routing
"""
class RandomAlgorithm(HalfAnnealingAlgorithm):
    @staticmethod
    def get_name():
        return "RR"

    def compute_route(self):
        util = JobConfig()
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util

"""
Simulated annealing algorithm combined with DRF
"""
class AnnealingAlgorithmDRF(AnnealingAlgorithm):
    @staticmethod
    def get_name():
        return "AA-DRF"

    def check_constraint(self, state):
        ret = True

        for host in state:
            if host.get_cpu() < self.cur_demand.get_cpu() or \
                host.get_mem() < self.cur_demand.get_mem():
                ret = False
                break

        return ret

    def _execute_job(self):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]

        util = JobConfig()

        # There are enough nodes to run the job
        if len(available_hosts) > (self.num_mappers + self.num_reducers):
            max_step = self.max_step
            max_util = self.cur_demand.get_net()

            # Executing simulated annealing for map-reduce placement
            simulated_annealing = SimulatedAnnealing(max_util, \
                                                     max_step, \
                                                     self.placement_init_state, \
                                                     self.placement_generate_neighbor, \
                                                     self.placement_compute_util, \
                                                     self.check_constraint)

            util = simulated_annealing.run()

        return util

    def execute_job(self, job, rsrc):
        self.cur_job = job
        self.cur_demand = rsrc

        return self._execute_job()

    def compute_route(self):
        util = JobConfig()

        if self.build_paths():
            max_step = self.max_step
            max_util = self.cur_demand.get_net()

            # Executing simulated annealing for map-reduce routing
            simulated_annealing = SimulatedAnnealing(max_util, \
                                                     max_step, \
                                                     self.routing_init_state, \
                                                     self.routing_generate_neighbor, \
                                                     self.routing_compute_util, \
                                                     self.check_constraint)

            util = simulated_annealing.run()

        # print "util: ", util.get_util()
        return util

"""
Simulated annealing algorithm using simulated annealing for only routing with DRF
"""
class HalfAnnealingAlgorithmDRF(AnnealingAlgorithmDRF):
    @staticmethod
    def get_name():
        return "HAA-DRF"

    def _execute_job(self):
        available_hosts = [h for h in self.graph.get_hosts() if h.is_free()]
        hosts = []
        util = JobConfig()

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
Simulated annealing algorithm using simulated annealing for only placement with DRF
"""
class HalfAnnealingAlgorithm2DRF(AnnealingAlgorithmDRF):
    @staticmethod
    def get_name():
        return "HAA2-DRF"

    def compute_route(self):
        util = JobConfig()
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util

"""
Simulated annealing algorithm with random placement and routing
"""
class RandomAlgorithmDRF(HalfAnnealingAlgorithmDRF):
    @staticmethod
    def get_name():
        return "RR-DRF"

    def compute_route(self):
        util = JobConfig()
        chosen_paths = []

        if self.build_paths():
            for path in self.valid_paths.values():
                chosen_paths.append(choice(path))
            util = self.routing_compute_util(chosen_paths)

        return util
