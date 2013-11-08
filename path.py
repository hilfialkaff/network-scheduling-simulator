from time import clock
from utils import * # pyflakes_bypass

class Path(object):
    def __init__(self, graph, k, num_alt_paths):
        self.graph = graph
        self.k = k
        self.num_alt_paths = num_alt_paths
        self.bandwidth = graph.get_bandwidth()

class KPath(Path):
    def __init__(self, graph, k, num_alt_paths):
        super(KPath, self).__init__(graph, k, num_alt_paths)
        self.k_paths = {}

        self.build_k_paths()

    def build_k_paths(self):
        start = clock()
        hosts_id = [node.get_id() for node in self.graph.get_hosts()]

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
                    self.k_paths[src][dst] = self.k_path(src, dst, self.bandwidth/10) # TODO

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

    def get_k_paths(self):
        return self.k_paths

"""
Modified floyd-warshall algorithm to find top k-paths instead of only the shortest path
"""
class FWPath(Path):
    def __init__(self, graph, k, num_alt_paths):
        super(FWPath, self).__init__(graph, k, num_alt_paths)
        self.distances = {}
        self.next_nodes = {}

        self.build_fw()

    def build_fw(self):
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

    def get_distances(self):
        return self.distances

    def get_next_nodes(self):
        return self.next_nodes
