from flow import Flow
from link import Link
from utils import * # pyflakes_bypass

"""
Represents a graph of nodes currently working in the cluster
"""
class Graph:
    def __init__(self, name, bandwidth, comm_pattern=[]):
        self.name = name
        self.bandwidth = bandwidth
        self.comm_pattern = comm_pattern # tuples of (src, dst, bandwidth desired)
        self.nodes = {}
        self.flows = {}
        self.links = {}
        self.k_path_validity = None # Heuristic for path validation in k-path

    def __del__(self):
        del self.comm_pattern
        del self.links
        del self.flows
        del self.nodes

    def reset(self):
        self.reset_flows()
        self.reset_links()

    def merge_paths(self, used_paths):
        # TODO: Need to rethink if apps have min. bandwidth requirement
        for paths in used_paths:
            path = paths[1]
            for i in range(len(path) - 1):
                l = self.links[Link.get_id(path[i], path[i + 1])]
                l.set_bandwidth(l.get_bandwidth() - self.bandwidth / 10)

    def set_k_path_validity(self, f):
        self.k_path_validity = f

    def set_k_path_heuristic(self, f):
        self.k_path_heuristic = f

    def get_name(self):
        return self.name

    def get_bandwidth(self):
        return self.bandwidth

    def set_links(self, links):
        self.links = links

    def get_link(self, node1, node2):
        ret = None
        link_id = Link.get_id(node1.get_id(), node2.get_id())

        if link_id in self.links:
            ret = self.links[link_id]

        return ret

    def get_links(self):
        return self.links

    # Print the links and the bandwidth consumption of the flows running on it
    def print_links(self):
        for link in self.links.values():
            print "link ", link.get_end_points(), " bandwidth ", link.get_bandwidth()
            flows = link.get_flows()

            for flow in flows:
                print "flow bandwidth: ", link.get_flow_bandwidth(flow)

    def add_link(self, node1, node2, bandwidth):
        link = Link(node1.get_id(), node2.get_id(), bandwidth, bandwidth)
        end_point = link.get_end_points()

        if link in self.links:
            raise Exception("Link already exist...")
        else:
            self.links[end_point] = link
            node1.add_link(link)
            node2.add_link(link)

        self.links[end_point] = link

    def copy_links(self):
        return copy_links(self.links)

    def reset_links(self):
        for link in self.links.values():
            link.reset()

    def set_nodes(self, nodes):
        self.nodes = nodes

    def get_node(self, node_id):
        ret = None
        if node_id in self.nodes:
            ret = self.nodes[node_id]
        return ret

    def get_nodes(self):
        return self.nodes

    def get_hosts(self):
        return filter(lambda node: node.get_type() == "host", self.nodes.values())

    def get_switches(self):
        return filter(lambda node: node.get_type() == "switch", self.nodes.values())

    def get_comm_pattern(self):
        return self.comm_pattern

    def reset_flows(self):
        del self.flows
        self.flows = {}

    def get_flow(self, flow_id):
        return self.flows[flow_id]

    def set_flows(self, flows):
        self.flows = flows

    def get_flows(self):
        return self.flows

    def add_node(self, node):
        node_id = node.get_id()

        if node_id in self.nodes:
            raise Exception("Node already exist...")
        else:
            self.nodes[node_id] = node

    def add_comm_pattern(self, comm_pattern):
        self.comm_pattern.extend(comm_pattern)

    def set_comm_pattern(self, comm_pattern=[]):
        self.comm_pattern = comm_pattern
        self.flows = None

    def set_flow(self, chosen_paths):
        self.flows = {}
        # print "chosen_paths: ", chosen_paths

        # Create Flow Objects
        for cp in self.comm_pattern:
            fl = Flow(cp[0], cp[1], cp[2])
            self.flows[fl.get_end_points()] = fl

        for p in chosen_paths:
            path = p[1]

            link_list = []
            for i in range(len(path)-1):
                l = self.links[Link.get_id(path[i], path[i+1])]
                link_list.append(l)

            fl = self.flows[Flow.get_id(path[0], path[-1])]
            fl.set_path(link_list)

        for link in self.get_links().values():
            link.adjust_flow_bandwidths()

        # for f in self.flows.keys():
        #     bottleneck = self.flows[f].getBottleneckLink()
        #     print "bw: ", bottleneck.getLinkBandwidth()
        #     print "links: ", [f.getEndPoints() for f in bottleneck.getFlows()]
        #     print "Flow%s Effective Bandwidth: %f" % (str(self.flows[f].getEndPoints()), self.flows[f].getEffectiveBandwidth())

    def compute_utilization(self):
        # print "begin compute utilization"
        # for flow in self.flows.keys():
        #     print "effective bw: ", self.flows[flow].get_effective_bandwidth()
        #     print "requested bw: ", self.flows[flow].get_requested_bandwidth()
        # print "end compute utilization"

        util = sum([self.flows[fl].get_effective_bandwidth() + self.flows[fl].get_requested_bandwidth() \
                    for fl in self.flows.keys()])
        return util

    def plot(self, graph_type):
        # pypy doesn't support matplotlib and networkx, :(
        import matplotlib.pyplot as plt
        import networkx as nx

        nx_graph = nx.Graph()

        for node in self.get_nodes().values():
            nx_graph.add_node(node.get_id(), type=node.get_type())

        for node in self.get_nodes().values():
            for link in node.get_links():
                end_point = link.get_end_points()
                neighbor = end_point[0] if node.get_id() != end_point[0] else end_point[1]
                nx_graph.add_edge(node.get_id(), neighbor, weight=100)

        hosts = [(u) for (u, v) in nx_graph.nodes(data = True) if v["type"] == "host"]
        switches = [(u) for (u, v) in nx_graph.nodes(data = True) if v["type"] == "switch"]

        pos = nx.graphviz_layout(nx_graph)

        # draw graph
        nx.draw_networkx_nodes(nx_graph, pos, nodelist=switches, node_size=100, label="x")
        nx.draw_networkx_nodes(nx_graph, pos, nodelist=hosts, node_size=50, node_color='b')
        nx.draw_networkx_edges(nx_graph, pos)

        plt.savefig("graphs/" + graph_type + '.png')
        plt.clf()
