from flow import Flow
from link import Link
from node import Node
from copy import deepcopy
from random import choice, sample
import matplotlib.pyplot as plt
import networkx as nx

class Graph(object):
    def __init__(self, g={}, commPattern=[]):
        self.g = g
        self.commPattern = commPattern # tuples of (src, dst, bandwidth desired)
        self.flows = None
        self.links = None
        self.hosts = None
        self.switches = None
        self.num_mappers = 1
        self.num_reducers = 1

        self.build()

    def __del__(self):
        del self.g
        del self.links
        del self.flows

    # TODO: Make this heterogeneous?
    def set_mappers_reducers(self, num_mappers, num_reducers):
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def clone(self):
        return Graph(deepcopy(self.g), deepcopy(self.commPattern))

    def build(self):
        hosts = []
        switches = []

        for nodeId in self.g:
          if nodeId.startswith("h"):
            hosts.append(Node(nodeId, self.num_mappers, self.num_reducers))
          elif nodeId.startswith("s"):
            switches.append(nodeId)

        self.hosts = hosts
        self.switches = switches
        self.set_link()

    def get_total_mappers(self):
        return sum([h.get_available_mappers() for h in hosts])

    def get_total_reducers(self):
        return sum([h.get_available_reducers() for h in hosts])

    def set_link(self):
        self.links = {}
        for n1 in self.g.keys():
            for n2 in self.g[n1]:
                if not Link.getLinkId(n1, n2) in self.links.keys():
                    l = Link(n1, n2, self.g[n1][n2])
                    self.links[l.getEndPoints()] = l

    def get_flat_graph(self):
        return self.g

    def get_links(self):
        return self.links

    def get_hosts(self):
        return self.hosts

    def get_switches(self):
        return self.switches

    def set_comm_pattern(self, commPattern=[]):
        self.commPattern = commPattern
        self.flows = None

    def get_comm_pattern(self):
        return self.commPattern

    def get_flows(self):
        return self.flows

    def add_flow(self, src, dst, bw, path):
        fl = Flow(src, dst, bw)

        linkList = []
        for i in range(len(path)-1):
            l = self.links[Link.getLinkId(path[i], path[i+1])]
            linkList.append(l)

        fl.setPath(linkList)
        self.flows[fl.getEndPoints()] = fl
        return fl

    def remove_flow(self, flow):
        flow.setPath(None)
        self.flows.pop(flow.getEndPoints())

    def set_flow(self, chosen_paths):
        self.flows = {}

        # Create Flow Objects
        for cp in self.commPattern:
            fl = Flow(cp[0], cp[1], cp[2])
            self.flows[fl.getEndPoints()] = fl

        for p in chosen_paths:
            path = p[1]

            linkList = []
            for i in range(len(path)-1):
                l = self.links[Link.getLinkId(path[i], path[i+1])]
                linkList.append(l)

            fl = self.flows[Flow.getFlowId(path[0], path[-1])]
            fl.setPath(linkList)

        # for f in self.flows.keys():
        #     bottleneck = self.flows[f].getBottleneckLink()
        #     print "bw: ", bottleneck.getLinkBandwidth()
        #     print "links: ", [f.getEndPoints() for f in bottleneck.getFlows()]
        #     print "Flow%s Effective Bandwidth: %f" % (str(self.flows[f].getEndPoints()), self.flows[f].getEffectiveBandwidth())

    def compute_utilization(self):
        util = sum([self.flows[fl].getEffectiveBandwidth() + self.flows[fl].getRequestedBandwidth() for fl in self.flows.keys()])
        return util

    def takeRandomLinkDown(self):
        if len(self.links) == 0:
            return None

        linkId = None

        while True:
            linkId = sample(self.links, 1)[0]
            link = self.links[linkId]

            if link.isActive():
                link.turnOff()

                del self.g[linkId[0]][linkId[1]]
                del self.g[linkId[1]][linkId[0]]
                break

        return linkId

    def get_edge_link_for_host(self, hostId):
        adjLinkIds = self.g[hostId].keys()

        # No edge switches found for this host
        if len(adjLinkIds) == 0:
            return None

        switchId = adjLinkIds[0]
        return self.links[Link.getLinkId(hostId, switchId)]

    def get_flow_allocation(self, link):
        return self.links[link].getFlowAllocation()

    def plot(self, graph_type):
        G = self.generate_graph()
        hosts = [(u) for (u, v) in G.nodes(data = True) if v["type"] == "host"]
        switches = [(u) for (u, v) in G.nodes(data = True) if v["type"] == "switch"]

        pos = nx.graphviz_layout(G)

        # draw graph
        nx.draw_networkx_nodes(G, pos, nodelist=switches, node_size=100, label="x")
        nx.draw_networkx_nodes(G, pos, nodelist=hosts, node_size=50, node_color='b')
        nx.draw_networkx_edges(G, pos)

        plt.savefig("graphs/" + graph_type + '.png')
        plt.clf()

    def generate_graph(self):
        graph = self.g
        nx_graph = nx.Graph()

        for node in graph:
            if node.startswith("h"):
                type = "host"
            else:
                type = "switch"
            nx_graph.add_node(node, type = type)

        for node in graph:
            for adjNode in graph[node]:
                weight = graph[node][adjNode]
                nx_graph.add_edge(node, adjNode, weight = weight)

        return nx_graph
