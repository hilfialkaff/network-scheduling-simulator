from random import randrange, choice, seed
from pprint import pprint
from graph import Graph
from node import Node
import math

class Topology(object):
    def __init__(self):
        self.graph = None
        self.bandwidth = 0
    
    def generate_graph(self):
        raise NotImplementedError('Method is unimplemented in abstract class!')
   
    def get_bandwidth(self):
        return self.bandwidth
    
class JellyfishTopology(Topology):
    def __init__(self, bandwidth=100, num_hosts=16, num_switches=20, num_ports=4):
        super(JellyfishTopology, self).__init__()
        self.bandwidth = bandwidth
        self.num_hosts = num_hosts
        self.num_switches = num_switches
        self.num_ports = num_ports
    
    def generate_graph(self):
        ''' Generate a Jellyfish topology-like graph
        @param num_hosts number of hosts
        @param num_switches number of switches
        @param num_ports number of ports in each switch
        @param s seed to be used for RNG
        @return dictionary of nodes and edges representing the topology
        '''
        seed(1) # for Random Number Generation
        hosts = []
        switches = []
        open_ports = []
        
        bandwidth = self.bandwidth
        num_hosts = self.num_hosts
        num_switches = self.num_switches
        num_ports = self.num_ports
        
        assert(num_switches >= num_hosts)
        assert(num_ports > 1)
        
        graph = Graph()
        
        # add hosts
        for i in range(1, num_hosts + 1):
            node_id = "h%s" % i

            new_host = Node(node_id)
            hosts.append(new_host)
            graph.add_node(new_host)
        
        # add switches
        for i in range(1, num_switches + 1):
            node_id = "s%s" % i
            new_switch = Node(node_id)

            open_ports.append(num_ports)
            new_switch = Node(node_id)
            switches.append(new_switch)
            graph.add_node(new_switch)

        # connect each server with a switch
        for i in range(num_hosts):
            graph.add_link(hosts[i], switches[i], bandwidth)
            open_ports[i] -= 1

        links = set()
        switches_left = num_switches
        consec_fails = 0

        while switches_left > 1 and consec_fails < 10:
            s1 = randrange(num_switches)
            while open_ports[s1] == 0:
                s1 = randrange(num_switches)

            s2 = randrange(num_switches)
            while open_ports[s2] == 0 or s1 == s2:
                s2 = randrange(num_switches)
        
            if (s1, s2) in links:
                consec_fails += 1
            else:
                consec_fails = 0
                links.add((s1, s2))
                links.add((s2, s1))
                
                open_ports[s1] -= 1
                open_ports[s2] -= 1
                
                if open_ports[s1] == 0:
                    switches_left -= 1
                
                if open_ports[s2] == 0:
                    switches_left -= 1
        
        if switches_left > 0:
            for i in range(num_switches):
                while open_ports[i] > 1:
                    while True:
                        rLink = choice(list(links))
                        if (i, rLink[0]) in links:
                          continue
                        if (i, rLink[1]) in links:
                          continue

                        #remove links
                        links.remove(rLink)
                        links.remove(rLink[::-1])

                        # add new links
                        links.add((i, rLink[0]))
                        links.add((rLink[0], i))
                        links.add((i, rLink[1]))
                        links.add((rLink[1], i))

                        open_ports[i] -= 2
                        break

        for link in links:
            # prevent double counting
            if link[0] < link[1]:
                graph.add_link(switches[link[0]], switches[link[1]], bandwidth)

        return graph

class FatTreeTopology(Topology):
    def __init__(self, bandwidth=100, num_ports=4):
        super(FatTreeTopology, self).__init__()
        self.bandwidth = bandwidth
        self.num_ports = num_ports

    def generate_graph(self):
        ''' Generate a Fat Tree topology-like graph
        @param num_ports number of ports in each switch
        @return dictionary of nodes and edges representing the topology
        '''
        hosts = []
        switches = []

        bandwidth = self.bandwidth
        num_ports = self.num_ports

        pods = range(0, num_ports)
        core_sws = range(1, num_ports/2 + 1)
        agg_sws = range(num_ports/2, num_ports)
        edge_sws = range(0, num_ports/2)
        hosts = range(2, num_ports/2 + 2)

        graph = Graph()

        for p in pods:
            for e in edge_sws:
                edge_id = "se_%i_%i_%i" % (p, e, 1)
                edge_switch = graph.get_node(edge_id)

                if not edge_switch:
                    edge_switch = Node(edge_id)
                    graph.add_node(edge_switch)

                for h in hosts:
                    host_id = "h_%i_%i_%i" % (p, e, h)
                    host = graph.get_node(host_id) 

                    if not host:
                        host = Node(host_id)
                        graph.add_node(host)
                    graph.add_link(edge_switch, host, bandwidth)

                for a in agg_sws:
                    agg_id = "sa_%i_%i_%i" %(p, a, 1)
                    agg_switch = graph.get_node(agg_id)

                    if not agg_switch:
                        agg_switch = Node(agg_id)
                        graph.add_node(agg_switch)
                    graph.add_link(agg_switch, edge_switch, bandwidth)

            for a in agg_sws:
                agg_id = "sa_%i_%i_%i" %(p, a, 1)
                agg_switch = graph.get_node(agg_id)
                c_index = a - num_ports / 2 + 1
                for c in core_sws:
                    core_id = "sc_%i_%i_%i" % (num_ports, c_index, c)
                    core_switch = graph.get_node(core_id)

                    if not core_switch:
                        core_switch = Node(core_id)
                        graph.add_node(core_switch)
                    graph.add_link(core_switch, agg_switch, bandwidth)
        
        return graph
