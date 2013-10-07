from random import randrange, choice, seed, random
from graph import Graph
from node import Node

class Topology(object):
    def __init__(self):
        self.bandwidth = 0

    def generate_graph(self):
        raise NotImplementedError('Method is unimplemented in abstract class!')

    def k_path_validity(self, path):
        return True

    def k_path_heuristic(self, path):
        return 0

    def get_bandwidth(self):
        return self.bandwidth

    @staticmethod
    def get_name():
        raise NotImplementedError("Method unimplemented in abstract class...")
class JellyfishTopology(Topology):
    def __init__(self, bandwidth, num_hosts, num_switches, num_ports):
        super(JellyfishTopology, self).__init__()
        self.bandwidth = bandwidth
        self.num_hosts = num_hosts
        self.num_switches = num_switches
        self.num_ports = num_ports

    @staticmethod
    def get_name():
        return "JF"

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

        graph = Graph(bandwidth)

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

        graph.set_k_path_heuristic(self.k_path_heuristic)
        graph.set_k_path_validity(self.k_path_validity)
        return graph

class Jellyfish2Topology(Topology):
    def __init__(self, bandwidth, num_hosts, num_switches, num_ports):
        super(Jellyfish2Topology, self).__init__()
        self.bandwidth = bandwidth
        self.num_hosts = num_hosts
        self.num_switches = num_switches
        self.num_ports = num_ports

    @staticmethod
    def get_name():
        return "JF2"

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

        graph = Graph(bandwidth)

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

        # Modified jellyfish: add "supernodes"
        for i in range(num_hosts):
            add_switch = 0
            added_switch = 0

            if random() > 0.75:
                add_switch = 0
            elif random() > 0.50:
                add_switch = 1
            elif random() > 0.25:
                add_switch = 2
            else:
                add_switch = 3

            while added_switch != add_switch:
                switch_id = randrange(num_switches)

                if graph.get_link(hosts[i], switches[switch_id]):
                    continue

                graph.add_link(hosts[i], switches[switch_id], bandwidth)
                open_ports[switch_id] -= 1
                added_switch += 1

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

        graph.set_k_path_heuristic(self.k_path_heuristic)
        graph.set_k_path_validity(self.k_path_validity)
        return graph

class FatTreeTopology(Topology):
    def __init__(self, bandwidth, num_ports):
        super(FatTreeTopology, self).__init__()
        self.bandwidth = bandwidth
        self.num_ports = num_ports

    @staticmethod
    def get_name():
        return "FT"

    def k_path_validity(self, path):
        se_count = 0
        sa_count = 0
        sc_count = 0
        h_count = 0
        ret = True

        # print "path: ", path
        for p in path:
            if 'se' in p:
                se_count += 1
            if 'sa' in p:
                sa_count += 1
            if 'sc' in p:
                sc_count += 1
            if 'h' in p:
                h_count += 1

            if h_count > 2:
                ret = False
                break

            if sc_count == 0 and (sa_count > 1 or se_count > 1):
                ret = False
                break

        # print "count: ", se_count, sa_count, sc_count, h_count
        return ret

    def k_path_heuristic(self, path):
        return 0
        se_count = 0
        sa_count = 0
        heuristic = 0

        for p in path:
            if 'se' in p:
                se_count += 1
            if 'sa' in p:
                sa_count += 1

        if 'se' > 2:
            heuristic += se_count - 2
        if 'sa' > 2:
            heuristic += sa_count - 2

        return heuristic

    def generate_graph(self):
        ''' Generate a Fat Tree topology-like graph
        @param num_ports number of ports in each switch
        @return dictionary of nodes and edges representing the topology
        '''
        hosts = []

        bandwidth = self.bandwidth
        num_ports = self.num_ports

        pods = range(0, num_ports)
        core_sws = range(1, num_ports/2 + 1)
        agg_sws = range(num_ports/2, num_ports)
        edge_sws = range(0, num_ports/2)
        hosts = range(2, num_ports/2 + 2)

        graph = Graph(bandwidth)

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

        graph.set_k_path_heuristic(self.k_path_heuristic)
        graph.set_k_path_validity(self.k_path_validity)
        return graph
