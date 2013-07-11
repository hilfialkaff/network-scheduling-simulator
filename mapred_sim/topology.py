#from drawgraph import drawGraph
from random import randrange, choice, seed
from pprint import pprint
from graph import Graph
import math

class Topology(object):
    def __init__(self):
        self.graph = None
        self.bandwidth = None
    
    def generateGraph(self):
        raise NotImplementedError('Method is unimplemented in abstract class!')
    
    def getGraph(self):
        if self.graph is None:
            self.graph = self.generateGraph()
    
        return self.graph
    
    def getBandwidth(self):
        return self.bandwidth
    
    def writeToFile(self):
        # TODO: Implement this
        pass
    
    def showGraph(self):
        raise NotImplementedError('Method no longer supported!')
        #assert(self.graph is not None)
        #drawGraph(self.graph, self.graph_type, self.graph_args)

class JellyfishTopology(Topology):
    def __init__(self, bandwidth = 100, nHosts = 16, nSwitches = 20, nPorts = 4):
        super(JellyfishTopology, self).__init__()
        self.bandwidth = bandwidth
        self.nHosts = nHosts
        self.nSwitches = nSwitches
        self.nPorts = nPorts
    
    def generateGraph(self):
        ''' Generate a Jellyfish topology-like graph
        @param nHosts number of hosts
        @param nSwitches number of switches
        @param nPorts number of ports in each switch
        @param s seed to be used for RNG
        @return dictionary of nodes and edges representing the topology
        '''
        seed(1) # for Random Number Generation
        hosts = []
        switches = []
        openPorts = []
        
        bandwidth = self.bandwidth
        nHosts = self.nHosts
        nSwitches = self.nSwitches
        nPorts = self.nPorts
        
        assert(nSwitches >= nHosts)
        assert(nPorts > 1)
        
        graph = {}
        
        # add hosts
        for i in range(1, nHosts + 1):
            nodeName = "h%s" % i
            hosts.append(nodeName)
            if not graph.has_key(nodeName):
                graph[nodeName] = {}
        
        # add switches
        for i in range(1, nSwitches + 1):
            nodeName = "s%s" % i
            switches.append(nodeName)
            openPorts.append(nPorts)
            if not graph.has_key(nodeName):
                graph[nodeName] = {}
        
        # connect each server with a switch
        for i in range(nHosts):
            graph[hosts[i]][switches[i]] = bandwidth
            graph[switches[i]][hosts[i]] = bandwidth
            openPorts[i] -= 1
        
        links = set()
        switchesLeft = nSwitches
        consecFails = 0
        
        while switchesLeft > 1 and consecFails < 10:
            s1 = randrange(nSwitches)
            while openPorts[s1] == 0:
                s1 = randrange(nSwitches)
        
            s2 = randrange(nSwitches)
            while openPorts[s2] == 0 or s1 == s2:
                s2 = randrange(nSwitches)
        
            if (s1, s2) in links:
                consecFails += 1
            else:
                consecFails = 0
                links.add((s1, s2))
                links.add((s2, s1))
                
                openPorts[s1] -= 1
                openPorts[s2] -= 1
                
                if openPorts[s1] == 0:
                    switchesLeft -= 1
                
                if openPorts[s2] == 0:
                    switchesLeft -= 1
        
        if switchesLeft > 0:
            for i in range(nSwitches):
                while openPorts[i] > 1:
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

                        openPorts[i] -= 2
                        break

        for link in links:
            # prevent double counting
            if link[0] < link[1]:
                graph[switches[link[0]]][switches[link[1]]] = bandwidth
                graph[switches[link[1]]][switches[link[0]]] = bandwidth

        return Graph(graph)

class FatTreeTopology(Topology):
    def __init__(self, bandwidth = 100, nPorts = 4):
        super(FatTreeTopology, self).__init__()
        self.bandwidth = 100
        self.nPorts = nPorts

    def generateGraph(self):
        ''' Generate a Fat Tree topology-like graph
        @param nPorts number of ports in each switch
        @return dictionary of nodes and edges representing the topology
        '''
        hosts = []
        switches = []

        bandwidth = self.bandwidth
        nPorts = self.nPorts

        pods = range(0, nPorts)
        core_sws = range(1, nPorts/2 + 1)
        agg_sws = range(nPorts/2, nPorts)
        edge_sws = range(0, nPorts/2)
        hosts = range(2, nPorts/2 + 2)

        graph = {}

        for p in pods:
            for e in edge_sws:
                edge_id = "se_%i_%i_%i" % (p, e, 1)
                if not graph.has_key(edge_id):
                    graph[edge_id] = {}

                for h in hosts:
                    host_id = "h_%i_%i_%i" % (p, e, h)
                    if not graph.has_key(host_id):
                        graph[host_id] = {}

                    graph[host_id][edge_id] = bandwidth
                    graph[edge_id][host_id] = bandwidth

                for a in agg_sws:
                    agg_id = "sa_%i_%i_%i" %(p, a, 1)
                    if not graph.has_key(agg_id):
                        graph[agg_id] = {}

                    graph[agg_id][edge_id] = bandwidth
                    graph[edge_id][agg_id] = bandwidth

            for a in agg_sws:
                agg_id = "sa_%i_%i_%i" %(p, a, 1)
                c_index = a - nPorts / 2 + 1
                for c in core_sws:
                    core_id = "sc_%i_%i_%i" % (nPorts, c_index, c)

                    if not graph.has_key(core_id):
                        graph[core_id] = {}

                    graph[core_id][agg_id] = bandwidth
                    graph[agg_id][core_id] = bandwidth
        
        return Graph(graph)
