from flow import *
from link import *
from copy import deepcopy
from random import choice, sample
from nodes import *

class Graph(object):
    def __init__(self, g={}, commPattern=[]):
        self.g = g
        self.commPattern = commPattern # tuples of (src, dst, bandwidth desired)
        self.flows = None
        self.links = None
        self.hosts = None
        self.switches = None

        self.build()

    def __del__(self):
        del self.g
        del self.links
        del self.flows

    def clone(self):
        return Graph(deepcopy(self.g), deepcopy(self.commPattern))

    def build(self):
        hosts = []
        switches = []

        for nodeId in self.g:
          if nodeId.startswith("h"):
            hosts.append(PhysicalMachine(nodeId))
          elif nodeId.startswith("s"):
            switches.append(nodeId)

        self.hosts = hosts
        self.switches = switches
        self.setLink()


    def setLink(self):
        self.links = {}
        for n1 in self.g.keys():
            for n2 in self.g[n1]:
                if not Link.getLinkId(n1, n2) in self.links.keys():
                    l = Link(n1, n2, self.g[n1][n2])
                    self.links[l.getEndPoints()] = l

    def getFlatGraph(self):
        return self.g

    def setFlatGraph(self, g={}):
        self.__init__(g, self.commPattern)

    def getLinks(self):
        return self.links

    def getHosts(self):
        return self.hosts

    def getSwitches(self):
        return self.switches

    def setCommPattern(self, commPattern=[]):
        self.commPattern = commPattern
        self.flows = None

    def getCommPattern(self):
        return self.commPattern

    def getFlows(self):
        return self.flows

    def addFlow(self, src, dst, bw, path):
        fl = Flow(src, dst, bw)

        linkList = []
        for i in range(len(path)-1):
            l = self.links[Link.getLinkId(path[i], path[i+1])]
            linkList.append(l)

        fl.setPath(linkList)
        self.flows[fl.getEndPoints()] = fl
        return fl

    def removeFlow(self, flow):
        flow.setPath(None)
        self.flows.pop(flow.getEndPoints())

    def setFlow(self, chosen_paths):
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

        # for f in flows.keys():
        #     bottleneck = flows[f].getBottleneckLink()
        #     print "bw: ", bottleneck.getLinkBandwidth()
        #     print "links: ", [f.getEndPoints() for f in bottleneck.getFlows()]

            # print "Flow%s Effective Bandwidth: %f" % (str(flows[f].getEndPoints()), flows[f].getEffectiveBandwidth())

    def computeUtilization(self):
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

    def getEdgeLinkForHost(self, hostId):
        adjLinkIds = self.g[hostId].keys()

        # No edge switches found for this host
        if len(adjLinkIds) == 0:
            return None

        switchId = adjLinkIds[0]
        return self.links[Link.getLinkId(hostId, switchId)]

    def getFlowAllocation(self, link):
        return self.links[link].getFlowAllocation()
