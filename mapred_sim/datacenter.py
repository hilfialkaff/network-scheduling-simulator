from random import shuffle, randrange, sample, seed
from routing import KShortestPathBWAllocation
from pprint import pprint
from copy import deepcopy
from nodes import *
from topology import *
from utils import *
import math
from guppy import hpy

class DataCenter:
    'Class simulating a data center'
    
    def __init__(self, topology):
        self.seed = randrange(100)
        self.topology = topology
        self.graph = topology.getGraph()
        self.tenants = {}
        self.nextTenantIdSeq = 1
        self.allocStrategy = None
        self.h = hpy()
        self.stopwatch = StopWatch()
    
    def __del__(self):
        pass
    
    def generateCommPattern(self, numFlows):
        flows = []
        hosts = self.graph.getHosts()
        bandwidth = self.topology.getBandwidth()
        
        while len(flows) != numFlows:
            ids = [i.nodeId for i in hosts]
            for _ in range(self.seed):
                sample(ids, 2)
            
            pair = sample(ids, 2)
            comm = (pair[0], pair[1], bandwidth / 10) # TODO: vary bandwidth
            
            if comm not in flows:
                flows.append(comm)
        
        # print "\ncomm pattern: ", flows
        self.graph.setCommPattern(flows)
    
    def getGraph(self):
        return self.graph
    
    def computeRoute(self):
        bandwidth = self.topology.getBandwidth()
    
        # for k in [1, 4, float('inf')]:
        for k in [1, 4]:
            graph = self.graph.clone()
            
            self.allocStrategy = KShortestPathBWAllocation(k)
            
            print "\nFinding optimal graph with k=%d" % k
            self.stopwatch.Start()
            self.allocStrategy.compute(graph, bandwidth)
            self.stopwatch.Log('No failure total time')
            p = 0.1
            self.allocStrategy.simulatePercentageLinkFailures(p)
            # print "Recalculating graph..."
            self.stopwatch.Start()
            self.allocStrategy.recoverBrokenFlows()
            self.stopwatch.Log('Recovering %d%% failure total time' % (p*100))
    
    def cleanUp(self):
        del self.graph
        self.allocStrategy.cleanUp()
    
    def displayTopology(self):
        self.topology.showGraph()
    
    def addTenant(self, minVM = 2, maxVM = 5):
        assert((minVM, maxVM) >= 0)
        tenantId = self.__getNextTenantID()
        vmCount = int(randrange(minVM, maxVM))
        vmSet = set()
        
        for i in range(vmCount):
            vmSet.add(VirtualMachine(tenantId, i))
        
        self.tenants[tenantId] = vmSet
        
        self.determinePlacement(vmSet)
        return tenantId
    
    def __getNextTenantID(self):
        tenantId = self.nextTenantIdSeq
        self.nextTenantIdSeq += 1
        return "t%i" % tenantId
    
    def determinePlacement(self, vmSet):
        '''
        VM Placement by first-fit heuristic
        Assumptions made:
        - The resouces (CPU, Network, etc) of PM is represented by an integer value
        - The requirements of VM is represented by an integer value
        '''
        failedAssignments = []  # just in case
        inactivePMs = []
        activePMs = []
        hosts = self.graph.getHosts()
        
        # classify Physical Machines by capacity
        for pm in hosts:
            if (pm.getResourcesLeft() == 0):
                pass
            elif (len(pm.getAssignedVMs()) == 0):
                inactivePMs.append(pm)
            else:
                activePMs.append(pm)
        
        shuffle(inactivePMs)
        
        for vm in vmSet:
            isAssigned = False
            
            # Try the active PMs first
            for pm in activePMs:
                if (pm.assignVM(vm)):
                    isAssigned = True
                    break
            
            if (isAssigned == True):
                continue
            
            for pm in inactivePMs:
                if (pm.assignVM(vm)):
                    isAssigned = True
                    inactivePMs.remove(pm)
                    activePMs.append(pm)
                    break
            
            if (isAssigned == False):
                failedAssignments.append(vm)
        
        if (len(failedAssignments) > 0):
            print "Failed to assign %i virtual machines" % len(failedAssignments)
    
    def listVMs(self, tenantId = '', hostId = ''):
        if (tenantId != ''):
            self.listVMsByTenant(tenantId)
        elif (hostId != ''):
            # TODO Unimplemented yet
            pass
        else:
            for tId in self.tenants.keys():
                self.listVMsByTenant(tId)
    
    def listVMsByTenant(self, tenantId):
        assert(tenantId != '')
        if (self.tenants.has_key(tenantId)):
            print "Tenant %s has %i VMs:" % (tenantId, len(self.tenants[tenantId]))
            for vm in self.tenants[tenantId]:
                print "  %s" % vm.toString()
                # hostPM = vm.getHostingPM()
                
                # if (hostPM is None):
                #   print "  VM %s is not being hosted" % vm.getId()
                # else:
                #   print "  VM %s is hosted at PM %s" % (vm.getId(), hostPM.getId())
        else:
            print "Tenant %s not found" % tenantId
