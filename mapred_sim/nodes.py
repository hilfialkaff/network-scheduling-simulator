from random import randrange, choice, seed

class PhysicalMachine:
  '''
  Physical Machine
  '''
  MIN_RESOURCES = 20
  MAX_RESOURCES = 25

  def __init__(self, nodeId, resources = 0):
    self.nodeId = nodeId
    self.assignedVMs = set()

    if (resources > 0):
      self.resources = resources
    else:
      self.resources = int(randrange(PhysicalMachine.MIN_RESOURCES, PhysicalMachine.MAX_RESOURCES))

    self.resourcesLeft = self.resources

  def __del__(self):
    pass

  def assignVM(self, vm):
    assert(vm is not None)

    vmLoad = vm.getLoad()
    if (self.resourcesLeft < vmLoad):
      # print "Error assigning VM %s (%i) to PM %s (%i)" % (vm.getId(), vm.getLoad(), self.getId(), self.getResourcesLeft())
      return False

    # print "Assigning VM %s (%i) to PM %s (%i)" % (vm.getId(), vm.getLoad(), self.getId(), self.getResourcesLeft())
    vm.setHostingPM(self)
    self.assignedVMs.add(vm)
    self.resourcesLeft -= vmLoad
    return True

  def getId(self):
    return self.nodeId

  def getResourcesLeft(self):
    return self.resourcesLeft

  def getAssignedVMs(self):
    return self.assignedVMs

  def toString(self):
    pass

class VirtualMachine:
  '''
  Virtual Machine class
  '''
  MIN_LOAD = 5
  MAX_LOAD = 15

  def __init__(self, tenantId, nodeId, load = 0):
    self.nodeId = "%svm%i" % (tenantId, nodeId)
    self.tenantId = tenantId

    if (load > 0):
      self.load = load
    else:
      self.load = int(randrange(VirtualMachine.MIN_LOAD, VirtualMachine.MAX_LOAD))

  def __del__(self):
    pass

  def getId(self):
    return self.nodeId

  def getTenantId(self):
    return self.tenantId

  def getLoad(self):
    return self.load

  def getHostingPM(self):
    return self.hostingPM

  def setHostingPM(self, pm):
    self.hostingPM = pm

  def toString(self):
    return "%s (load=%i,host=%s)" % (self.nodeId, self.load, self.hostingPM.getId())
