import flow

class Link:
  'A link between switches/hosts'

  def __init__(self, n1, n2, bw = 100, isFullDuplex = True):
    self.endPoints = Link.getLinkId(n1, n2, isFullDuplex)
    self.bandwidth = float(bw)
    self.flows = {}
    self.state = True

  @staticmethod
  def getLinkId(n1, n2, isFullDuplex = True):
    return tuple(sorted([n1, n2]) if isFullDuplex else [n1, n2])

  def setState(self, state):
    if self.state == state:
      return

    self.state = state

    for f in self.getFlows():
      f.linkStateChanged(self)


  def turnOff(self):
    self.setState(False)

  def turnOn(self):
    self.setState(True)

  def isActive(self):
    return self.state

  def containsFlow(self, fl):
    return fl in self.flows

  def getEndPoints(self):
    return self.endPoints

  def getFlowAllocation(self):
    return self.flows

  def getFlows(self):
    return self.flows.keys()

  def getUtilization(self):
    bw_used = 0.0

    for fl in self.flows.keys():
      bw_used += fl.getEffectiveBandwidth()

    return bw_used / self.bandwidth

  def removeFlow(self, fl):
    if self.containsFlow(fl):
      del self.flows[fl]
      self.adjustFlowBandwidths()

  def addFlow(self, fl):
    if not self.containsFlow(fl):
      self.flows[fl] = 0
      self.adjustFlowBandwidths()

  def getLinkBandwidth(self):
    return self.bandwidth

  def getBandwidthForFlow(self, fl):
    if self.containsFlow(fl):
      return self.flows[fl]
    else:
      raise Exception('Flow does not exist')

  def adjustFlowBandwidths(self):
    self.__computeMaxMinFairness()

  def __computeMaxMinFairness(self):
    flowList = self.flows.keys()

    if (len(flowList) == 0):
      return

    flowList.sort(key = lambda x: x.bandwidth, reverse = True)

    allocation = {}
    for fl in flowList:
      allocation[fl] = 0

    bw_spare = self.bandwidth
    while (bw_spare > 0):
      deleteList = []

      # Distribute spare bandwidth among all flows
      if (len(flowList) == 0):
        bw_portion = bw_spare / len(allocation.keys())
        bw_spare = 0

        for fl in allocation.keys():
          allocation[fl] = allocation[fl] + bw_portion

      else:
        bw_portion = bw_spare / len(flowList)
        bw_spare = 0

        for fl in flowList:
          allocation[fl] = allocation[fl] + bw_portion

          if (allocation[fl] > fl.getRequestedBandwidth()):
            bw_spare = bw_spare + (allocation[fl] - fl.getRequestedBandwidth())
            allocation[fl] = fl.getRequestedBandwidth()
            deleteList.append(fl)

        for fl in deleteList:
          flowList.remove(fl)

    self.flows = allocation
    # print self.flows
