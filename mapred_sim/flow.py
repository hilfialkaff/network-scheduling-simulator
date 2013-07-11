import link

class Flow:
  'A flow class'

  def __init__(self, h1, h2, bw = 100):
    self.endPoints = Flow.getFlowId(h1, h2)
    self.bandwidth = float(bw)
    self.path = []
    self.state = True

  @staticmethod
  def getFlowId(h1, h2):
    return tuple(sorted([h1, h2]))

  def getEndPoints(self):
    return self.endPoints

  def getPath(self):
    return self.path


  def isActive(self):
    return self.state

  def linkStateChanged(self, link):
    if link in self.path:
      if not link.isActive():
        self.state = False
      else:
        self.state = True
        for l in self.path:
          if not l.isActive():
            self.state = False
            break

  def removeCurrentPath(self):
    if self.path == None:
      return

    for l in self.path:
      l.removeFlow(self)

    self.path = None
    self.state = False


  def setPath(self, newPath):
    self.removeCurrentPath()
    self.path = newPath

    if newPath is None:
      self.state = False
      return

    self.state = True

    for l in newPath:
      l.addFlow(self)

      if not l.isActive():
        self.state = False

  def isLinkInPath(link):
    return link in self.path

  def getEffectiveBandwidth(self):
    if len(self.path) == 0:
      raise Exception('No path assigned for this flow')

    minValue = float('inf')
    for link in self.path:
      minValue = min(minValue, link.getBandwidthForFlow(self))

    return minValue


  def getBottleneckLink(self):
    if len(self.path) == 0:
      return

    minBW = float("inf")
    slowLink = None
    for link in self.path:
      if link.getLinkBandwidth() < minBW:
        minBW = link.getLinkBandwidth()
        slowLink = link

    return slowLink

  def getRequestedBandwidth(self):
    return self.bandwidth
