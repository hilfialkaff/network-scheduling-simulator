from time import time
from heapq import heappush, heappop

class StopWatch:
	def __init__(self):
		self.startTime = None
		self.stopTime = None

	def Start(self):
		self.startTime = time()
		self.stopTime = None

	def Stop(self):
		if self.startTime is None:
			return

		self.stopTime = time()

	def Log(self, label='Elapsed Time'):
		stop = time() if self.stopTime is None else self.stopTime
		d = stop - self.startTime
		print '%s: %d ms' % (label, d * 1000)


class PriorityQueue:
  def  __init__(self):
    self.heap = []

  def push(self, priority, *args):
      pair = [priority, args]
      heappush(self.heap, pair)

  def pop(self):
      priority, args = heappop(self.heap)
      pair = [priority]
      pair.extend([a for a in args])
      return pair

  def isEmpty(self):
    return len(self.heap) == 0
