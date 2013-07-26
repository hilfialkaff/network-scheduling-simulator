from time import time
from heapq import heappush, heappop

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
