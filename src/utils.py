from heapq import heappush, heappop
from link import Link

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

  def is_empty(self):
    return len(self.heap) == 0

def copy_links(links):
    new_links = {}

    for link in links.values():
        bandwidth = link.get_bandwidth()
        orig_bandwidth = link.get_orig_bandwidth()
        [node1_id, node2_id] = link.get_end_points()
        new_link = Link(node1_id, node2_id, bandwidth, orig_bandwidth)

        new_links[(node1_id, node2_id)] = new_link

    return new_links
