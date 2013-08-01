import link

"""
Represents a flow in the datacenter network
"""
class Flow:
    def __init__(self, node1, node2, bw = 100):
        self.end_points = Flow.get_id(node1, node2)
        self.path = []
        self.bandwidth = float(bw)

    @staticmethod
    def get_id(node1, node2):
        return tuple(sorted([node1, node2]))

    def get_end_points(self):
        return self.end_points

    def get_path(self):
        return self.path

    def get_requested_bandwidth(self):
        return self.bandwidth

    def get_effective_bandwidth(self):
        if len(self.path) == 0:
            raise Exception('No path assigned for this flow')

        min_bandwidth = float('inf')
        for link in self.path:
            min_bandwidth = min(min_bandwidth, link.get_flow_bandwidth(self))

        return min_bandwidth

    def set_path(self, path):
        self.path = path

        for l in path:
            l.add_flow(self)

    def get_bottleneck_link(self):
        if len(self.path) == 0:
            return

        min_bandwidth = float("inf")
        bottleneck = None
        for link in self.path:
            link_bandwidth = link.get_bandwidth()
            if link_bandwidth < min_bandwidth:
                min_bandwidth = link_bandwidth
                bottleneck = link

        return bottleneck
