"""
A link between switches/hosts
"""
class Link:
    def __init__(self, node1_id, node2_id, bandwidth, orig_bandwidth=0):
        self.end_points = Link.get_id(node1_id, node2_id)
        self.bandwidth = bandwidth
        if not orig_bandwidth:
            self.orig_bandwidth = bandwidth
        else:
            self.orig_bandwidth = orig_bandwidth
        self.flows = {}

    @staticmethod
    def get_id(node1, node2):
        return tuple(sorted([node1, node2]))

    def has_flow(self, fl):
        return fl in self.flows

    def get_end_points(self):
        return self.end_points

    def get_flow_allocation(self):
        return self.flows

    def set_flows(self, flows):
        self.flows = flows

    def get_flows(self):
        return self.flows.keys()

    def remove_flow(self, fl):
        if self.has_flow(fl):
            del self.flows[fl]
            self.adjust_flow_bandwidths()
        else:
            raise Exception("Flow does not exist...")
            exit(1)

    def add_flow(self, fl):
        if self.has_flow(fl):
            raise Exception("Flow already exists...")
            exit(1)
        else:
            self.flows[fl] = 0

    def set_bandwidth(self, bandwidth):
        self.bandwidth = bandwidth

    def get_bandwidth(self):
        return self.bandwidth

    def set_orig_bandwidth(self, orig_bandwidth):
        self.orig_bandwidth = orig_bandwidth

    def get_orig_bandwidth(self):
        return self.orig_bandwidth

    def get_flow_bandwidth(self, fl):
        if self.has_flow(fl):
            return self.flows[fl]
        else:
            raise Exception("Flow does not exist...")

    # Computing max-min fairness
    def adjust_flow_bandwidths(self):
        flow_list = self.flows.keys()

        # No flow to adjust
        if (len(flow_list) == 0):
            return

        # print "flow list: ", len(flow_list)

        flow_list.sort(key=lambda x: x.bandwidth, reverse=True)

        allocation = {}
        for fl in flow_list:
            allocation[fl] = 0

        bw_spare = self.bandwidth
        while (bw_spare > 0):
            delete_list = []

            # Distribute spare bandwidth among all flows
            if (len(flow_list) == 0):
                bw_portion = float(bw_spare) / len(allocation.keys())
                bw_spare = 0

                for fl in allocation.keys():
                    allocation[fl] = allocation[fl] + bw_portion

            else:
                bw_portion = float(bw_spare) / len(flow_list)
                bw_spare = 0

                for fl in flow_list:
                    allocation[fl] = allocation[fl] + bw_portion

                    if (allocation[fl] > fl.get_requested_bandwidth()):
                        bw_spare = bw_spare + (allocation[fl] - fl.get_requested_bandwidth())
                        allocation[fl] = fl.get_requested_bandwidth()
                        delete_list.append(fl)

                for fl in delete_list:
                    flow_list.remove(fl)

        self.flows = allocation
        # print self.flows

    def reset(self):
        self.set_flows({})
        self.set_bandwidth(self.orig_bandwidth)
