"""
Represents a resource that each node can request for
"""
class Resource:
    def __init__(self, net=.0, cpu=.0, mem=.0):
        self.net = net
        self.cpu = cpu
        self.mem = mem

    def get_net(self):
        return self.net

    def get_cpu(self):
        return self.cpu

    def get_mem(self):
        return self.mem

    def __repr__(self):
        return "Resource: net->%f, cpu->%f, mem->%f" % (self.net, self.cpu, self.mem)

    def __add__(self, other):
        new_net = self.get_net() + other.get_net()
        new_cpu = self.get_cpu() + other.get_cpu()
        new_mem = self.get_mem() + other.get_mem()

        return Resource(new_net, new_cpu, new_mem)

    def __sub__(self, other):
        new_net = self.get_net() - other.get_net()
        new_cpu = self.get_cpu() - other.get_cpu()
        new_mem = self.get_mem() - other.get_mem()

        return Resource(new_net, new_cpu, new_mem)

    def __gt__(self, other):
        return self.get_net() > other.get_net() or \
            self.get_cpu() > other.get_cpu() or \
            self.get_mem() > other.get_mem()
