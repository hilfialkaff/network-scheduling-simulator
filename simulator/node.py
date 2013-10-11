"""
Represents a node in the cluster.

TODO:
- Node may execute multiple mappers/reducers
"""
class Node:
    def __init__(self, node_id, cpu=0, mem=0):
        self.node_id = node_id
        self.cpu = cpu
        self.mem = mem
        self.links = []
        self.job_id_executed = -1 # Not executing any job

    def __repr__(self):
        return "Node: %s" % (self.node_id)

    def set_free(self):
        self.job_id_executed = -1

    def is_free(self):
        return self.job_id_executed == -1

    def get_id(self):
        return self.node_id

    def get_cpu(self):
        return self.cpu

    def set_cpu(self, cpu):
        self.cpu = cpu

    def get_mem(self):
        return self.mem

    def set_mem(self, mem):
        self.mem = mem

    def get_type(self):
        _type = ""
        if self.node_id.startswith("h"):
            _type = "host"
        elif self.node_id.startswith("s"):
            _type = "switch"

        return _type

    def get_link(self, node):
        ret = None

        for link in self.links:
            end_point = link.get_end_points()

            if (end_point[0] == node.get_id()) or (end_point[1] == node.get_id()):
                ret = link
                break

        return ret

    def get_links(self):
        return self.links

    def add_link(self, link):
        self.links.append(link)

    def get_job_id_executed(self):
        return self.job_id_executed

    def set_job_id_executed(self, job_id):
        self.job_id_executed = job_id
