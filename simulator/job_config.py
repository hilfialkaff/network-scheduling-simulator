"""
Represents how job is placed and which links and paths are being
used in the datacenters.

TODO:
- Possibly merging this with job.py
"""

class JobConfig:
    def __init__(self, util=0, total_util=0, links=None, used_paths=None):
        self.util = util
        self.total_util = total_util # TODO: Hacky -> represents utilization of whole cluster
        self.links = links
        self.used_paths = used_paths

    def get_placements(self):
        return self.placements

    def set_placements(self, placements):
        self.placements = placements

    def get_total_util(self):
        return self.total_util

    def set_total_util(self, total_util):
        self.total_util = total_util

    def set_util(self, util):
        self.util = util

    def get_util(self):
        return self.util

    def get_links(self):
        return self.links

    def get_used_paths(self):
        return self.used_paths

    def compare(self, util):
        return self.util > util.get_util()
