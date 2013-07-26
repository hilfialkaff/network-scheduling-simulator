from random import randrange, choice, seed

class Node:
    def __init__(self, node_id, num_mappers = 1, num_reducers = 1):
        self.node_id = node_id
        self.available_mappers = num_mappers
        self.available_reducers = num_reducers
    
    def get_id(self):
        return self.node_id

    def get_available_mappers(self):
        return self.available_mappers

    def get_available_reducers(self):
        return self.available_reducers
