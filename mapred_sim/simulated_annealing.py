from random import random

class SimulatedAnnealing:
    def __init__(self, max_util, max_step, init_state, find_temperature, generate_neighbor, compute_util, transition):
        self.max_util = max_util
        self.max_step = max_step
        self.init_state = init_state
        self.find_temperature = find_temperature
        self.generate_neighbor = generate_neighbor
        self.compute_util = compute_util
        self.transition = transition

    def run(self):
        step = 1
        util = 0
        best_util = 0
        state = self.init_state()

        while step < self.max_step and util < self.max_util:
            temperature = self.find_temperature(float(step)/self.max_step)
            new_state = self.generate_neighbor(state)
            new_util = self.compute_util(new_state)

            if self.transition(util, new_util, temperature) > random():
                state = new_state
                util = new_util

            if new_util > best_util:
                best_util = new_util

            step += 1

        return best_util
