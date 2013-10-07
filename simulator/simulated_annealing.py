from random import random
from job_config import JobConfig
from math import exp

class SimulatedAnnealing:
    def __init__(self, max_util, max_step, init_state, generate_neighbor, compute_util, \
        check_constraint=None):
        self.max_util = max_util
        self.max_step = max_step
        self.init_state = init_state
        self.generate_neighbor = generate_neighbor
        self.compute_util = compute_util
        self.check_constraint = None

    def transition(self, old_util, new_util, temperature):
        ret = 1
        _new_util = new_util.get_total_util()
        _old_util = old_util.get_total_util()

        if _old_util > _new_util:
            ret = exp((_new_util - _old_util)/temperature)

        return ret

    def find_temperature(self, step):
        return 1/(step * 0.01)

    def run(self):
        step = 1
        util = JobConfig(0, None, None)
        best_util = JobConfig(0, None, None)
        state = self.init_state()

        if self.check_constraint:
            while self.check_constraint(state):
                state = self.init_state()

        while step < self.max_step and util.get_util() < self.max_util:
            temperature = self.find_temperature(float(step)/self.max_step)
            new_state = self.generate_neighbor(state)

            if self.check_constraint:
                while self.check_constraint(new_state):
                    new_state = self.generate_neighbor(new_state)
            new_util = self.compute_util(new_state, self.max_util)

            if self.transition(util, new_util, temperature) > random():
                state = new_state
                util = new_util

            if new_util.compare(best_util):
                best_util = new_util

            step += 1

        return best_util
