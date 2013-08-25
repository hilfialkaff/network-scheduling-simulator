import numpy as np
import os
import matplotlib.pyplot as plt
import statsmodels.api as sm

LOG_NAME="./3_log"
PLOT_DIR="./figs/"

jobs = {}
algo = {}

class Job:
    def __init__(self, submitted, start):
        self.submitted = submitted
        self.start = start
        self.end = 0
        self.util = float(0)
        self.util_helper = 1

def parse():
    f = open(LOG_NAME)

    topo = ""
    num_host = 0
    num_mr = 0

    for line in f:
        l = line.split()

        if "JF" in line or "FT" in line:
            (topo, routing, num_host, num_mr) = l[0], l[1], int(l[2]), int(l[3])

            if topo not in jobs:
                jobs[topo] = {}
                algo[topo] = {}
            if routing not in jobs[topo]:
                jobs[topo][routing] = {}
                algo[topo][routing] = {}
            if num_mr not in jobs[topo][routing]:
                jobs[topo][routing][num_mr] = {}
                algo[topo][routing][num_mr] = {}
            if num_host not in jobs[topo][routing][num_mr]:
                jobs[topo][routing][num_mr][num_host] = {}
                algo[topo][routing][num_mr][num_host] = []

        if "Executing" in line:
            job_id = int(l[2])
            submitted = int(l[-3])
            start = int(l[-1])

            job = Job(submitted, start)
            jobs[topo][routing][num_mr][num_host][job_id] = job

        if "utilization" in line:
            job_id = int(l[1])
            cur_util = float(l[-1])/1e6
            job = jobs[topo][routing][num_mr][num_host][job_id]

            job.util = (job.util * (job.util_helper - 1)/job.util_helper) + cur_util * 1/job.util_helper
            job.util_helper += 1

        if "done" in line:
            job_id = int(l[1])
            jobs[topo][routing][num_mr][num_host][job_id].end = int(l[-1])

        if "Algorithm" in line:
            algo[topo][routing][num_mr][num_host].append(float(l[-1]))

    f.close()

""" Same topology, varied routing """
def plot_routing_throughput(name):
    global jobs

    fig = plt.figure()
    ax = fig.add_subplot(111)

    print "name:", name
    for topo, _ in jobs.items():
        if topo != name:
            continue

        for routing, __ in _.items():
            for num_mr, ___ in __.items():
                y = []
                x = []

                for num_host, _jobs in sorted(___.items()):
                    x.append(num_host)
                    y.append([job.util for job in _jobs.values()])

                print routing, num_mr
                print "x:", x
                print "y:", [np.average(arr) for arr in y]

                plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], \
                             fmt='--o', label=(str(routing) + " " + str(num_mr)))

    # ax.set_title(name + " w/ Varied Routing")
    ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Throughput (MBps)")

    plt.savefig(PLOT_DIR + name.lower() + "_throughput.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all throughput graphs """
def plot_throughput():
    print "Plotting throughput..."

    plot_routing_throughput("JF")
    plot_routing_throughput("JF2")
    plot_routing_throughput("FT")

def plot_routing_delay(name):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    num_bins = 100
    def arange(arr):
        return np.arange(min(arr), max(arr), (max(arr) - min(arr)) / num_bins)

    for topo, _ in jobs.items():
        if topo != name:
            continue

        for routing, __ in _.items():
            for num_mr, ___ in [__.items()[1]]:
                y = []
                x = []

                for num_host, _jobs in sorted(___.items()):
                    sample = [(job.end - job.start) for job in _jobs.values()]
                    ecdf = sm.distributions.ECDF(sample)

                    x = np.linspace(min(sample), max(sample))
                    y = ecdf(x)
                    plt.plot(x, y, label=(str(routing) + " " + str(num_mr) + " " + str(num_host)))

    # ax.set_title(name + "w/ Varied Routing")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Delay (s)")
    ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))

    plt.savefig(PLOT_DIR + name.lower() + "_delay.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all delay graphs """
def plot_delay():
    plot_routing_delay("JF")
    plot_routing_delay("JF2")
    plot_routing_delay("FT")

""" Same topology, varied routing """
def plot_routing_algo(name):
    global algo

    fig = plt.figure()
    ax = fig.add_subplot(111)

    for topo, _ in algo.items():
        if topo != name:
            continue

        for routing, __ in _.items():
            for num_mr, ___ in __.items():
                y = []
                x = []

                for num_host, times in sorted(___.items()):
                    x.append(num_host)
                    y.append(times)

                plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], \
                             fmt='--o', label=(str(routing) + " " + str(num_mr)))

    # ax.set_title(name + " w/ Varied Routing")
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Computation time (s)")
    ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))

    plt.savefig(PLOT_DIR + name.lower() + "_algo.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all algorithm graphs """
def plot_algorithm():
    plot_routing_algo("JF")
    plot_routing_algo("JF2")
    plot_routing_algo("FT")

""" Same topology, varied routing """
def plot_routing_ct(name):
    global jobs

    fig = plt.figure()
    ax = fig.add_subplot(111)

    print "name:", name
    for topo, _ in jobs.items():
        if topo != name:
            continue

        for routing, __ in _.items():
            for num_mr, ___ in __.items():
                y = []
                x = []

                for num_host, _jobs in sorted(___.items()):
                    x.append(num_host)
                    y.append([(job.end - job.start) for job in _jobs.values()])

                print routing, num_mr
                print "x:", x
                print "y:", [np.average(arr) for arr in y]

                plt.errorbar(x, [np.average(arr) for arr in y],  \
                             fmt='--o', label=(str(routing) + " " + str(num_mr)))

    # ax.set_title(name + " w/ Varied Routing")
    ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Completion time (s)")

    plt.savefig(PLOT_DIR + name.lower() + "_ct.png", format="png", bbox_inches='tight')
    plt.clf()

""" Same topology, varied routing """
def plot_completion_time():
    print "Plotting completion time..."
    plot_routing_ct("JF")
    plot_routing_ct("JF2")
    plot_routing_ct("FT")

if __name__ == '__main__':
    parse()
    plot_throughput()
    # plot_algorithm()
    # plot_delay()
    plot_completion_time()
