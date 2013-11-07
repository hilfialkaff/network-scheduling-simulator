"""
Usage: ./plot.py <log_name>
"""
import numpy as np
import sys
import matplotlib.pyplot as plt
import statsmodels.api as sm

PLOT_DIR="./figs/"

jobs = {}
algo = {}

NET = 0
CPU = 1
rsrc_name = {NET: "network", CPU: "cpu"}

class Job:
    def __init__(self):
        self.submitted = 0
        self.start = 0
        self.end = 0
        self.util = float(0)
        self.util_helper = 1
        self.net_usage = []
        self.cpu_usage = []
        self.mem_usage = []

def parse(log_name):
    f = open(log_name)

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

            if job_id not in jobs[topo][routing][num_mr][num_host]:
                jobs[topo][routing][num_mr][num_host][job_id] = Job()

            jobs[topo][routing][num_mr][num_host][job_id].submitted = submitted
            jobs[topo][routing][num_mr][num_host][job_id].start = start

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

        if "allocation" in line:
            job_id = int(line.split(' ')[1])
            tmp = line.split(',')

            if job_id not in jobs[topo][routing][num_mr][num_host]:
                jobs[topo][routing][num_mr][num_host][job_id] = Job()

            jobs[topo][routing][num_mr][num_host][job_id].net_usage.append(float(tmp[0].split('->')[1]))
            jobs[topo][routing][num_mr][num_host][job_id].cpu_usage.append(float(tmp[1].split('->')[1]))
            jobs[topo][routing][num_mr][num_host][job_id].mem_usage.append(float(tmp[2].split('->')[1]))
    f.close()

""" Same topology, varied routing """
def plot_routing_throughput(name, routings=[], num_mrs=[]):
    global jobs

    fig = plt.figure()
    ax = fig.add_subplot(111)
    width = 0.2
    i = 0
    bars = []
    labels = []
    ind = []
    hatches = [' ', 'x', '+', 'o', '.', '*', '/']

    for topo, _ in jobs.items():
        if topo != name:
            continue

        for routing, __ in reversed(sorted(_.items())):
            if routings and routing not in routings:
                continue

            for num_mr, ___ in __.items():
                if num_mrs and num_mr not in num_mrs:
                    continue

                y = []
                x = []

                for num_host, _jobs in sorted(___.items()):
                    x.append(num_host)
                    y.append([job.util for job in _jobs.values()])

                ind = np.arange(len(x))
                means = [np.average(arr) for arr in y]
                std = [np.std(arr) for arr in y]
                bar = ax.bar(ind + width * i, means, width, yerr=std, color='w', ecolor='r', \
                    hatch=hatches[i])
                bars.append(bar[0])
                labels.append(str(routing) + " " + str(num_mr))

                i += 1

    # ax.set_title(name + " w/ Varied Routing")
    plt.ylim(ymin=0)
    ax.set_xticks(ind + (width * i)/2)
    ax.set_xticklabels(x)
    # ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))
    ax.legend(bars, labels, bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, mode="expand", borderaxespad=0.)
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Throughput (MBps)")

    plt.savefig(PLOT_DIR + name.lower() + "_throughput.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all throughput graphs """
def plot_throughput():
    print "Plotting throughput..."

    # plot_routing_throughput("FT_2", ["RR", "HAA"], [4])
    plot_routing_throughput("FT_4", ["RR", "HAA2"], [4])
    # plot_routing_throughput("JF2_2")
    plot_routing_throughput("JF2_4", ["RR", "HAA2"], [4])

def plot_routing_delay(name, routings=[], num_mrs=[]):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    num_bins = 100
    def arange(arr):
        return np.arange(min(arr), max(arr), (max(arr) - min(arr)) / num_bins)

    for topo, _ in jobs.items():
        if topo != name:
            continue

        for routing, __ in _.items():
            if routings and routing not in routings:
                continue

            for num_mr, ___ in [__.items()[1]]:
                if num_mrs and num_mr not in num_mrs:
                    continue

                y = []
                x = []

                for num_host, _jobs in sorted(___.items()):
                    if num_host != 63:
                        continue

                    sample = [(job.end - job.submitted) for job in _jobs.values()]
                    ecdf = sm.distributions.ECDF(sample)

                    x = np.linspace(min(sample), max(sample), num=1000)
                    y = ecdf(x)
                    plt.plot(x, y, label=(str(routing) + " " + str(num_mr) + " " + str(num_host)))

    # ax.set_title(name + "w/ Varied Routing")
    ax.set_ylabel("Probability")
    ax.set_xlabel("Delay (s)")
    ax.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, mode="expand", borderaxespad=0.)

    plt.savefig(PLOT_DIR + name.lower() + "_delay.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all delay graphs """
def plot_delay():
    print "Plotting delay..."

    # plot_routing_delay("JF")
    # plot_routing_delay("JF2")
    # plot_routing_delay("FT")

    # plot_routing_delay("JF2_2")
    plot_routing_delay("JF2_4", ["RR", "HAA2"], [4])
    # plot_routing_delay("FT_2")
    plot_routing_delay("FT_4", ["RR", "HAA2"], [4])

""" Same topology, varied routing """
def plot_routing_algo(name, routings=[], num_mrs=[]):
    global algo

    fig = plt.figure()
    ax = fig.add_subplot(111)
    width = 0.35
    i = 0.5

    for topo, _ in algo.items():
        if topo != name:
            continue

        for routing, __ in reversed(sorted(_.items())):
            if routings and routing not in routings:
                continue

            for num_mr, ___ in __.items():
                if num_mrs and num_mr not in num_mrs:
                    continue

                y = []
                x = []

                for num_host, times in sorted(___.items()):
                    x.append(num_host)
                    y.append(times)

                ind = np.arange(len(x))
                means = [np.average(arr) for arr in y]
                std = [np.std(arr) for arr in y]
                ax.set_xticks(ind + width)
                ax.set_xticklabels(x)
                ax.bar(ind + width * i, means, width, yerr=std, color='0.75', ecolor='r')
                plt.ylim(ymin=0)
            i += 1

    # ax.set_title(name + " w/ Varied Routing")
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Computation time (s)")

    plt.savefig(PLOT_DIR + name.lower() + "_algo.png", format="png", bbox_inches='tight')
    plt.clf()

""" Plot all algorithm graphs """
def plot_algorithm():
    print "Plotting algorithm..."

    # plot_routing_algo("JF")
    # plot_routing_algo("JF2")
    # plot_routing_algo("FT")

    # plot_routing_algo("FT_2")
    plot_routing_algo("FT_4", ["HAA2"], [4])
    # plot_routing_algo("JF2_2")
    plot_routing_algo("JF2_4", ["HAA2"], [4])

""" Same topology, varied routing """
def plot_routing_ct(name):
    global jobs

    fig = plt.figure()
    ax = fig.add_subplot(111)

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
    # plot_routing_ct("JF")
    # plot_routing_ct("JF2")
    # plot_routing_ct("FT")

    plot_routing_ct("FT_2")
    plot_routing_ct("FT_4")
    # plot_routing_ct("JF2_2")
    # plot_routing_ct("JF2_4")

def plot_routing_rsrc_changes(name, rsrc_type, is_incremental):
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

                    if is_incremental:
                        div = 2
                    else:
                        div = 1

                    if rsrc_type == NET:
                        y.append([np.std(job.net_usage[:len(job.net_usage)/div])/div for job in _jobs.values()])
                    elif rsrc_type == CPU:
                        y.append([np.std(job.cpu_usage[:len(job.cpu_usage)/div])/div for job in _jobs.values()])

                print routing, num_mr
                print "x:", x
                print "y:", [np.average(arr) for arr in y]

                plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], \
                             fmt='--o', label=(str(routing) + " " + str(num_mr)))

    # ax.set_title(name + " w/ Varied Routing")
    ax.legend(loc='center right', bbox_to_anchor=(1.30,0.5))
    ax.set_xlabel("Number of hosts")
    ax.set_ylabel("Changes in " + rsrc_name[rsrc_type] + '_' + str(is_incremental))

    plt.savefig(PLOT_DIR + name.lower() + "_" + rsrc_name[rsrc_type] + "_changes.png", \
        format="png", bbox_inches='tight')
    plt.clf()

def plot_rsrc_changes():
    print "Plotting resource changes..."
    plot_routing_rsrc_changes("JF", CPU, 0)
    plot_routing_rsrc_changes("JF", NET, 0)
    plot_routing_rsrc_changes("JF2", CPU, 0)
    plot_routing_rsrc_changes("JF2", NET, 0)

    plot_routing_rsrc_changes("JF", CPU, 1)
    plot_routing_rsrc_changes("JF", NET, 1)
    plot_routing_rsrc_changes("JF2", CPU, 1)
    plot_routing_rsrc_changes("JF2", NET, 1)

if __name__ == '__main__':
    log_name = sys.argv[1]

    parse(log_name)
    # plot_throughput()
    # plot_algorithm()
    plot_delay()
    # plot_completion_time()
    # plot_rsrc_changes()
