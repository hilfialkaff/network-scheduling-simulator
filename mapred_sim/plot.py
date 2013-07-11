import numpy as np
import os
import matplotlib.pyplot as plt

""

LOG = "./log"
first_times = {}
first_utils = {}
kpath_times = {}
kpath_utils = {}
fails_times = {}
fails_utils = {}

def init():
    first_times["jf"] = {}
    first_utils["jf"] = {}
    kpath_times["jf"] = {}
    kpath_utils["jf"] = {}
    fails_times["jf"] = {}
    fails_utils["jf"] = {}

    # for i in [4, 8, 12, 16, 20]: # Number of ports/server/switches
    #     times["ft"][i] = {} 
    #     for j in [2, 4, 6, 8]: # Number of flows
    #         times["ft"][i][j] = []

    for i in [10, 20, 30, 40, 50]: # Number of ports/server/switches
        first_times["jf"][i * 4] = {}
        first_utils["jf"][i * 4] = {}
        kpath_times["jf"][i * 4] = {}
        kpath_utils["jf"][i * 4] = {}
        fails_times["jf"][i * 4] = {}
        fails_utils["jf"][i * 4] = {}
        for j in [2, 4, 6, 8]: # Number of flows
            first_times["jf"][i * 4][j] = []
            first_utils["jf"][i * 4][j] = []
            kpath_times["jf"][i * 4][j] = []
            kpath_utils["jf"][i * 4][j] = []
            fails_times["jf"][i * 4][j] = []
            fails_utils["jf"][i * 4][j] = []

def parse():
    f = open(LOG)
    l = f.readline()
    while l != '':
        arr = l.split(' ')
        name = arr[0]
        numHost = int(arr[1])
        numFlow = int(arr[2])

        l = f.readline()
        l = f.readline()
        while not l.startswith("jf"):
            k = int(l.split('=')[1])
            l = f.readline()
            utils = float(l.split(' ')[-1])
            first_utils[name][numHost][numFlow].append(utils)
            l = f.readline()
            time = float(l.split(' ')[-2])
            first_times[name][numHost][numFlow].append(time)
            l = f.readline()
            l = f.readline()
            if not l.startswith("No flows"):
                utils = float(l.split(' ')[-1])
            l = f.readline()
            time = float(l.split(' ')[-2])
            l = f.readline()
            l = f.readline()

            k = int(l.split('=')[1])
            l = f.readline()
            utils = float(l.split(' ')[-1])
            kpath_utils[name][numHost][numFlow].append(utils)
            l = f.readline()
            time = float(l.split(' ')[-2])
            kpath_times[name][numHost][numFlow].append(time)
            l = f.readline()
            l = f.readline()
            if not l.startswith("No flows"):
                utils = float(l.split(' ')[-1])
            fails_utils[name][numHost][numFlow].append(utils)
            l = f.readline()
            time = float(l.split(' ')[-2])
            fails_times[name][numHost][numFlow].append(time)
            l = f.readline()
            l = f.readline()

            if l == '':
                break

    # print "first_times: ", first_times
    # print "first_utils: ", first_utils
    # print "kpath_times: ", kpath_times
    # print "kpath_utils: ", kpath_utils
    # print "fails_times: ", fails_times
    # print "fails_utils: ", fails_utils

def plot_flows():
    fig = plt.figure()
    # ax = fig.add_subplot(111)
    # x = sorted(times["ft"][16].keys())
    # y = sorted(times["ft"][16].values())

    # # print "read avg: ", [np.average(arr) for arr in y]
    # # print "read std: ", [np.std(arr) for arr in y]
    # plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt='o-')

    # plt.legend()
    # plt.xlim([1, 9])
    # ax.set_xlabel("Number of flows")
    # ax.set_ylabel("Time taken (ms)")
    # ax.set_title("Fat tree 1024 server")
    # plt.savefig("./fattree_flow.png", format='png')
    # plt.clf()

    ax = fig.add_subplot(111)

    for graph in [first_times["jf"], kpath_times["jf"]]:
        x = sorted(graph[200].keys())
        y = sorted(graph[200].values())

        if graph == first_times["jf"]:
            label = "First path"
            fmt = 'b^-'
        elif graph == kpath_times["jf"]:
            label = "K-path"
            fmt = 'ro-'

        plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt=fmt, label=label)

    plt.legend(loc=2)
    plt.xlim([1, 9])
    plt.ylim([0, max([np.average(arr) for arr in y]) + max([np.std(arr) for arr in y])])
    ax.set_xlabel("Number of flows")
    ax.set_ylabel("Time taken (ms)")
    ax.set_title("Jellyfish 200 servers")
    plt.savefig("jellyfish_flows.png", format='png')

def plot_servers():
    fig = plt.figure()

    # ax = fig.add_subplot(111)
    # x = []
    # y = []
    # for k,v in sorted(times["ft"].items()):
    #     x.append((k**3)/4)
    #     y.append(v[4])

    # plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt='o-')

    # plt.legend()
    # plt.xlim([min(x) * 0.9, max(x) * 1.1])
    # ax.set_xlabel("Number of server")
    # ax.set_ylabel("Time taken (ms)")
    # ax.set_title("Fat tree 4 flows")
    # plt.savefig("fattree_server.png", format='png')
    # plt.clf()

    ax = fig.add_subplot(111)

    for graph in [first_times["jf"], kpath_times["jf"]]:
        x = []
        y = []
        label = ""

        for k, v in sorted(graph.items()):
            x.append(k)
            y.append(v[8])

        if graph == first_times["jf"]:
            label = "First path"
            fmt = 'b^-'
        elif graph == kpath_times["jf"]:
            label = "K-path"
            fmt = 'ro-'

        plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt=fmt, label=label)
    plt.legend(loc=2)

    title = "Jellyfish 8 flows"
    plt.xlim([min(x) * 0.9, max(x) * 1.1])
    plt.ylim([0, max([np.average(arr) for arr in y]) + max([np.std(arr) for arr in y])])
    ax.set_xlabel("Number of server")
    ax.set_ylabel("Time taken (ms)")
    ax.set_title(title)

    plt.savefig("jellyfish_servers.png", format='png')

def plot_utils():
    fig = plt.figure()

    # ax = fig.add_subplot(111)
    # x = []
    # y = []
    # for k,v in sorted(times["ft"].items()):
    #     x.append((k**3)/4)
    #     y.append(v[4])

    # plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt='o-')

    # plt.legend()
    # plt.xlim([min(x) * 0.9, max(x) * 1.1])
    # ax.set_xlabel("Number of server")
    # ax.set_ylabel("Time taken (s)")
    # ax.set_title("Fat tree 4 flows")
    # plt.savefig("fattree_server.png", format='png')
    # plt.clf()

    ax = fig.add_subplot(111)

    for graph in [kpath_utils["jf"]]:
        x = []
        y = []
        label = ""
        fmt = ""

        for k, v in sorted(graph.items()):
            x.append(k)
            y.append(v[8])

        # if graph == first_utils["jf"]:
        #     label = "First path"
        #     fmt = 'b^-'
        if graph == kpath_utils["jf"]:
            # label = "K-path"
            fmt = 'o-'

        plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt=fmt, label=label)
    plt.legend()

    title = "Jellyfish 8 flows"
    plt.xlim([min(x) * 0.9, max(x) * 1.1])
    plt.ylim([min([np.average(arr) for arr in y]) - max([np.std(arr) for arr in y]), max([np.average(arr) for arr in y]) + max([np.std(arr) for arr in y])])
    ax.set_xlabel("Number of server")
    ax.set_ylabel("Bandwidth KBps")
    ax.set_title(title)

    plt.savefig("jellyfish_utils.png", format='png')

def plot_fails_servers():
    fig = plt.figure()

    ax = fig.add_subplot(111)

    graph = fails_times["jf"]
    x = []
    y = []
    label = ""

    for k, v in sorted(graph.items()):
        x.append(k)
        y.append(v[8])

    plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt='o-')
    plt.legend()

    title = "Recovery: Jellyfish 8 flows after 25% failures"
    plt.xlim([min(x) * 0.9, max(x) * 1.1])
    plt.ylim([0, max([np.average(arr) for arr in y]) + max([np.std(arr) for arr in y])])
    ax.set_xlabel("Number of server")
    ax.set_ylabel("Time taken (ms)")
    ax.set_title(title)

    plt.savefig("fails_servers.png", format='png')

def plot_fails_utils():
    fig = plt.figure()

    ax = fig.add_subplot(111)

    graph = fails_utils["jf"]
    x = []
    y = []
    label = ""

    for k, v in sorted(graph.items()):
        x.append(k)
        y.append(v[8])

    plt.errorbar(x, [np.average(arr) for arr in y], yerr=[np.std(arr) for arr in y], fmt='o-')
    plt.legend()

    title = "Recovery: Jellyfish 8 flows after 25% failures"
    plt.xlim([min(x) * 0.9, max(x) * 1.1])
    plt.ylim([min([np.average(arr) for arr in y]) - max([np.std(arr) for arr in y]), max([np.average(arr) for arr in y]) + max([np.std(arr) for arr in y])])
    ax.set_xlabel("Number of server")
    ax.set_ylabel("Bandwidth KBps")
    ax.set_title(title)

    plt.savefig("fails_utils.png", format='png')

if __name__ == '__main__':
    init()
    parse()
    plot_flows()
    plot_servers()
    plot_utils()
    plot_fails_servers()
    plot_fails_utils()
