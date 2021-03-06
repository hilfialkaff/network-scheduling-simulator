from copy import deepcopy
import logging
import optparse
import sys

sys.path.append("./src")

from manager import Manager
from topology import Jellyfish2Topology, FatTreeTopology
from parse import ParsePlacementWorkload
from path import KPath, FWPath
from algorithm import * # pyflakes_bypass

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

CONFIG_NAME = 'config'
BANDWIDTH = 10000000 # 100000 # 100 MBps link

num_jobs = [] # Number of jobs to run in the cluster from the traces
num_ports = [] # Number of ports in the topology
num_hosts = [] # Number of host nodes in the topology
ft_num_hosts = [] # Number of host nodes in fat-tree topology
num_switches = [] # Number of switches in the topology
num_mr = [] # Number of maps/reducers
num_path = [] # Number of paths
cpu = [] # Number of CPU cores/machine
mem = [] # GB of RAM/machine

def set_logging():
    parser = optparse.OptionParser()
    parser.add_option('-l', '--logging-level', help='Logging level')
    parser.add_option('-f', '--logging-file', help='Logging file name')
    (options, args) = parser.parse_args()
    logging_level = LOGGING_LEVELS.get(options.logging_level, logging.NOTSET)
    logging.basicConfig(level=logging_level, filename=options.logging_file,
                        format='%(levelname)s %(funcName)s@%(filename)s: %(message)s')

def read_config():
    global num_jobs, num_ports, num_hosts, ft_num_hosts, num_switches, \
        num_mr, num_path, cpu, mem

    def _read_config():
        line = f.readline()
        return [int(_) for _ in line.split(' ')[1:]]

    f = open(CONFIG_NAME)

    num_jobs = _read_config()
    num_ports = _read_config()
    num_hosts = _read_config()
    ft_num_hosts = _read_config()
    num_switches = _read_config()
    num_mr = _read_config()
    num_path = _read_config()
    cpu = _read_config()
    mem = _read_config()

    f.close()

def run_placement():
    workload = "workload/FB-2010_samples_24_times_1hr_0.tsv"
    parser = ParsePlacementWorkload(workload, num_jobs[0])
    jobs = parser.parse()

    # # Jellyfish
    # for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
    #     for j in num_path: # Number of paths taken
    #         topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port, j)
    #         graph = topo.generate_graph()
    #         for k in num_mr: # Number of maps/reducers
    #             for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
    #                 mgr = Manager(graph, algorithm, Algorithm.K_PATH, deepcopy(jobs), k, k, j)
    #                 mgr.run()
    #                 mgr.clean_up()
    #                 graph.reset()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_path: # Number of paths taken
            topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port, j)
            graph = topo.generate_graph()
            paths = KPath(graph, j, 10)

            for k in num_mr: # Number of maps/reducers
                for algorithm in [HalfAnnealingAlgorithm2, RandomAlgorithm]:
                    mgr = Manager(graph, algorithm, Algorithm.K_PATH, deepcopy(jobs), k, k, j, \
                        paths)
                    mgr.run()
                    mgr.clean_up()
                    graph.reset()

    # Fat-Tree
    for i, num_host in zip(num_ports, ft_num_hosts):
        for j in num_path: # Number of paths taken
            topo = FatTreeTopology(BANDWIDTH, i, j)
            graph = topo.generate_graph()
            paths = FWPath(graph, j, 10)

            for k in num_mr:
                for algorithm in [HalfAnnealingAlgorithm2, RandomAlgorithm]:
                    mgr = Manager(graph, algorithm, Algorithm.FLOYD_WARSHALL, deepcopy(jobs), k, k, j, \
                        paths)
                    mgr.run()
                    mgr.clean_up()
                    graph.reset()

def main():
    run_placement()

if __name__ == "__main__":
    set_logging()
    read_config()
    main()
