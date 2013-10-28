from copy import deepcopy
import logging
import optparse

from manager import Manager
from topology import JellyfishTopology, Jellyfish2Topology, FatTreeTopology
from parse import ParsePlacementWorkload
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
        num_mr, cpu, mem

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
    cpu = _read_config()
    mem = _read_config()

    f.close()

def run_placement():
    workload = "workload/FB-2010_samples_24_times_1hr_0.tsv"
    parser = ParsePlacementWorkload(workload, num_jobs[0])
    jobs = parser.parse()

    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, deepcopy(jobs), j, j)
                mgr.run()
                mgr.clean_up()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, deepcopy(jobs), j, j)
                mgr.run()
                mgr.clean_up()

    # # Fat-Tree
    # for i, num_host in zip(num_ports, ft_num_hosts):
    #     for j in num_mr:
    #         for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
    #             topo = FatTreeTopology(BANDWIDTH, i)
    #             mgr = Manager(topo, algorithm, Algorithm.FLOYD_WARSHALL, deepcopy(jobs), j, j)
    #             mgr.run()
    #             mgr.clean_up()

def main():
    run_placement()

if __name__ == "__main__":
    set_logging()
    read_config()
    main()
