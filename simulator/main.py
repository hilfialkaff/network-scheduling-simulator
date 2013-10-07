from copy import deepcopy
import sys
import logging
import optparse

from manager import Manager
from topology import JellyfishTopology, Jellyfish2Topology, FatTreeTopology
from algorithm import *
from drf import DRF
from parse import ParsePlacementWorkload, ParseDRFWorkload


LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

CONFIG_NAME = 'config'
BANDWIDTH = 10000000 # 100 MBps link

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
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm]:
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, deepcopy(jobs), j, j)
                mgr.graph.plot("jf_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm]:
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, jobs, j, j)
                mgr.graph.plot("jf2_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Fat-Tree
    for i, num_host in zip(num_ports, ft_num_hosts):
        for j in num_mr:
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
                topo = FatTreeTopology(BANDWIDTH, i)
                mgr = Manager(topo, algorithm, Algorithm.FLOYD_WARSHALL, num_host, jobs, j, j)
                mgr.graph.plot("ft_" + str(i))
                mgr.run()
                mgr.clean_up()

def run_drf():
    workload = "workload/facebook.tsv"
    parser = ParseDRFWorkload(workload, num_jobs[0], BANDWIDTH/10) # TODO
    jobs = parser.parse()

    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2DRF, HalfAnnealingAlgorithmDRF]:
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, jobs, j, j, \
                    cpu[0], mem[0], DRF.STATIC_DRF)
                mgr.graph.plot("jf_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2DRF, HalfAnnealingAlgorithmDRF]:
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, jobs, j, j, \
                    cpu[0], mem[0], DRF.STATIC_DRF)
                mgr.graph.plot("jf2_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Fat-Tree
    for i, num_host in zip(num_ports, ft_num_hosts):
        for j in num_mr:
            for algorithm in [HalfAnnealingAlgorithm2DRF, \
                HalfAnnealingAlgorithmDRF, RandomAlgorithmDRF]:
                topo = FatTreeTopology(BANDWIDTH, i)
                mgr = Manager(topo, algorithm, Algorithm.FLOYD_WARSHALL, num_host, jobs, j, j, \
                    cpu[0], mem[0], DRF.STATIC_DRF)
                mgr.graph.plot("ft_" + str(i))
                mgr.run()
                mgr.clean_up()

def main():
    run_placement()
    run_drf()

if __name__ == "__main__":
    set_logging()
    read_config()
    sys.exit(main())
