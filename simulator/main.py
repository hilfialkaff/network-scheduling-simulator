from manager import Manager
from topology import *
from algorithm import *
import sys

CONFIG_NAME = 'config'
BANDWIDTH = 10000000 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv"

num_ports = [] # Number of ports in the topology
num_hosts = [] # Number of host nodes in the topology
ft_num_hosts = [] # Number of host nodes in fat-tree topology
num_switches = [] # Number of switches in the topology
num_mr = [] # Number of maps/reducers

def read_config():
    f = open(CONFIG_NAME)

    global num_ports
    global num_hosts
    global ft_num_hosts
    global num_switches
    global num_mr

    line = f.readline()
    num_ports = [int(_) for _ in line.split(' ')[1:]]
    line = f.readline()
    num_hosts = [int(_) for _ in line.split(' ')[1:]]
    line = f.readline()
    ft_num_hosts = [int(_) for _ in line.split(' ')[1:]]
    line = f.readline()
    num_switches = [int(_) for _ in line.split(' ')[1:]]
    line = f.readline()
    num_mr = [int(_) for _ in line.split(' ')[1:]]

def main():
    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm]:
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, WORKLOAD, j, j)
                mgr.graph.plot("jf_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm]:
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, algorithm, Algorithm.K_PATH, num_host, WORKLOAD, j, j)
                mgr.graph.plot("jf2_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Fat-Tree
    # for i, num_host in zip(num_ports, ft_num_hosts):
    #     for j in num_mr:
    #         for algorithm in [HalfAnnealingAlgorithm2, HalfAnnealingAlgorithm, RandomAlgorithm]:
    #             topo = FatTreeTopology(BANDWIDTH, i)
    #             mgr = Manager(topo, algorithm, Algorithm.FLOYD_WARSHALL, num_host, WORKLOAD, j, j)
    #             mgr.graph.plot("ft_" + str(i))
    #             mgr.run()
    #             mgr.clean_up()

if __name__ == "__main__":
    read_config()
    sys.exit(main())
