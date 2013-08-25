from manager import Manager
from topology import *
from routing import *
import sys

BANDWIDTH = 10000000 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv"

def main():
    # num_ports = [6, 7, 8, 9, 10, 11]
    # num_hosts = [45, 61, 80, 101, 125, 151] # Number of host nodes in the topology
    # ft_num_hosts = [54, 63, 128, 144, 250, 275] # Number of host nodes in fat-tree topology
    # num_switches = [45, 61, 80, 101, 125, 151] # Number of switches in the topology
    # num_mr = [2, 4] # Number of maps/reducers

    num_ports = [6, 7, 8, 9, 10]
    num_hosts = [45, 61, 80, 101, 125] # Number of host nodes in the topology
    ft_num_hosts = [54, 63, 128, 144, 250] # Number of host nodes in fat-tree topology
    num_switches = [45, 61, 80, 101, 125] # Number of switches in the topology
    num_mr = [2, 4] # Number of maps/reducers

    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for routing in [HalfAnnealingRouting2]:
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, routing, num_host, WORKLOAD, j, j)
                mgr.graph.plot("jf_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for routing in [HalfAnnealingRouting2, HalfAnnealingRouting, RandomRouting]:
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                mgr = Manager(topo, routing, num_host, WORKLOAD, j, j)
                mgr.graph.plot("jf2_" + str(num_port))
                mgr.run()
                mgr.clean_up()

    # Fat-Tree
    for i, num_host in zip(num_ports, ft_num_hosts):
        for j in num_mr:
            for routing in [HalfAnnealingRouting2, HalfAnnealingRouting, RandomRouting]:
                topo = FatTreeTopology(BANDWIDTH, i)
                mgr = Manager(topo, routing, num_host, WORKLOAD, j, j)
                mgr.graph.plot("ft_" + str(i))
                mgr.run()
                mgr.clean_up()

if __name__ == "__main__":
  sys.exit(main())
