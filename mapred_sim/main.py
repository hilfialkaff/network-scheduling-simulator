from manager import Manager
from topology import *
import sys

BANDWIDTH = 12500000 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv"

def main():
    num_ports = range(4, 10)
    num_hosts = [16, 20, 45, 61, 80, 101] # Number of host nodes in the topology
    ft_num_hosts = [16, 20, 54, 63, 128, 144] # Number of host nodes in fat-tree topology
    num_switches = [20, 31, 45, 61, 80, 101] # Number of switches in the topology
    num_mr = range(2, 5) # Number of maps/reducers

    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            for _ in range(1):
                print "Jellyfish", num_host, j
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                graph = topo.generate_graph()
                mgr = Manager(graph, WORKLOAD, j, j)
                mgr.graph.plot("jellyfish_" + str(num_port))
                mgr.run()
                mgr.clean_up()
            print ""

    # Modified jellyfish
    # for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
    #     for j in num_mr: # Number of maps/reducers
    #         print "Jellyfish2", num_host, j
    #         for _ in range(1):
    #             topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
    #             graph = topo.generate_graph()
    #             mgr = Manager(graph, WORKLOAD, j, j)
    #             mgr.graph.plot("jellyfish2_" + str(num_port))
    #             # mgr.run()
    #             mgr.clean_up()
    #         print ""

    # Fat-Tree
    # for i, num_host in zip(num_ports, ft_num_hosts):
    #     for j in num_mr:
    #         print "Fat-tree", num_host, j
    #         topo = FatTreeTopology(BANDWIDTH, i)
    #         graph = topo.generate_graph()
    #         mgr = Manager(graph, WORKLOAD, j, j)
    #         mgr.graph.plot("fattree_" + str(i))
    #         mgr.run()
    #         mgr.clean_up()
    #         print ""

if __name__ == "__main__":
  sys.exit(main())
