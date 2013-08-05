from manager import Manager
from topology import *
import sys

BANDWIDTH = 100 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv"

def main():
    num_ports = range(4, 10)
    # Fat-tree: num_hosts = [16, 20, 54, 63, 128, 144]
    num_hosts = [16, 20, 45, 61, 80, 101]
    num_switches = [20, 31, 45, 61, 80, 101]
    num_mr = range(4, 8)

    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            print "jf %d %d" % (num_port, j)
            for _ in range(1):
                topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
                graph = topo.generate_graph()
                mgr = Manager(graph, WORKLOAD, j, j)
                mgr.graph.plot("jellyfish_" + str(num_port))
                mgr.run()
                mgr.clean_up()
            print "\n"

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        for j in num_mr: # Number of maps/reducers
            print "jf %d %d" % (num_port, j)
            for _ in range(1):
                topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
                graph = topo.generate_graph()
                mgr = Manager(graph, WORKLOAD, j, j)
                mgr.graph.plot("jellyfish_" + str(num_port))
                mgr.run()
                mgr.clean_up()
            print "\n"

    # Fat-Tree
    for i in num_ports:
        for j in num_mr: # Number of maps/reducers
            print "ft %d %d" % (i, j)
            topo = FatTreeTopology(BANDWIDTH, i)
            graph = topo.generate_graph()
            mgr = Manager(graph, WORKLOAD, j, j)
            mgr.graph.plot("fattree_" + str(i))
            mgr.run()
            mgr.clean_up()
            print "\n"

if __name__ == "__main__":
  sys.exit(main())
