from manager import Manager
from topology import *
import sys
from time import time
from random import randrange

BANDWIDTH = 100 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv" 

def main():
    # Fat-Tree
    # for i in [3, 4, 6, 8]: # Number of ports/server/switches
    for i in [4]: # Number of ports/server/switches
        for j in [2, 3]: # Number of maps/reducers
            print "ft %d %d" % (i, j)
            topo = FatTreeTopology(BANDWIDTH, i)
            graph = topo.generate_graph()
            mgr = Manager(graph, WORKLOAD, j, j)
            mgr.graph.plot("fattree_" + str(i))
            mgr.run()
            mgr.clean_up()
            print "\n"

    # Jellyfish
    for i in [4]: # Number of ports/server/switches
        for j in [2, 3]: # Number of maps/reducers
            print "jf %d %d" % (i, j)
            for _ in range(1):
                topo = JellyfishTopology(BANDWIDTH, i * 4, i * 5, i)
                graph = topo.generate_graph()
                mgr = Manager(graph, WORKLOAD, j, j)
                mgr.graph.plot("jellyfish_" + str(i))
                mgr.run()
                mgr.clean_up()
            print "\n"

if __name__ == "__main__":
  sys.exit(main())
