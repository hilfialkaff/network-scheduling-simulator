from manager import Manager
from routing import KShortestPathBWAllocation
from topology import *
import sys
from time import time
from random import randrange

BANDWIDTH = 100 # 100 MBps link
WORKLOAD = "workload/FB-2010_samples_24_times_1hr_0.tsv" 

def main():
    # Fat-Tree
    for i in [4]: # Number of ports/server/switches
        for j in [2, 3]: # Number of maps/reducers
            print "ft %d %d" % (i, j)
            fatTopo = FatTreeTopology(BANDWIDTH, i)
            mgr = Manager(fatTopo, WORKLOAD, j, j)
            # mgr.graph.plot("fattree_" + str(i))
            mgr.run()
            mgr.clean_up()
            print "\n"

    # Jellyfish
    for i in [4]: # Number of ports/server/switches
        for j in [2, 3]: # Number of maps/reducers
            print "jf %d %d" % (i, j)
            for _ in range(1):
                jellyTopo = JellyfishTopology(BANDWIDTH, i * 4, i * 5, i)
                mgr = Manager(jellyTopo, WORKLOAD, j, j)
                # mgr.graph.plot("jellyfish_" + str(i))
                mgr.run()
                mgr.clean_up()
            print "\n"

if __name__ == "__main__":
  sys.exit(main())
