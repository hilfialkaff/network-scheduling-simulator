from datacenter import DataCenter
from topology import *
import sys
from time import time
from random import randrange

bandwidth = 100
LOGS = "log"

def clearLog():
  f = open(LOGS, 'w')
  f.close()

def setLog(graph_type, *args):
  f = open(LOGS, 'a')
  f.write('\n' + graph_type + ''.join(['_' + str(a) for a in args]) + '\n')
  f.close()

def timeRoute(dc):
    start = time()
    dc.computeRoute()
    end = time()
    f = open(LOGS, 'a')
    f.write("Time taken: %f" % (end - start) + '\n')
    f.close()

def main():

    # Fat-Tree
    # for i in [4, 8, 12, 16, 20]: # Number of ports/server/switches
    #   for j in [2, 4, 6, 8]: # Number of flows
    #     print "\nft %d %d" % (i, j)
    #     fatTopo = FatTreeTopology(bandwidth, i)
    #     dc = DataCenter(fatTopo)
    #     # setLog('ft', j, bandwidth, i)
    #     # dc.displayTopology()
    #     # for _ in range(1, 10):
    #     #   dc.generateCommPattern(j)
    #     #   # timeRoute(dc)
    #     #   dc.computeRoute()
    #     # del dc

    # Jellyfish
    for i in [4]: # Number of ports/server/switches
        for j in [2, 4, 6, 8]: # Number of flows
            print "\njf %d %d" % (i * 4, j)
            for _ in range(1, 10):
                jellyTopo = JellyfishTopology(bandwidth, i * 4, i * 5, i)
                dc = DataCenter(jellyTopo)

  #       # setLog('jf', j, bandwidth, i * 4, i * 5, i, 0)
  #       # dc.displayTopology()
  #       dc.generateCommPattern(j)
  #       dc.computeRoute()
  #       # timeRoute(dc)
  #       del dc
  #       # dc.cleanUp()


  # print "Adding tenant with 4-5 nodes..."
  # dc.addTenant(4, 5)

  # print "Adding tenant with 6-8 nodes..."
  # dc.addTenant(6, 8)

  # print "Adding tenant with 7-11 nodes..."
  # dc.addTenant(7, 11)

  # dc.listVMs()
  # dc.computeMapReduce()

if __name__ == "__main__":
  sys.exit(main())
