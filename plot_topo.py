from topology import JellyfishTopology, Jellyfish2Topology, FatTreeTopology
from algorithm import * # pyflakes_bypass

CONFIG_NAME = '../config'
BANDWIDTH = 100000 # 100 MBps link

num_jobs = [] # Number of jobs to run in the cluster from the traces
num_ports = [] # Number of ports in the topology
num_hosts = [] # Number of host nodes in the topology
ft_num_hosts = [] # Number of host nodes in fat-tree topology
num_switches = [] # Number of switches in the topology
num_mr = [] # Number of maps/reducers
cpu = [] # Number of CPU cores/machine
mem = [] # GB of RAM/machine

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

def plot_topo():
    # Jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        topo = JellyfishTopology(BANDWIDTH, num_host, num_switch, num_port)
        graph = topo.generate_graph()
        graph.plot("jf_" + str(num_port))

    # Modified jellyfish
    for num_port, num_host, num_switch in zip(num_ports, num_hosts, num_switches):
        topo = Jellyfish2Topology(BANDWIDTH, num_host, num_switch, num_port)
        graph = topo.generate_graph()
        graph.plot("jf2_" + str(num_port))

    # Fat-Tree
    for i, num_host in zip(num_ports, ft_num_hosts):
        topo = FatTreeTopology(BANDWIDTH, i)
        graph = topo.generate_graph()
        graph.plot("ft_" + str(i))

if __name__ == '__main__':
    read_config()
    plot_topo()
