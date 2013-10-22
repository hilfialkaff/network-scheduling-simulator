import unittest

from drf import DRF
from job import Job
from resource import Resource

"""
Test case: Simple test with 2 jobs and 2 resource demands
"""
class TestDRF1(unittest.TestCase):
    def setUp(self):
        rsrc = Resource(net=1, cpu=9, mem=18)
        jobs = [Job(1, 0, 0, 0, 0, 0, cpu_usage=3, mem_usage=1), \
                     Job(2, 0, 0, 0, 0, 0, cpu_usage=1, mem_usage=4)]
        self.drf = DRF(rsrc, jobs)

    def test_drf(self):
        self.drf.run()

        rsrc1 = self.drf.get_resource_alloc(1)
        self.assert_(rsrc1.get_net() == 0 and rsrc1.get_cpu() == 6 and \
                     rsrc1.get_mem() == 2)
        rsrc2 = self.drf.get_resource_alloc(2)
        self.assert_(rsrc2.get_net() == 0 and rsrc2.get_cpu() == 3 and \
                     rsrc2.get_mem() == 12)

        consumed_rsrc = self.drf.get_consumed_rsrc()
        self.assert_(consumed_rsrc.get_net() == 0 and \
                     consumed_rsrc.get_cpu() == 9 and \
                     consumed_rsrc.get_mem() == 14)

"""
Test case: 10 jobs with identical demand
"""
class TestDRF2(unittest.TestCase):
    def setUp(self):
        rsrc = Resource(net=100, cpu=100, mem=100)
        jobs = []
        self.num_jobs = 10

        for i in range(self.num_jobs):
            jobs.append(Job(i, 0, 0, 0, 0, 0, cpu_usage=10, mem_usage=10, net_usage=10))
        self.drf = DRF(rsrc, jobs)

    def test_drf(self):
        self.drf.run()

        for i in range(self.num_jobs):
            rsrc = self.drf.get_resource_alloc(i)
            self.assert_(rsrc.get_net() == rsrc.get_cpu() == rsrc.get_mem() == 10)

        consumed_rsrc = self.drf.get_consumed_rsrc()
        self.assert_(consumed_rsrc.get_net() == consumed_rsrc.get_cpu() == \
                     consumed_rsrc.get_mem() == 100)

"""
Test case: 5 jobs with multiplicative demand of each other
"""
class TestDRF3(unittest.TestCase):
    def setUp(self):
        rsrc = Resource(net=80, cpu=80, mem=80)
        jobs = []
        self.num_jobs = 5

        for i in range(self.num_jobs):
            jobs.append(Job(i, 0, 0, 0, 0, 0, cpu_usage=(2**i), mem_usage=(2**i), net_usage=(2**i)))
        self.drf = DRF(rsrc, jobs)

    def test_drf(self):
        self.drf.run()

        for i in range(self.num_jobs):
            rsrc = self.drf.get_resource_alloc(i)
            self.assert_(rsrc.get_net() == rsrc.get_cpu() == rsrc.get_mem() == 16)

        consumed_rsrc = self.drf.get_consumed_rsrc()
        self.assert_(consumed_rsrc.get_net() == consumed_rsrc.get_cpu() == \
                     consumed_rsrc.get_mem() == 80)

if __name__ == '__main__':
    unittest.main()
