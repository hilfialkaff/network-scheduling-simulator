import unittest

from link import Link

"""
Test the link object by itself
"""
class TestLink1(unittest.TestCase):
    def test_link(self):
        link1 = Link('host1', 'host2', 1000)
        self.assert_(link1.get_bandwidth() == 1000)
        self.assert_(link1.get_end_points() == ('host1', 'host2'))
        self.assert_(not link1.get_flows())

class TestLink2(unittest.TestCase):
    def test_link(self):
        link1 = Link('host1', 'host2', 1000)
        new_bw = 100
        link1.set_bandwidth(new_bw)
        self.assert_(link1.get_bandwidth() == new_bw)

if __name__ == '__main__':
    unittest.main()
