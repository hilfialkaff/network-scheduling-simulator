import unittest

from flow import Flow

"""
Test the flow object by itself
"""
class TestFlow1(unittest.TestCase):
    def test_flow(self):
        flow1 = Flow('host1', 'host2', 1000)
        self.assert_(flow1.get_requested_bandwidth() == 1000)
        self.assert_(flow1.get_end_points() == ('host1', 'host2'))
        self.assert_(not flow1.get_path())

if __name__ == '__main__':
    unittest.main()
