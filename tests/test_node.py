import unittest

from node import Node

class TestNode1(unittest.TestCase):
    def test_node(self):
        node = Node(1)
        self.assert_(node.get_id() == 1)
        self.assert_(node.get_cpu() == 0)
        self.assert_(node.get_mem() == 0)
        self.assert_(node.get_links() == [])
        self.assert_(node.is_free())

        node.set_job_id_executed(2)
        self.assert_(node.get_job_id_executed() == 2)

class TestNode2(unittest.TestCase):
    def test_node(self):
        node = Node(1, 2, 3)
        self.assert_(node.get_id() == 1)
        self.assert_(node.get_cpu() == 2)
        self.assert_(node.get_mem() == 3)
        self.assert_(node.get_links() == [])
        self.assert_(node.is_free())

        node.set_job_id_executed(2)
        self.assert_(node.get_job_id_executed() == 2)

if __name__ == '__main__':
    unittest.main()
