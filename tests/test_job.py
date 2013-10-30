import unittest

from job import Job

class TestJob1(unittest.TestCase):
    def setUp(self):
        self.job = Job('job0', 9, 9, 1762, 0, 14347)

    def test_job(self):
        self.assert_(self.job.get_state() == Job.NOT_EXECUTED)
        self.assert_(self.job.get_id() == 'job0')
        self.assert_(self.job.get_submit_time() == 9)
        self.assert_(self.job.get_inter_job_diff() == 9)
        self.assert_(self.job.get_map_size() == 1762)
        self.assert_(self.job.get_shuffle_size() == 0)
        self.assert_(self.job.get_reduce_size() == 14347)

class TestJob2(unittest.TestCase):
    def setUp(self):
        self.job = Job('job0', 9, 9, 0, 10000, 0)

    def test_job(self):
        self.assert_(self.job.get_data_left() == 10000)

        for i in range(1, 100):
            data_left = self.job.update_data_left(100)
            self.assert_(data_left == 10000 - 100 * i)

if __name__ == '__main__':
    unittest.main()
