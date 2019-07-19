# -*- coding: utf-8 -*-
import unittest

import htcondor

from lib.execution_engine2.utils.Condor import Condor


# from nose.plugins.attrib import attr


class CondorSchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.condor = Condor('deploy.cfg')
        cls.job_id = ""
        cls.submit_some_jobs()

    # @attr("Integration")
    def submit_some_jobs(self):
        print("Submit a sleep job")
        schedd = htcondor.Schedd()
        sub = htcondor.Submit({})
        sub["executable"] = "/bin/sleep"
        sub["arguments"] = "5m"
        sub["jobbatchname"] = "boris"
        with schedd.transaction() as txn:
            sub.queue(txn, 10)
        print("Done scheduling jobs")

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create_scheduler(self):
        print("Ok")
        scheduler = self.condor
        print(scheduler.get_job_info())


print(1)
