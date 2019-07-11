# -*- coding: utf-8 -*-
import unittest

import htcondor
import classad

from execution_engine2.utils.Condor import Condor


class execution_engine2Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.condor = Condor()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def test_create_scheduler(self):
        print("Ok")
        scheduler = self.condor
        schedd = htcondor.Schedd()
        #
        # sub = htcondor.Submit({})
        # sub['executable'] = '/bin/sleep'
        # sub['arguments'] = '5m'
        # with schedd.transaction() as txn:
        #     print(sub.queue(txn, 2))

        job_id = 1
         # type: classad.ClassAd

        print(job.printJson())
