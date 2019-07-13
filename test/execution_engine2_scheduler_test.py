# -*- coding: utf-8 -*-
import unittest

import htcondor
import classad
import logging
import pwd
import os

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor


class execution_engine2Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.condor = Condor()

        cls.check_kbase_user()
        cls.schedd = htcondor.Schedd()
        if cls.queue_is_empty():
            cls.add_a_sleep_job()
        jobs = cls.queue_status()
        for job in jobs:
            logging.info(f"clusterid {job.get('ClusterId')} running cmd ( {job.get('Cmd')} ) is in state {job.get('JobStatus')}")





    @classmethod
    def add_a_sleep_job(cls):
        logging.info("Adding a sleep job")
        sub = htcondor.Submit({})
        sub['executable'] = '/bin/sleep'
        sub['arguments'] = '5m'
        sub['user'] = 'kbase'
        with cls.schedd.transaction() as txn:
            print(sub.queue(txn, 1))

    @classmethod
    def check_kbase_user(cls):
        my_uid = os.getuid()
        kbase_uid = pwd.getpwnam("kbase").pw_uid
        if my_uid != kbase_uid:
            logging.error(f"I'm not the KBASE User. My UID is {my_uid}. The KBASE uid is {kbase_uid}")
            raise Exception
        logging.info("Success. I'm the KBASE User")


    @classmethod
    def queue_is_empty(cls):
        if len( cls.schedd.query(limit=1)) == 0:
            logging.info("Queue is empty")
            return True
        return False

    @classmethod
    def queue_status(cls):
        return cls.schedd.query()


    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'wsName'):
            cls.wsClient.delete_workspace({'workspace': cls.wsName})
            print('Test workspace was deleted')

    def test_create_scheduler(self):

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
