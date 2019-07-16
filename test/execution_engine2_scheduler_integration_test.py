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
        cls.deploy = "deploy.cfg"
        cls.condor = Condor(cls.deploy)
        cls.job_id = "1234"
        cls.user = "kbase"
        cls.token = "bogus"

        cls.check_kbase_user()
        cls.schedd = htcondor.Schedd()
        if cls.queue_is_empty():
            cls.add_a_sleep_job()
        jobs = cls.queue_status()
        for job in jobs:
            logging.info(
                f"clusterid {job.get('ClusterId')} running cmd ( {job.get('Cmd')} ) is in state {job.get('JobStatus')}"
            )

    @classmethod
    def add_a_sleep_job(cls):
        logging.info("Adding a sleep job")
        sub = htcondor.Submit({})
        sub["executable"] = "/bin/sleep"
        sub["arguments"] = "5m"
        sub["AccountingGroup"] = "kbase"
        with cls.schedd.transaction() as txn:
            print(sub.queue(txn, 1))

    @classmethod
    def check_kbase_user(cls):
        my_uid = os.getuid()
        kbase_uid = pwd.getpwnam("kbase").pw_uid
        if my_uid != kbase_uid:
            logging.error(
                f"I'm not the KBASE User. My UID is {my_uid}. The KBASE uid is {kbase_uid}"
            )
            raise Exception
        logging.info("Success. I'm the KBASE User")

    @classmethod
    def queue_is_empty(cls):
        if len(cls.schedd.query(limit=1)) == 0:
            logging.info("Queue is empty")
            return True
        return False

    @classmethod
    def queue_status(cls):
        return cls.schedd.query()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def _create_sample_params(self):
        params = dict()
        params["job_id"] = self.job_id



    def test_create_submit_file(self):
        # Test with empty clientgroup
        c = Condor("deploy.cfg")
        params = {
            "job_id": "test_job_id",
            "user": "test",
            "token": "test_token",
            "client_group_and_requirements": "",
        }
        # run_job = c.run_job(params)
        # print(run_job.clusterid)
        # print(run_job.error)

        #Remove OWNER
        sub_dict = c.create_submit_file(params)
        sub = htcondor.Submit({})
        with open("sub", 'w') as f:
            for key,value in sub_dict.items():
                if(key is not '+OWNER'):
                    sub[key] = value

        c.run_job(params, htcondor_submit=sub)




