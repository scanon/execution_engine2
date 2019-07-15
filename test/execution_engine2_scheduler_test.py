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
        sub["user"] = "kbase"
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
        c = Condor("deploy.cfg")
        params = {
            "job_id": "test_job_id",
            "user": "test",
            "client_group_and_requirements": "",
        }
        config = {
            "executable": '"/kb/deployment/misc/sdklocalmethodrunner.sh"',
            "kbase-endpoint": "{{ kbase_endpoint }}",
        }

        default_sub = c.create_submit_file(params, config)

        sub = default_sub
        self.assertEqual(sub["executable"], config["executable"])
        self.assertEqual(
            sub["arguments"], f"{params['job_id']} {config['kbase-endpoint']}"
        )
        self.assertEqual(sub["universe"], "vanilla")
        self.assertEqual(sub["+AccountingGroup"], params["user"])
        self.assertEqual(sub["Concurrency_Limits"], params["user"])
        self.assertEqual(sub["+Owner"], "condor_pool")
        self.assertEqual(sub["ShouldTransferFiles"], "YES")
        self.assertEqual(sub["When_To_Transfer_Output"], "ON_EXIT")
        # TODO test config here or otherplace
        self.assertEqual(sub["+CLIENTGROUP"], "njs")
        self.assertEqual(sub[Condor.REQUEST_CPUS], c.config['njs'][Condor.REQUEST_CPUS])
        self.assertEqual(sub[Condor.REQUEST_MEMORY], c.config['njs'][Condor.REQUEST_MEMORY])
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config['njs'][Condor.REQUEST_DISK])


        params = {
            "job_id": "test_job_id",
            "user": "test",
            "client_group_and_requirements": "njs,request_cpus=8,request_memory=10GB,request_apples=5",
        }
        config = {
            "executable": '"/kb/deployment/misc/sdklocalmethodrunner.sh"',
            "kbase-endpoint": "{{ kbase_endpoint }}",
        }


        njs_sub = c.create_submit_file(params, config)
        sub = njs_sub

        self.assertEqual(sub["+CLIENTGROUP"], "njs")
        self.assertEqual(sub[Condor.REQUEST_CPUS], '8')
        self.assertEqual(sub[Condor.REQUEST_MEMORY], '10GB')
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config['njs'][Condor.REQUEST_DISK])

        print(sub)
        (
            "environment",
            "DOCKER_JOB_TIMEOUT=604800 KB_ADMIN_AUTH_TOKEN=None KB_AUTH_TOKEN=None CLIENTGROUP=None JOB_ID=None WORKDIR=None/None/None CONDOR_ID=$(Cluster).$(Process) ",
        )
