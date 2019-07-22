# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.deploy = "deploy.cfg"
        cls.condor = Condor(cls.deploy)
        cls.job_id = "1234"
        cls.user = "kbase"

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def _create_sample_params(self):
        params = dict()
        params["job_id"] = self.job_id
        params["user_id"] = "kbase"
        params["token"] = "test_token"
        params["client_group_and_requirements"] = "njs"
        return params

    def test_empty_params(self):
        c = Condor("deploy.cfg")
        params = {"job_id": "test_job_id", ""user_id"": "test", "token": "test_token"}
        with self.assertRaisesRegex(
            Exception, "client_group_and_requirements not found in params"
        ) as error:
            c.create_submit(params)

    def test_create_submit_file(self):
        # Test with empty clientgroup
        c = Condor("deploy.cfg")
        params = self._create_sample_params()

        default_sub = c.create_submit(params)

        sub = default_sub
        self.assertEqual(sub["executable"], c.executable)
        self.assertEqual(sub["arguments"], f"{params['job_id']} {c.ee_endpoint}")
        self.assertEqual(sub["universe"], "vanilla")
        self.assertEqual(sub["+AccountingGroup"], params["user_id"])
        self.assertEqual(sub["Concurrency_Limits"], params["user_id"])
        self.assertEqual(sub["+Owner"], "condor_pool")
        self.assertEqual(sub["ShouldTransferFiles"], "YES")
        self.assertEqual(sub["When_To_Transfer_Output"], "ON_EXIT")
        # TODO test config here or otherplace
        self.assertEqual(sub["+CLIENTGROUP"], "njs")
        self.assertEqual(sub[Condor.REQUEST_CPUS], c.config["njs"][Condor.REQUEST_CPUS])
        self.assertEqual(
            sub[Condor.REQUEST_MEMORY], c.config["njs"][Condor.REQUEST_MEMORY]
        )
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        # TODO Test this variable somehow
        environment = sub["environment"].split(" ")

        # Test with filled out clientgroup

        params[
            "client_group_and_requirements"
        ] = "njs,request_cpus=8,request_memory=10GB,request_apples=5"
        logging.info(params)

        njs_sub = c.create_submit(params)
        sub = njs_sub

        self.assertEqual(sub["+CLIENTGROUP"], "njs")
        self.assertEqual(sub[Condor.REQUEST_CPUS], "8")
        self.assertEqual(sub[Condor.REQUEST_MEMORY], "10GB")
        self.assertEqual(sub[Condor.REQUEST_DISK], c.config["njs"][Condor.REQUEST_DISK])

        # Test with json version of clientgroup

        # TODO: Do online and offline tests
        # TODO Mock schedd, so it doesn't crash...
