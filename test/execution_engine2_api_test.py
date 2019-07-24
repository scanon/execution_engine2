# -*- coding: utf-8 -*-
import logging
import unittest

logging.basicConfig(level=logging.INFO)

from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner
from configparser import ConfigParser


class ExecutionEngine2SchedulerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config_parser = ConfigParser()
        cls.config_parser.read("deploy.cfg")
        cls.config = {}
        cls.config["mongo-host"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-host"
        )
        cls.config["mongo-database"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-database"
        )
        cls.config["mongo-user"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-user"
        )
        cls.config["mongo-password"] = cls.config_parser.get(
            section="execution_engine2", option="mongo-password"
        )

        cls.config["mongo-collection"] = None
        cls.config["mongo-port"] = 27017
        cls.config["mongo-authmechanism"] = "DEFAULT"
        cls.config["start-local-mongo"] = 0

        cls.config["catalog-url"] = cls.config_parser.get(
            section="execution_engine2", option="catalog-url"
        )

        cls.config["workspace-url"] = cls.config_parser.get(
            section="execution_engine2", option="workspace-url"
        )

        cls.ctx = {"job_id": "test", "user_id": "test", "token": "test"}

    def _create_sample_params(self):
        params = dict()
        params["job_id"] = self.job_id
        params["user_id"] = "kbase"
        params["token"] = "test_token"
        params["client_group_and_requirements"] = "njs"
        return params
