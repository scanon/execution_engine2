# -*- coding: utf-8 -*-
import os
import time
import unittest
from configparser import ConfigParser
import re

from execution_engine2.execution_engine2Impl import execution_engine2
from execution_engine2.execution_engine2Server import MethodContext
from execution_engine2.authclient import KBaseAuth as _KBaseAuth

from installed_clients.WorkspaceClient import Workspace


class execution_engine2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        token = os.environ.get("KB_AUTH_TOKEN", None)
        config_file = os.environ.get("KB_DEPLOYMENT_CONFIG", None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items("execution_engine2"):
            cls.cfg[nameval[0]] = nameval[1]
        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg["auth-service-url"]
        auth_client = _KBaseAuth(authServiceUrl)
        user_id = auth_client.get_user(token)
        # WARNING: don't call any logging methods on the context object,
        # it'll result in a NoneType error
        cls.ctx = MethodContext(None)
        cls.ctx.update(
            {
                "token": token,
                "user_id": user_id,
                "provenance": [
                    {
                        "service": "execution_engine2",
                        "method": "please_never_use_it_in_production",
                        "method_params": [],
                    }
                ],
                "authenticated": 1,
            }
        )
        cls.wsURL = cls.cfg["workspace-url"]
        cls.wsClient = Workspace(cls.wsURL)
        cls.serviceImpl = execution_engine2(cls.cfg)
        cls.scratch = cls.cfg["scratch"]
        cls.callback_url = os.environ["SDK_CALLBACK_URL"]
        suffix = int(time.time() * 1000)
        cls.wsName = "test_ContigFilter_" + str(suffix)
        cls.wsClient.create_workspace({'workspace': cls.wsName})

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "wsName"):
            cls.wsClient.delete_workspace({"workspace": cls.wsName})
            print("Test workspace was deleted")

    def test_status(self):
        status = self.serviceImpl.status(self.ctx)[0]

        self.assertTrue('servertime' in status)

    def test_ver(self):
        ver = self.serviceImpl.ver(self.ctx)[0]

        self.assertTrue(isinstance(ver, str))
        pattern = re.compile("\d\.\d\.\d")
        self.assertTrue(pattern.match(ver))
