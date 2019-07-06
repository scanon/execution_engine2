# -*- coding: utf-8 -*-
import os
import unittest
from configparser import ConfigParser
import inspect

from execution_engine2.authclient import KBaseAuth as _KBaseAuth
from execution_engine2.utils.SDKMethodRunner import SDKMethodRunner


class SDKMethodRunner_test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.token = os.environ.get('KB_AUTH_TOKEN', None)
        config_file = os.environ.get('KB_DEPLOYMENT_CONFIG', None)
        cls.cfg = {}
        config = ConfigParser()
        config.read(config_file)
        for nameval in config.items('execution_engine2'):
            cls.cfg[nameval[0]] = nameval[1]

        # Getting username from Auth profile for token
        authServiceUrl = cls.cfg['auth-service-url']
        auth_client = _KBaseAuth(authServiceUrl)
        cls.user_id = auth_client.get_user(cls.token)

        cls.method_runner = SDKMethodRunner(cls.cfg)

    def getRunner(self):
        return self.__class__.method_runner

    def start_test(self):
        testname = inspect.stack()[1][3]
        print('\n*** starting test: ' + testname + ' **')

    def test_init_ok(self):
        self.start_test()
        class_attri = ['catalog']
        runner = self.getRunner()
        self.assertTrue(set(class_attri) <= set(runner.__dict__.keys()))

    def test_get_client_groups(self):
        self.start_test()

        runner = self.getRunner()

        client_groups = runner._get_client_groups('kb_uploadmethods.import_sra_from_staging')

        expected_groups = ['kb_upload']  # expected to fail if catalog is updated
        self.assertCountEqual(expected_groups, client_groups)

        client_groups = runner._get_client_groups('MEGAHIT.run_megahit')
        self.assertEqual(0, len(client_groups))

        with self.assertRaises(ValueError) as context:
            runner._get_client_groups('kb_uploadmethods')

        self.assertIn('unrecognized method:', str(context.exception.args))
