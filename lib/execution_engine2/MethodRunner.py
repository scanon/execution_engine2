import json
import os
import time
from configparser import ConfigParser
from typing import Dict


class MethodRunner:
    """
    Maybe move this config loading to somewhere where the config is cached,
    init of upper level method?

    """

    JOB_ID_LENGTH = 24
    NJS = "NarrativeJobService"

    @staticmethod
    def _get_config() -> ConfigParser:
        parser = ConfigParser()
        parser.read(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        return parser

    def __init__(self, ctx):
        self.parser = self._get_config()
        self.njs_db_name = self.parser.get("NarrativeJobService", "mongodb-database")
        self.njs_jobs_collection_name = "exec_tasks"
        self.njs_logs_collection_name = "exec_logs"
        self.ctx = ctx

    def check_job_cancelled(self, params: Dict[str, str]):
        return {
            'test': 'test'
        }

    def status(self):
        return json(
            {
                "servertime": f"{time.time()}",
            })

    def check_job_permissions(self):
        pass
