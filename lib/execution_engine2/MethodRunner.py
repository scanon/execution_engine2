import os
from configparser import ConfigParser
from typing import Dict

from lib.execution_engine2.exceptions import *
from lib.installed_clients.UserAndJobStateClient import UserAndJobState


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
        if 'job_id' not in params:
            raise MissingParamsException()
        job_id = params['job_id'].strip()
        if len(job_id) < self.JOB_ID_LENGTH:
            raise MalformedJobIdException()
        ujs = UserAndJobState(token=self.ctx['token'],
                              url=self.parser.get(self.NJS, "jobstatus.srv.url"))

        return ujs.get_job_status(job_id)

    def check_job_permissions(self):
        pass
