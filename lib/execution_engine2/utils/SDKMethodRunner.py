import logging
import re
from datetime import datetime

from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.models.models import Job, JobOutput, JobInput

from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace
from execution_engine2.utils.Condor import Condor

from configparser import ConfigParser
import os

logging.basicConfig(level=logging.INFO)
import json


class SDKMethodRunner:
    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """
        if method is None:
            raise ValueError("Please input module_name.function_name")

        pattern = re.compile(".*\..*")
        if method is not None and not pattern.match(method):
            raise ValueError(
                "unrecognized method: {}. Please input module_name.function_name".format(
                    method
                )
            )

        module_name, function_name = method.split(".")

        group_config = self.catalog.list_client_group_configs(
            {"module_name": module_name, "function_name": function_name}
        )

        if group_config:
            client_groups = group_config[0].get("client_groups")
        else:
            client_groups = list()

        return client_groups

    def _check_ws_objects(self, source_objects):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.workspace.get_object_info3(
                {"objects": objects, "ignoreErrors": 1}
            )
            paths = info.get("paths")

            if None in paths:
                raise ValueError("Some workspace object is inaccessible")

    def _get_module_git_commit(self, method, service_ver=None):
        module_name = method.split(".")[0]

        if not service_ver:
            service_ver = "release"

        module_version = self.catalog.get_module_version(
            {"module_name": module_name, "version": service_ver}
        )

        git_commit_hash = module_version.get("git_commit_hash")

        return git_commit_hash

    def _init_job_rec(self, user_id, params):

        job = Job()
        output = JobOutput()
        inputs = JobInput()

        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get("wsid")
        job.creation_time = datetime.timestamp(job.created)

        inputs.wsid = job.wsid
        inputs.method = params.get("method")
        inputs.params = params
        inputs.service_ver = params.get("service_ver")
        inputs.app_id = params.get("app_id")

        job.job_input = inputs
        job.job_output = output

        insert_rec = self.mongo_util.insert_one(job.to_mongo())

        return str(insert_rec)

    def get_mongo_util(self):
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self):
        if self.condor is None:
            self.condor = self.condor = Condor(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        return self.condor

    def __init__(self, config):
        self.config = config
        self.mongo_util = None
        self.condor = None

        catalog_url = config["catalog-url"]
        self.catalog = Catalog(catalog_url)

        workspace_url = config["workspace-url"]
        self.workspace = Workspace(workspace_url)

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.INFO
        )

    def run_job(self, params, ctx):
        """

        :param params: RunJobParams object (See spec file)
        :param ctx: User_Id and Token from the request
        :return: The condor job id
        """

        method = params.get("method")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(params.get("source_ws_objects"))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(ctx["user_id"], params)

        logging.info("About to run job with")
        logging.info(params)
        logging.info(ctx)
        params["job_id"] = job_id
        params["user_id"] = ctx["user_id"]
        params["token"] = ctx["token"]
        params["cg_resources_requirements"] = client_groups
        try:
            condor_job_id = self.condor.run_job(params)
        except Exception as e:
            ## delete job from database? Or mark it to a state it will never run?
            raise e
            pass

        return job_id
