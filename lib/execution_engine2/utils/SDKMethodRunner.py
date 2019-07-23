import logging
import re
from datetime import datetime

from execution_engine2.utils.MongoUtil import MongoUtil
from execution_engine2.models.models import Job, JobOutput, JobInput

from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace


class SDKMethodRunner:
    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """

        pattern = re.compile(".*\..*")
        if not pattern.match(method):
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

    def __init__(self, config):

        self.mongo_util = MongoUtil(config)

        catalog_url = config["catalog-url"]
        self.catalog = Catalog(catalog_url)

        workspace_url = config["workspace-url"]
        self.workspace = Workspace(workspace_url)

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.INFO
        )

    def run_job(self, params, user_id):

        method = params.get("method")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(params.get("source_ws_objects"))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(user_id, params)

        return job_id
