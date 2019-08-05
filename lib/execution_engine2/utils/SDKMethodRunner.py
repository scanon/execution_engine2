import json
import logging
import os
import re
from enum import Enum
from time import time

from mongoengine import connect
import datetime

from execution_engine2.models.models import Job, JobInput, Meta, JobLog
from execution_engine2.utils.Condor import Condor
from execution_engine2.utils.MongoUtil import MongoUtil
from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace

debug = json.loads(os.environ.get("debug", "False").lower())

if debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.WARN)


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
            client_groups = group_config[0].get("client_groups")[0]
        else:
            client_groups = ""

        return client_groups

    def _check_ws_objects(self, source_objects, ctx):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{"ref": ref} for ref in source_objects]
            info = self.get_workspace(ctx=ctx).get_object_info3(
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

        inputs = JobInput()

        job.user = user_id
        job.authstrat = "kbaseworkspace"
        job.wsid = params.get("wsid")
        job.status = "created"

        inputs.wsid = job.wsid
        inputs.method = params.get("method")
        inputs.params = params.get("params")
        inputs.service_ver = params.get("service_ver")
        inputs.app_id = params.get("app_id")

        # TODO Add Meta Fields From Params
        inputs.narrative_cell_info = Meta()

        job.job_input = inputs

        with self.get_mongo_util().me_collection(self.config["mongo-jobs-collection"]):
            job.save()

        return str(job.id)

    def get_mongo_util(self):
        if self.mongo_util is None:
            self.mongo_util = MongoUtil(self.config)
        return self.mongo_util

    def get_condor(self):
        if self.condor is None:
            self.condor = Condor(os.environ.get("KB_DEPLOYMENT_CONFIG"))
        return self.condor

    def get_workspace(self, ctx):
        if ctx is None:
            raise Exception("Need to provide credentials for the workspace")
        if self.workspace is None:
            self.workspace = Workspace(token=ctx["token"], url=self.workspace_url)
        return self.workspace

    class WorkspacePermissions(Enum):
        ADMINISTRATOR = "a"
        READ_WRITE = "w"
        READ = "r"
        NONE = "n"

    def connect_to_mongoengine(self):
        mu = self.get_mongo_util()
        connect(
            db=mu.mongo_database,
            host=mu.mongo_host,
            port=mu.mongo_port,
            authentication_source=mu.mongo_database,
            username=mu.mongo_user,
            password=mu.mongo_pass,
            alias="ee2",
        )

    def get_job_status(self, job_id, ctx):
        logging.debug(f"About to view logs for {job_id}")
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=False)
        logging.debug("Success, you have permission to view job status for " + job_id)
        return {}

    def _get_job_log(self, job_id, skip_lines):
        log = self.get_mongo_util().get_job_log(job_id)
        # if skip_lines #TODO
        """
            :returns: instance of type "GetJobLogsResults" (last_line_number -
           common number of lines (including those in skip_lines parameter),
           this number can be used as next skip_lines value to skip already
           loaded lines next time.) -> structure: parameter "lines" of list
           of type "LogLine" -> structure: parameter "line" of String,
           parameter "is_error" of type "boolean" (@range [0,1]), parameter
           "last_line_number" of Long
        """

        # TODO Filter the lines in the mongo query?

        log_obj = {"lines": {log.lines}, "last_line_number": 123}
        return log_obj

    def view_job_logs(self, job_id, skip_lines, ctx):
        logging.debug(f"About to view logs for {job_id}")
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=False)
        logging.debug("Success, you have permission to view logs for " + job_id)
        return self._get_job_log(job_id, skip_lines)

    def _append_log_lines(self, log, lines):
        return 1

    def _create_new_log(self, lines):
        return 1

    def add_job_logs(self, job_id, lines, ctx):
        logging.debug(f"About to add logs for {job_id}")
        self.check_permission_for_job(job_id=job_id, ctx=ctx, write=True)
        logging.debug("Success, you have permission to view logs for " + job_id)
        try:
            log = self.get_mongo_util().get_job_log(job_id)
            logging.debug(f"Found job log for {job_id}")
            line_count = self._append_log_lines(log, lines)
            logging.debug(f"Added log lines to {job_id}")
        except Exception:
            line_count = self._create_new_log(lines)
            logging.debug(f"Created new log for {job_id}")

        logging.debug(f"Line count is now {line_count}")
        return line_count

    def __init__(self, config):
        self.config = config
        self.mongo_util = None
        self.condor = None
        self.workspace = None
        catalog_url = config["catalog-url"]
        self.catalog = Catalog(catalog_url)

        self.workspace_url = config["workspace-url"]

        logging.basicConfig(
            format="%(created)s %(levelname)s: %(message)s", level=logging.debug
        )

    def status(self):
        return {"servertime": f"{time()}"}

    def run_job(self, params, ctx):
        """

        :param params: RunJobParams object (See spec file)
        :param ctx: User_Id and Token from the request
        :return: The condor job id
        """

        if not self._can_write_ws(
            self.get_permissions_for_workspace(wsid=params["wsid"], ctx=ctx)
        ):
            logging.debug("You don't have permission to run jobs in this workspace")

        method = params.get("method")

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_objects(source_objects=params.get("source_ws_objects"), ctx=ctx)

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get("service_ver"))
        params["service_ver"] = git_commit_hash

        # insert initial job document
        job_id = self._init_job_rec(ctx["user_id"], params)

        # TODO Figure out log level
        logging.debug("About to run job with")
        logging.debug(client_groups)
        logging.debug(params)
        logging.debug(ctx)
        params["job_id"] = job_id
        params["user_id"] = ctx["user_id"]
        params["token"] = ctx["token"]
        params["cg_resources_requirements"] = client_groups
        try:
            submission_info = self.get_condor().run_job(params)
            condor_job_id = submission_info.clusterid
            logging.debug("Submitted job id and got ")
            logging.debug(condor_job_id)
        except Exception as e:
            ## delete job from database? Or mark it to a state it will never run?
            logging.error(e)
            raise e

        if submission_info.error is not None or condor_job_id is None:
            raise (submission_info.error)

        logging.debug("Submission info is")
        logging.debug(submission_info)
        logging.debug(condor_job_id)
        logging.debug(type(condor_job_id))
        return condor_job_id

    def get_permissions_for_workspace(self, wsid, ctx):

        username = ctx["user_id"]
        logging.debug(f"Checking permissions for workspace {wsid} for {username}")
        ws = self.get_workspace(ctx)
        logging.debug(ws)

        perms = ws.get_permissions_mass({"workspaces": [{"id": wsid}]})["perms"]

        ws_permission = self.WorkspacePermissions.NONE
        for p in perms:
            if username in p:
                ws_permission = self.WorkspacePermissions(p[username])
        return ws_permission

    @staticmethod
    def _can_read_ws(p):
        read_permissions = [
            SDKMethodRunner.WorkspacePermissions.ADMINISTRATOR,
            SDKMethodRunner.WorkspacePermissions.READ_WRITE,
            SDKMethodRunner.WorkspacePermissions.READ,
        ]
        return p in read_permissions

    @staticmethod
    def _can_write_ws(p):
        write_permissions = [
            SDKMethodRunner.WorkspacePermissions.ADMINISTRATOR,
            SDKMethodRunner.WorkspacePermissions.READ_WRITE,
        ]
        return p in write_permissions

    def check_permission_for_job(self, job_id, ctx, write=False):
        """
        Check for permissions to modify or read this record, based on WSID associated with the record

        :param job_id: The job id to look up to get it's WSID
        :param ctx: The REQUEST
        :param write: Whether or not to check for Read Permissions or Write Permissions
        :return:
        """
        with self.get_mongo_util().mongo_engine_connection():
            logging.debug(f"Getting job {job_id}")
            job = Job.objects(id=job_id)[0]
            logging.debug(f"Got {job}")
            permission = self.get_permissions_for_workspace(wsid=job.wsid, ctx=ctx)
            if write is True:
                permitted = self._can_write_ws(permission)
            else:
                permitted = self._can_read_ws(permission)

            if not permitted:
                raise PermissionError(
                    f"User {ctx['user_id']} does not have permissions to get status for wsid:{job.wsid}, job_id:{job_id} permission{permission}"
                )
