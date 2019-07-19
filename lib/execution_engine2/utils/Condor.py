import json
from collections import namedtuple
from configparser import ConfigParser

import htcondor

from execution_engine2.utils.Scheduler import Scheduler


class Condor(Scheduler):
    job_info = namedtuple("job_info", "info error")
    submission_info = namedtuple("submission_info", "clusterid submit error")
    job_resource = namedtuple("job_resource", "amount unit")

    # TODO: Should these be outside of the class?
    REQUEST_CPUS = "request_cpus"
    REQUEST_MEMORY = "request_memory"
    REQUEST_DISK = "request_disk"
    CG = "+CLIENTGROUP"
    EE2 = "execution_engine2"
    ENDPOINT = "kbase-endpoint"
    EXECUTABLE = "executable"
    AUTH_TOKEN = "KB_ADMIN_AUTH_TOKEN"
    DOCKER_TIMEOUT = "docker_timeout"
    POOL_USER = "condor_pool"

    DEFAULT_CLIENT_GROUP = "default_client_group"

    def __init__(self, config_filepath):
        self.config = ConfigParser()
        self.config.read(config_filepath)
        self.ee_endpoint = self.config.get(section=self.EE2, option=self.ENDPOINT)
        self.executable = self.config.get(section=self.EE2, option=self.EXECUTABLE)
        self.kb_auth_token = self.config.get(section=self.EE2, option=self.AUTH_TOKEN)
        self.docker_timeout = self.config.get(
            section=self.EE2, option=self.DOCKER_TIMEOUT, fallback="604801"
        )
        self.pool_user = self.config.get(
            section=self.EE2, option=self.POOL_USER, fallback="condor_pool"
        )

    def get_default_client_group_and_requirements(self, client_group):
        """
        Search the config file for default client groups and requirements
        :param client_group: The section of the config file to search for requirements
        :return: The default requirements for that client group, or for no client group provided
        """
        default_resources = dict()
        if client_group in self.config.sections():
            section = client_group
            default_resources[self.CG] = client_group
        else:
            section = self.config["DEFAULT"][self.DEFAULT_CLIENT_GROUP]
            default_resources[self.CG] = section

        for item in [self.REQUEST_CPUS, self.REQUEST_DISK, self.REQUEST_MEMORY]:
            default_resources[item] = self.config.get(section=section, option=item)

        return default_resources

    def get_client_group_and_requirements(self, cgr, json_input=False):
        """
          Example CGR string = njs,required_cpus=1,required_mem=5
        :param cgr:
        :param json_input:
        :return:
        """

        reqs = dict()
        client_group = None
        if json_input is False:
            cgr_split = cgr.split(",")  # List
            client_group = cgr_split.pop(0)
            requirements = {self.CG: client_group}
            for item in cgr_split:
                (req, value) = item.split("=")
                requirements[req] = value
            reqs = requirements
        else:
            reqs = json.loads(cgr)

        default_requirements = self.get_default_client_group_and_requirements(
            client_group
        )
        for key, value in default_requirements.items():
            if key not in reqs or reqs[key].strip() is "":
                reqs[key] = value

        return reqs

    def cleanup_submit_file(self, submit_filepath):
        pass

    def setup_environment_vars(self, params):
        # 7 day docker job timeout default, Catalog token used to get access to volume mounts
        environment_vars = {
            "DOCKER_JOB_TIMEOUT": self.docker_timeout,
            "KB_ADMIN_AUTH_TOKEN": self.kb_auth_token,
            "KB_AUTH_TOKEN": params.get("token"),
            "CLIENTGROUP": params.get("clientgroup"),
            "JOB_ID": params.get("job_id"),
            # "WORKDIR": f"{config.get('WORKDIR')}/{params.get('USER')}/{params.get('JOB_ID')}",
            "CONDOR_ID": "$(Cluster).$(Process)",
        }

        environment = ""
        for key, val in environment_vars.items():
            environment += f"{key}={val} "

        return environment

    @staticmethod
    def validate_params(params):
        # TODO: Should we check them here or before?
        for item in ("token", "user", "job_id", "client_group_and_requirements"):
            if item not in params:
                raise Exception(f"{item} not found in params")

    def create_submit_file(self, params):
        self.validate_params(params)
        sub = dict()
        sub["executable"] = self.executable
        sub["arguments"] = " ".join([params.get("job_id"), self.ee_endpoint])
        sub["environment"] = self.setup_environment_vars(params)
        sub["universe"] = "vanilla"
        sub["+AccountingGroup"] = params.get("user")
        sub["Concurrency_Limits"] = params.get("user")
        sub["+Owner"] = self.pool_user
        sub["ShouldTransferFiles"] = "YES"
        sub["When_To_Transfer_Output"] = "ON_EXIT"


        cg_and_params = self.get_client_group_and_requirements(
            cgr=params["client_group_and_requirements"]
        )
        print(cg_and_params)
        for key, value in cg_and_params.items():
            sub[key] = value

        return sub

    def run_job(self, params, submit_file=None):
        """
       TODO: Add a retry
        TODO: Add list of required params
        :param params:  Params to run the job, such as the username, job_id, token, client_group_and_requirements
        :param submit_file:
        :return:
        """
        if submit_file is None:
            condor_submit = htcondor.Submit(self.create_submit_file(params))
        else:
            condor_submit = htcondor.Submit(submit_file)
            condor_submit['+OWNER'] = ''

        return self.run_condor_submit(condor_submit)



    #TODO add to pyi
    def run_condor_submit(self, condor_submit):
        try:
            schedd = htcondor.Schedd()
            with schedd.transaction() as txn:
                return self.submission_info(
                    clusterid=condor_submit.queue(txn, 1), submit=condor_submit, error=None
                )
        except Exception as e:
            return self.submission_info(None, submit=condor_submit, error=e)

    def get_job_info(self, batch_name=None, cluster_id=None):
        if batch_name is not None and cluster_id is not None:
            return self.job_info(
                info={},
                error=Exception("Please use only batch name or cluster_id, not both"),
            )

        constraint = None
        if batch_name:
            constraint = f"JobBatchName=?={batch_name}"
        if cluster_id:
            constraint = f"ClusterID=?={cluster_id}"

        try:
            job = htcondor.Schedd().query(constraint=constraint, limit=1)[0]
            return self.job_info(info=job, error=None)
        except Exception as e:
            return self.job_info(info={}, error=e)

    def get_user_info(self, user_id, projection=None):
        pass

    def cancel_job(self, job_id):
        pass
