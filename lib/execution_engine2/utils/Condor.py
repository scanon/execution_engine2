from collections import namedtuple

import htcondor


from execution_engine2.utils.Scheduler import Scheduler


class Condor(Scheduler):
    job_info = namedtuple("job_info", "info error")

    def cleanup_submit_file(self, submit_filepath):
        pass

    def setup_environment_vars(self, params, config):
        # 7 day docker job timeout default, Catalog token used to get access to volume mounts
        environment_vars = {
            "DOCKER_JOB_TIMEOUT": config.get("DOCKER_JOB_TIMEOUT", "604800"),
            "KB_ADMIN_AUTH_TOKEN": config.get("KB_ADMIN_AUTH_TOKEN"),
            "KB_AUTH_TOKEN": params.get("TOKEN"),
            "CLIENTGROUP": params.get("CLIENTGROUP"),
            "JOB_ID": params.get("JOB_ID"),
            "WORKDIR": f"{config.get('WORKDIR')}/{params.get('USER')}/{params.get('JOB_ID')}",
            "CONDOR_ID": "$(Cluster).$(Process)",
        }

        environment = ""
        for key, val in environment_vars():
            environment += f"{key}={val}"

        return environment

    def submit_job(self, params, config):
        sub = htcondor.Submit({})
        sub["executable"] = config["executable"]
        sub["arguments"] = list(params.get("job_id"), config.get("endpoint"))
        sub["environment"] = self.setup_environment_vars(params, config)
        sub["universe"] = "vanilla"
        sub["+AccountingGroup"] = params.get("USER")
        sub["Concurrency_Limits"] = params.get("USER")
        sub["OWNER"] = config.get("POOL_USER", "condor_pool")
        sub["ShouldTransferFiles"] = config.get("SHOULD_TRANSFER_FILES", "YES")
        sub["When_To_Transfer_Output"] = config.get(
            "WHEN_TO_TRANSFER_OUTPUT", "ON_EXIT"
        )

        # with schedd.transaction() as txn:
        #     print(sub.queue(txn, 2))

    def run_submit_file(self, submit_filepath):
        pass

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
