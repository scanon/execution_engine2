from collections import namedtuple

import htcondor

from execution_engine2.utils.Scheduler import Scheduler


class Condor(Scheduler):
    job_info = namedtuple("job_info", "info error")

    def cleanup_submit_file(self, submit_filepath):
        pass

    def submit_job(self, params, config):
        




    def run_submit_file(self, submit_filepath):
        pass

    def get_job_info(self, batch_name=None, cluster_id=None):
        if batch_name is not None and cluster_id is not None:
            return self.job_info(info={}, error=Exception(
                "Please use only batch name or cluster_id, not both"))

        constraint = None
        if batch_name:
            constraint = f'JobBatchName=?={batch_name}'
        if cluster_id:
            constraint = f'ClusterID=?={cluster_id}'

        try:
            job = htcondor.Schedd().query(constraint=constraint, limit=1)[0]
            return self.job_info(info=job, error=None)
        except Exception as e:
            return self.job_info(info={}, error=e)

    def get_user_info(self, user_id, projection=None):
        pass

    def cancel_job(self, job_id):
        pass
