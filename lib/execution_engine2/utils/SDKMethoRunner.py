import logging


class SDKMethoRunner:

    def _get_client_groups(self, method):
        client_groups = ''

        return client_groups

    def __init__(self, config):

        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)

    def run_job(self, params):

        method = params.get('method')

        client_groups = self._get_client_groups(method)

        job_id = 'test_job_id'

        return job_id
