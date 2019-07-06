import logging
import re

from installed_clients.CatalogClient import Catalog


class SDKMethodRunner:

    def _get_client_groups(self, method):
        """
        get client groups info from Catalog
        """

        pattern = re.compile(".*\..*")
        if not pattern.match(method):
            raise ValueError('unrecognized method: {}. Please input module_name.function_name'.format(method))

        module_name, function_name = method.split('.')

        group_config = self.catalog.list_client_group_configs({'module_name': module_name,
                                                               'function_name': function_name})

        if group_config:
            client_groups = group_config[0].get('client_groups')
        else:
            client_groups = list()

        return client_groups

    def __init__(self, config):

        catalog_url = config['catalog-url']
        self.catalog = Catalog(catalog_url)

        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)

    def run_job(self, params):

        method = params.get('method')

        client_groups = self._get_client_groups(method)

        job_id = 'test_job_id'

        return job_id
