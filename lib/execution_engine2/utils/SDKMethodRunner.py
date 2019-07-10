import logging
import re

from execution_engine2.utils.MongoUtil import MongoUtil

from installed_clients.CatalogClient import Catalog
from installed_clients.WorkspaceClient import Workspace


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

    def _check_ws_ojects(self, source_objects):
        """
        perform sanity checks on input WS objects
        """

        if source_objects:
            objects = [{'ref': ref} for ref in source_objects]
            info = self.workspace.get_object_info3({"objects": objects, 'ignoreErrors': 1})
            paths = info.get('paths')

            if None in paths:
                raise ValueError('Some workspace object is inaccessible')

    def _get_module_git_commit(self, method, service_ver=None):
        module_name = method.split('.')[0]

        if not service_ver:
            service_ver = 'release'

        module_version = self.catalog.get_module_version({'module_name': module_name,
                                                          'version': service_ver})

        git_commit_hash = module_version.get('git_commit_hash')

        return git_commit_hash

    def __init__(self, config):

        self.mongo_util = MongoUtil(config)

        catalog_url = config['catalog-url']
        self.catalog = Catalog(catalog_url)

        workspace_url = config['workspace-url']
        self.workspace = Workspace(workspace_url)

        logging.basicConfig(format='%(created)s %(levelname)s: %(message)s',
                            level=logging.INFO)

    def run_job(self, params):

        method = params.get('method')

        client_groups = self._get_client_groups(method)

        # perform sanity checks before creating job
        self._check_ws_ojects(params.get('source_ws_objects'))

        # update service_ver
        git_commit_hash = self._get_module_git_commit(method, params.get('service_ver'))
        params['service_ver'] = git_commit_hash

        job_id = 'test_job_id'
        print(client_groups)

        return job_id
