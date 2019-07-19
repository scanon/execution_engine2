def _create_sample_params(self):
    params = dict()
    params["job_id"] = self.job_id
    params["user"] = "kbase"
    params["token"] = "test_token"
    params["client_group_and_requirements"] = "njs"
    return params
