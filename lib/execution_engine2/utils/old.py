def process_old_format(self, cg_resources_requirements):
    """
    Old format is njs,request_cpu=1,request_memory=1,request_disk=1,request_color=blue
    Regex is assumed to be true

    :param cg_resources_requirements:
    :return:
    """
    cg_res_req_split = cg_resources_requirements.split(",")  # List

    # Access and remove clientgroup from the statement
    client_group = cg_res_req_split.pop(0)

    requirements = dict()
    for item in cg_res_req_split:
        (req, value) = item.split("=")
        requirements[req] = value

    # Set up default resources
    resources = self.get_default_resources(client_group)

    if client_group is None or client_group is "":
        client_group = resources[self.CG]

    requirements_statement = []

    for key, value in requirements.items():
        if key in resources:
            # Overwrite the resources with catalog entries
            resources[key] = value
        else:
            # Otherwise add it to the requirements statement
            requirements_statement.append(f"{key}={value}")

    # Delete special keys
    print(resources)
    print(requirements)

    del requirements[self.REQUEST_MEMORY]
    del requirements[self.REQUEST_CPUS]
    del requirements[self.REQUEST_DISK]

    # Set the clientgroup just in case it was blank

    # Add clientgroup to resources because it is special
    # Regex is enabled by default
    cge = f'regexp("{client_group}",CLIENTGROUP)'
    requirements_statement.append(cge)

    rv = dict()
    rv[self.CG] = client_group
    rv["client_group_expression"] = cge
    rv["requirements"] = "".join(requirements_statement)
    rv["requirements_statement"] = cge
    for key, value in resources.items():
        rv[key] = value

    return rv


def process_new_format(self, client_group_and_requirements):
    """
    New format is {'client_group' : 'njs', 'request_cpu' : 1, 'request_disk' :
    :param client_group_and_requirements:
    :return:
    """
    reqs = json.loads(client_group_and_requirements)

    def generate_requirements(self, cg_resources_requirements):
        print(cg_resources_requirements)
        if "{" in cg_resources_requirements:
            reqs = self.process_new_format(cg_resources_requirements)
        else:
            reqs = self.process_old_format(cg_resources_requirements)

        self.check_for_missing_requirements(reqs)

        return self.resource_requirements(
            request_cpus=reqs["request_cpus"],
            request_disk=reqs["request_disk"],
            request_memory=reqs["request_memory"],
            requirements_statement=reqs["requirements"],
        )
        return r

    @staticmethod
    def check_for_missing_requirements(requirements):
        for item in (
            "client_group_expression",
            "request_cpus",
            "request_disk",
            "request_memory",
        ):
            if item not in requirements:
                raise MissingCondorRequirementsException(
                    f"{item} not found in requirements"
                )

    def _process_requirements_new_format(self, requirements):
        requirements = dict()
        cg = requirements.get("client_group", "")
        if cg is "":
            # requirements[

            if bool(requirements.get("regex", False)) is True:
                cg["client_group_requirement"] = f'regexp("{cg}",CLIENTGROUP)'
            else:
                cg["client_group_requirement"] = f"+CLIENTGROUP == {client_group} "
