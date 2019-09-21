from typing import List, Set
from installed_clients.authclient import (
    KBaseAuth,
    TokenCache
)
import logging
import requests
from json import JSONDecodeError

IS_ADMIN = "EE2_ADMIN"
NOT_ADMIN = "NOT_ADMIN"


class AuthUtil(object):
    def __init__(self, auth_url: str, admin_roles: List):
        # Use the token cache from the service's authclient, but put in booleans for whether the
        # token's account is an admin or not.
        self.__admin_cache = TokenCache()
        self.auth_url = auth_url
        self.admin_roles = set(admin_roles)

    def is_admin(self, token: str) -> bool:
        if not token:
            raise ValueError("Must supply token to check privileges")
        is_admin = self.__admin_cache.get_user(token)
        if is_admin is not None:
            return is_admin == IS_ADMIN

        roles = self._fetch_user_roles(token)
        if self.admin_roles.intersection(set(roles)):
            self.__admin_cache.add_valid_token(token, IS_ADMIN)
        else:
            self.__admin_cache.add_valid_token(token, NOT_ADMIN)

    def _fetch_user_roles(self, token: str) -> Set:
        if not token:
            raise ValueError("Must supply a token to fetch user roles")
        try:
            ret = requests.get(self.auth_url + "/api/V2/me", headers={"Authorization": token})
            ret.raise_for_status()  # this is a no-op if all is well
            user_info = ret.json()
            return set(user_info.get('customroles', []))
        except requests.HTTPError as e:
            err_msg = dict()
            try:
                err_msg = e.response.json().get('error', {})
            except JSONDecodeError:
                pass
            raise ValueError("An error occurred while fetching user roles from auth service: {} {}\n{}".format(
                e.response.status_code,
                e.response.reason,
                err_msg.get('message', 'Unknown Auth error')
            ))
