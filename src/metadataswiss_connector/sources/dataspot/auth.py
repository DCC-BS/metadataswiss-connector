from requests import PreparedRequest

from dlt.common.configuration.specs.base_configuration import configspec
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials


@configspec
class DataspotAuth(OAuth2ClientCredentials):
    """Dataspot API authentication.

    Combines Azure AD M2M OAuth2 (client_credentials) with a custom
    dataspot-access-key header required by the Dataspot API.
    """

    dataspot_access_key: str = None

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request = super().__call__(request)
        request.headers["dataspot-access-key"] = self.dataspot_access_key
        return request
