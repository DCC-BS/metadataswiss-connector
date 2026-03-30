from dlt.sources.rest_api import rest_api_source

from .auth import DataspotAuth


def dataspot_source(
    base_url: str = None,
    database_name: str = None,
    exposed_client_id: str = None,
    tenant_id: str = None,
    client_id: str = None,
    client_secret: str = None,
    dataspot_access_key: str = None,
):
    """dlt source for the Dataspot metadata catalog REST API.

    Extracts metadata objects (data products, etc.) from a Dataspot instance.
    Config is resolved from function arguments, environment variables, or
    .dlt/config.toml and .dlt/secrets.toml.

    Args:
        base_url: Dataspot instance base URL (e.g. https://datenkatalog.bs.ch)
        database_name: Dataspot database name (e.g. prod)
        exposed_client_id: Azure AD exposed client ID for scope
        tenant_id: Azure AD tenant ID
        client_id: Azure AD client ID
        client_secret: Azure AD client secret
        dataspot_access_key: Dataspot service user access key
    """
    auth = DataspotAuth(
        access_token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data={"scope": f"api://{exposed_client_id}/.default"},
        dataspot_access_key=dataspot_access_key,
    )

    config = {
        "client": {
            "base_url": f"{base_url}/rest/{database_name}",
            "auth": auth,
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "data_products",
                "endpoint": {
                    "path": "schemes/Datenprodukte/datasets",
                },
            },
        ],
    }

    return rest_api_source(config, name="dataspot")
