import dlt
from dlt.sources.rest_api import rest_api_source

from metadataswiss_connector.sources.dataspot.auth import DataspotAuth


def dataspot_source(
    base_url: str | None = None,
    database_name: str | None = None,
    exposed_client_id: str | None = None,
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    dataspot_access_key: str | None = None,
):
    """dlt source for the Dataspot metadata catalog REST API.

    Any argument left as ``None`` is resolved from dlt's config/secrets
    system (``.dlt/config.toml``, ``.dlt/secrets.toml``, or env vars under
    ``sources.dataspot.*``). Explicit arguments take precedence.
    """
    base_url = base_url or dlt.config["sources.dataspot.base_url"]
    database_name = database_name or dlt.config["sources.dataspot.database_name"]
    exposed_client_id = (
        exposed_client_id or dlt.config["sources.dataspot.exposed_client_id"]
    )
    tenant_id = tenant_id or dlt.secrets["sources.dataspot.tenant_id"]
    client_id = client_id or dlt.secrets["sources.dataspot.client_id"]
    client_secret = client_secret or dlt.secrets["sources.dataspot.client_secret"]
    dataspot_access_key = (
        dataspot_access_key or dlt.secrets["sources.dataspot.dataspot_access_key"]
    )

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
                    "paginator": "single_page",
                    "data_selector": "_embedded.datasets",
                },
                "processing_steps": [
                    {
                        "filter": lambda x: x["publicState"] == "PUBLIC"
                    },
                ],
            },
            {
                "name": "distributions",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "datasets/{resources.data_products.id}/distributions",
                    "paginator": "single_page",
                    "data_selector": "_embedded.distributions",
                    "response_actions": [
                        {"status_code": 404, "action": "ignore"},
                    ],
                },
                "processing_steps": [
                    {
                        "filter": lambda x: x["publicState"] == "PUBLIC"
                    },
                ],
                "include_from_parent": ["id"],
            },
        ],
    }

    return rest_api_source(config, name="dataspot")
