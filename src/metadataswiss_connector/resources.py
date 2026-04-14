"""Dagster resources for the metadataswiss connector."""

import os

import dlt
from dagster import ConfigurableResource, EnvVar

from i14y_client import I14YAuth, I14YClient

DUCKDB_PATH = "dataspot.duckdb"


def duckdb_destination():
    """Shared DuckDB destination used by raw and DCAT pipelines."""
    return dlt.destinations.duckdb(DUCKDB_PATH)


def dataspot_raw_pipeline() -> dlt.Pipeline:
    """The dlt pipeline that lands raw Dataspot data in DuckDB."""
    return dlt.pipeline(
        pipeline_name="dataspot",
        destination=duckdb_destination(),
        dataset_name="dataspot_raw",
    )


class I14YResource(ConfigurableResource):
    """Dagster resource that provides an authenticated I14Y API client.

    Configure via environment variables:
      - I14Y_BASE_URL
      - I14Y_TOKEN_URL
      - I14Y_CLIENT_ID
      - I14Y_CLIENT_SECRET
      - I14Y_PUBLISHER
    """

    base_url: str = os.getenv("I14Y_BASE_URL", "https://api-a.i14y.admin.ch/api/partner/v1")
    token_url: str = os.getenv("I14Y_TOKEN_URL", "https://identity.i14y.a.c.bfs.admin.ch/realms/bfs-sis-a/protocol/openid-connect/token")
    client_id: str = os.getenv("I14Y_CLIENT_ID", "")
    client_secret: str = os.getenv("I14Y_CLIENT_SECRET", "")
    publisher: str = os.getenv("I14Y_PUBLISHER", "")

    def get_client(self) -> I14YClient:
        auth = I14YAuth(
            token_url=self.token_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return I14YClient(base_url=self.base_url, auth=auth)
