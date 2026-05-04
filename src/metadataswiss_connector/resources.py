"""Shared dlt + I14Y client helpers."""

from typing import TYPE_CHECKING

import dlt

from i14y_client import I14YAuth, I14YClient
from metadataswiss_connector.config import I14YConfig

if TYPE_CHECKING:
    from metadataswiss_connector.registry import CatalogSource

DUCKDB_PATH = "metadata.duckdb"


def duckdb_destination():
    """Shared DuckDB destination used by raw and DCAT pipelines."""
    return dlt.destinations.duckdb(DUCKDB_PATH)


def raw_pipeline_for(source: "CatalogSource") -> dlt.Pipeline:
    """The dlt pipeline that lands raw data for a given source in DuckDB."""
    return dlt.pipeline(
        pipeline_name=source.name,
        destination=duckdb_destination(),
        dataset_name=f"{source.name}_raw",
    )


def i14y_client_from_env() -> I14YClient:
    """Build an I14Y API client from environment-derived config."""
    cfg = I14YConfig.from_env()
    auth = I14YAuth(
        token_url=cfg.token_url,
        client_id=cfg.client_id,
        client_secret=cfg.client_secret,
    )
    return I14YClient(base_url=cfg.base_url, auth=auth, user_agent=cfg.user_agent)
