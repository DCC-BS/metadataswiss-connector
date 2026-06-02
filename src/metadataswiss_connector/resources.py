"""Shared dlt + I14Y client helpers."""

import logging
import os
from typing import TYPE_CHECKING

import dlt

from i14y_client import I14YAuth, I14YClient
from metadataswiss_connector.config import I14YConfig

if TYPE_CHECKING:
    from metadataswiss_connector.registry import CatalogSource

# Location of the local DuckDB warehouse. Override via ``DUCKDB_PATH`` to
# point at a persistent volume in a containerised deployment; defaults to a
# file in the working directory for local/CLI use.
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "data/metadata.duckdb")


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


def i14y_client_from_env(logger: logging.Logger | None = None) -> I14YClient:
    """Build an I14Y API client from environment-derived config.

    ``logger`` lets the caller (e.g. a Dagster asset passing
    ``context.log``) capture per-request telemetry alongside their own
    output. Defaults to the package logger.
    """
    cfg = I14YConfig.from_env()
    auth = I14YAuth(
        token_url=cfg.token_url,
        client_id=cfg.client_id,
        client_secret=cfg.client_secret,
    )
    kwargs: dict = {
        "base_url": cfg.base_url,
        "auth": auth,
        "user_agent": cfg.user_agent,
    }
    if logger is not None:
        kwargs["logger"] = logger
    return I14YClient(**kwargs)
