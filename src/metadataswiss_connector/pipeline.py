"""Framework-free per-source / per-resource pipeline helpers.

These wrap the three logical steps (extract, transform, publish) so they
can be invoked from the CLI and from an orchestrator (e.g. Dagster)
alike. The CLI runs them in source-wide loops; Dagster materialises one
resource at a time.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from metadataswiss_connector.config import I14YConfig
from metadataswiss_connector.dcat.duckdb_io import read_transformed
from metadataswiss_connector.dcat.transforms import run_transform_resource
from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.resources import (
    duckdb_destination,
    i14y_client_from_env,
    raw_pipeline_for,
)
from metadataswiss_connector.sync import SyncResult, sync

logger = logging.getLogger(__name__)


# Directory holding the per-resource remote-ID state files. Override via
# ``CONNECTOR_DATA_DIR`` to put them on a persistent volume in a containerised
# deployment; defaults to ``data/state/`` in the working directory for local use.
STATE_DIR = Path(os.environ.get("CONNECTOR_DATA_DIR", "data/state"))


def state_path(source_name: str, resource_name: str) -> Path:
    """One state file per (source, resource) pair."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{source_name}_{resource_name}_ids.json"


def extract_source(source: CatalogSource) -> None:
    """Run the dlt extract for an entire source (all resources at once)."""
    pipeline = raw_pipeline_for(source)
    load_info = pipeline.run(source.dlt_source_factory())
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    for table, count in sorted(row_counts.items()):
        if not table.startswith("_dlt"):
            logger.info("extracted %s rows into %s", count, table)
    logger.info("%s", load_info)


def transform_resource(source: CatalogSource, resource_name: str) -> dict:
    """Transform raw rows of a single resource into the i14y_dcat dataset.

    Returns the stats dict from ``run_transform_resource`` so Dagster
    assets (and any other caller) can surface row counts and validation
    rate as materialization metadata.
    """
    spec = source.resources[resource_name]
    cfg = I14YConfig.from_env()
    publisher = source.publisher or cfg.publisher
    return run_transform_resource(
        pipeline_raw=raw_pipeline_for(source),
        resource_name=resource_name,
        spec=spec,
        destination=duckdb_destination(),
        publisher=publisher,
    )


def publish_resource(
    source: CatalogSource,
    resource_name: str,
    *,
    limit: int | None = None,
    log: logging.Logger | None = None,
) -> SyncResult:
    """Publish transformed records of a single resource to I14Y.

    ``log`` is threaded into the I14Y client so per-request telemetry
    (method, status, duration, request id) shows up on the Dagster
    asset's run log when called from an asset.
    """
    spec = source.resources[resource_name]
    records = read_transformed(resource_name, limit=limit)
    if not records:
        (log or logger).warning(
            "no transformed records for %s.%s — skipping",
            source.name, resource_name,
        )
        return SyncResult()
    with i14y_client_from_env(logger=log) as client:
        return sync(
            client,
            records,
            kind=spec.kind,
            state_path=state_path(source.name, resource_name),
        )
