"""Dagster resources for the metadataswiss connector."""

import dlt

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
