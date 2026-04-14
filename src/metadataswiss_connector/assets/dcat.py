"""Downstream assets that transform raw Dataspot data into I14Y DCAT."""

from dagster import AssetExecutionContext, asset

from metadataswiss_connector.assets.dataspot import dataspot_resource_asset_key
from metadataswiss_connector.dcat.transforms import run_transform
from metadataswiss_connector.resources import (
    I14YResource,
    dataspot_raw_pipeline,
    duckdb_destination,
)
from metadataswiss_connector.sources.dataspot.transform import transform_to_dcat

# Resources of the Dataspot source that should be transformed to DCAT.
# Keep in sync with metadataswiss_connector/sources/dataspot/source.py.
DATASPOT_RESOURCES = ["data_products"]


@asset(
    group_name="i14y_dcat",
    deps=[dataspot_resource_asset_key(r) for r in DATASPOT_RESOURCES],
    required_resource_keys={"i14y"},
)
def dataspot_i14y_dcat(context: AssetExecutionContext) -> None:
    """Transform raw Dataspot tables in DuckDB to the I14Y DCAT dataset."""
    i14y_resource: I14YResource = context.resources.i14y
    run_transform(
        pipeline_raw=dataspot_raw_pipeline(),
        resource_names=DATASPOT_RESOURCES,
        transform_fn=transform_to_dcat,
        destination=duckdb_destination(),
        publisher=i14y_resource.publisher,
    )
    context.log.info("DCAT transformation complete")
