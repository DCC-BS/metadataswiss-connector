"""Dagster assets for the Dataspot dlt source.

Uses ``@dlt_assets`` so each dlt resource (e.g. ``data_products``) becomes
its own Dagster asset, with row counts and load info reported automatically.
"""

from dagster import AssetExecutionContext, AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from metadataswiss_connector.resources import dataspot_raw_pipeline
from metadataswiss_connector.sources.dataspot import dataspot_source

DATASPOT_GROUP = "dataspot"


class DataspotDltTranslator(DagsterDltTranslator):
    """Map each dlt resource to a stable, predictable AssetKey."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        spec = super().get_asset_spec(data)
        # Drop the synthetic "source" parent key that dlt's default deps fn
        # injects for non-transformer resources — the dlt source is the root
        # here, not an upstream Dagster asset.
        return spec.replace_attributes(
            key=AssetKey([DATASPOT_GROUP, data.resource.name]),
            deps=[],
        )


def dataspot_resource_asset_key(resource_name: str) -> AssetKey:
    """Public helper so downstream assets can declare deps without guessing."""
    return AssetKey([DATASPOT_GROUP, resource_name])


@dlt_assets(
    dlt_source=dataspot_source(),
    dlt_pipeline=dataspot_raw_pipeline(),
    name="dataspot",
    group_name=DATASPOT_GROUP,
    dagster_dlt_translator=DataspotDltTranslator(),
)
def dataspot_dlt_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)
