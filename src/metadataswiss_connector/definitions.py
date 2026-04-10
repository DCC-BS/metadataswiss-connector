"""Dagster definitions for the metadataswiss connector."""

from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource

from metadataswiss_connector.assets import dataspot, dcat

defs = Definitions(
    assets=load_assets_from_modules([dataspot, dcat]),
    resources={"dlt": DagsterDltResource()},
)
