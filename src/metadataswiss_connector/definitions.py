"""Dagster definitions for the metadataswiss connector."""

from dagster import Definitions, load_assets_from_modules
from dagster_dlt import DagsterDltResource

from metadataswiss_connector.assets import dataspot, dcat, publish
from metadataswiss_connector.resources import I14YResource

defs = Definitions(
    assets=load_assets_from_modules([dataspot, dcat, publish]),
    resources={
        "dlt": DagsterDltResource(),
        "i14y": I14YResource(),
    },
)
