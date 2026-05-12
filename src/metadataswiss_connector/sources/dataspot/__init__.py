from metadataswiss_connector.registry import CatalogSource, ResourceSpec
from metadataswiss_connector.sources.dataspot.constants import TRANSFORM_VERSION
from metadataswiss_connector.sources.dataspot.source import dataspot_source
from metadataswiss_connector.sources.dataspot.transform import (
    transform_to_concept,
    transform_to_dcat,
)

dataspot_catalog_source = CatalogSource(
    name="dataspot",
    dlt_source_factory=dataspot_source,
    resources={
        "data_products": ResourceSpec(transform_to_dcat, kind="dataset"),
        "code_lists": ResourceSpec(transform_to_concept, kind="concept"),
    },
    transform_version=TRANSFORM_VERSION,
)

__all__ = ["dataspot_source", "dataspot_catalog_source"]
