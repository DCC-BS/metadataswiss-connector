from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.sources.dataspot.source import dataspot_source
from metadataswiss_connector.sources.dataspot.transform import (
    TRANSFORM_VERSION,
    transform_to_dcat,
)

dataspot_catalog_source = CatalogSource(
    name="dataspot",
    dlt_source_factory=dataspot_source,
    resources={"data_products": transform_to_dcat},
    transform_version=TRANSFORM_VERSION,
)

__all__ = ["dataspot_source", "dataspot_catalog_source"]
