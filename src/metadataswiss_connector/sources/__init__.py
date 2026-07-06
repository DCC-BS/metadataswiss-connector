"""Registry of all catalog sources known to the connector.

Add a new source by importing its ``CatalogSource`` and appending it
here. The CLI runner iterates over ``SOURCES``.
"""

from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.sources.dataspot import dataspot_catalog_source

SOURCES: list[CatalogSource] = [
    dataspot_catalog_source,
]

__all__ = ["SOURCES"]
