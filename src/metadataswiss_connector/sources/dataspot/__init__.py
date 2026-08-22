from metadataswiss_connector.registry import CatalogSource, ResourceSpec
from metadataswiss_connector.sources.dataspot.source import (
    dataspot_source,
    drain_extract_skips,
)
from metadataswiss_connector.sources.dataspot.transform import (
    transform_to_concept,
    transform_to_dataservice,
    transform_to_dataset,
)

dataspot_catalog_source = CatalogSource(
    name="dataspot",
    dlt_source_factory=dataspot_source,
    drain_extract_skips=drain_extract_skips,
    resources={
        "data_products": ResourceSpec(
            transform_to_dataset,
            kind="dataset",
            # ``distributions`` resolves from the broad ``data_products_all``
            # extract resource, so its parent-ref column is
            # ``_data_products_all_id`` — not ``_data_products_id``.
            sibling_parent="data_products_all",
            lookups=(
                "distributions",
                "dataset_structure_components",
                "dataset_collection_path",
                "collection_data_owners",
                "collection_agencies",
                "collections",
                "kontaktstelle_agencies",
            ),
        ),
        "data_services": ResourceSpec(
            transform_to_dataservice,
            kind="dataservice",
            lookups=(
                "dataset_collection_path",
                "collection_data_owners",
                "collection_agencies",
                "collections",
                "dataservice_serves_datasets",
            ),
        ),
        "code_lists": ResourceSpec(
            transform_to_concept,
            kind="concept",
            lookups=("code_list_entries",),
        ),
    },
)

__all__ = ["dataspot_source", "dataspot_catalog_source"]
