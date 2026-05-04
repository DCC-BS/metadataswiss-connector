"""Dataspot-specific field mapping to I14Y DCAT format."""

# Bump when the mapping logic in this module changes in a way that
# should force a re-publish of every record, even if the source
# ``modified`` timestamp hasn't moved. Sync compares this against the
# value persisted in the per-source state file.
TRANSFORM_VERSION = 1

from metadataswiss_connector.dcat import builders as dcat
from metadataswiss_connector.sources.dataspot import mappings
from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    DcatDatasetInputModel,
    DcatDistributionInputModel,
    IdentifierInputModel,
    ResourceModel,
    VCardModel,
)


def transform_to_dcat(
    record: dict,
    children: dict[str, list],
    *,
    lookups: dict[str, list[dict]],
    publisher: str,
) -> DcatDatasetInputModel:
    """Map a flattened Dataspot dataset row to a DcatDatasetInputModel.

    Args:
        record: Flattened row from DuckDB. Nested fields use dlt's ``__``
                separator (e.g. ``custom_properties__publisher``).
        children: dlt 1:n child tables keyed by field name (e.g. ``tags``).
        lookups: cross-reference tables (e.g. ``collections``,
                 ``dataset_collection_path``, ``collection_agencies``,
                 ``collection_data_owners``) keyed by table name.
        publisher: I14Y publisher identifier (e.g. "Basel-Stadt").
                   Configured globally via I14Y_PUBLISHER_IDENTIFIER env var.
    """
    tags = children.get("tags", [])
    distributions = [
        _map_distribution(d)
        for d in children.get("distributions", [])
        if d.get("access_url")
    ]
    contact_points = _organizational_unit_contact_points(record["id"], lookups)
    data_owner = _resolve_data_owner(record["id"], lookups)
    return DcatDatasetInputModel(
        data_owner=data_owner,
        title=dcat.multi_language(record.get("label")),
        description=dcat.multi_language(dcat.html_to_plain_text(record.get("description"))),
        identifiers=[record["id"]],
        publisher=IdentifierInputModel(identifier=publisher),
        access_rights=CodeInputModel(code="PUBLIC"),
        confidentiality_person=mappings.confidentiality_person(
            children.get("custom_properties__personal_data", [])
        ),
        issued=dcat.epoch_ms_to_datetime(
            record.get("custom_properties__publication_date")
        ),
        modified=dcat.epoch_ms_to_datetime(record.get("custom_properties__last_update")),
        frequency=dcat.frequency(record.get("accrual_periodicity")),
        keywords=dcat.keywords(tags),
        themes=mappings.themes(
            children.get("custom_properties__topics", []),
            record.get("custom_properties__geo_category"),
        ),
        spatial=[record["spatial"]] if record.get("spatial") else None,
        temporal_coverage=dcat.temporal_coverage(record.get("temporal_start"), record.get("temporal_end")),
        contact_points=contact_points or None,
        languages=[CodeInputModel(code="de")],
        retention_period_complement=mappings.retention_period_complement(
            record.get("custom_properties__retention_period"),
            record.get("custom_properties__retention_justification"),
        ),
        distributions=distributions or None,
    )


def _resolve_data_owner(
    dataset_id: str, lookups: dict[str, list[dict]]
) -> str | None:
    """Resolve the dataset's data-owner name from its collection ancestry.

    Walks ``dataset_collection_path`` nearest-first and returns the first
    ancestor's data-owner name from ``collection_data_owners`` (already
    resolved in the source via attribution → post → postAgents label).
    """
    paths = sorted(
        (
            p for p in lookups.get("dataset_collection_path", [])
            if p.get("dataset_id") == dataset_id
        ),
        key=lambda p: p.get("depth", 0),
    )
    if not paths:
        return None
    name_by_collection = {
        o.get("collection_id"): o.get("name")
        for o in lookups.get("collection_data_owners", [])
    }
    for path in paths:
        name = name_by_collection.get(path.get("collection_id"))
        if name:
            return name
    return None


def _organizational_unit_contact_points(
    dataset_id: str, lookups: dict[str, list[dict]]
) -> list[VCardModel]:
    """Build VCardModel contact points from the dataset's owning agency.

    Walks the dataset's collection ancestry (``dataset_collection_path``)
    from nearest to furthest, picking the first ancestor whose collection
    has a matching row in ``collection_agencies`` (i.e. a collection with
    stereotype ``organizationalUnit`` resolved to a Staatskalender agency).
    The agency's ``email`` becomes ``hasEmail``; the collection's ``label``
    populates ``fn``.
    """
    paths = [
        p for p in lookups.get("dataset_collection_path", [])
        if p.get("dataset_id") == dataset_id
    ]
    if not paths:
        return []
    paths.sort(key=lambda p: p.get("depth", 0))

    agencies_by_collection = {
        a.get("collection_id"): a for a in lookups.get("collection_agencies", [])
    }
    collections_by_id = {
        c.get("id"): c for c in lookups.get("collections", [])
    }

    for path in paths:
        collection_id = path.get("collection_id")
        agency = agencies_by_collection.get(collection_id)
        if not agency:
            continue
        email = agency.get("email")
        if not email:
            continue
        collection = collections_by_id.get(collection_id, {})
        return [
            VCardModel(
                fn=dcat.multi_language(collection.get("label")),
                has_address=dcat.multi_language(
                    "\n".join(
                        line for line in (
                            agency.get("location_address"),
                            agency.get("location_code_city"),
                        ) if line
                    ) or None
                ),
                has_email=email,
                has_telephone=agency.get("phone"),
            )
        ]
    return []


def _map_distribution(raw: dict) -> DcatDistributionInputModel:
    """Map a Dataspot distribution row to a DcatDistributionInputModel."""
    label = raw.get("label")
    return DcatDistributionInputModel(
        title=dcat.multi_language(label),
        description=dcat.multi_language(dcat.html_to_plain_text(raw.get("description")) or label),
        access_url=ResourceModel(uri=raw.get("access_url") or ""),
        identifier=raw.get("id"),
        issued=dcat.epoch_ms_to_datetime(raw.get("date_created")),
        format=dcat.file_format(raw.get("format")),
    )
