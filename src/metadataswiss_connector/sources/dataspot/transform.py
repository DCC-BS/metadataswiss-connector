"""Dataspot-specific field mapping to I14Y DCAT format."""

from metadataswiss_connector.dcat import builders as dcat
from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    DcatDatasetInputModel,
    DcatDistributionInputModel,
    IdentifierInputModel,
    ResourceModel,
)


def transform_to_dcat(
    record: dict,
    children: dict[str, list],
    *,
    publisher: str,
) -> DcatDatasetInputModel:
    """Map a flattened Dataspot dataset row to a DcatDatasetInputModel.

    Args:
        record: Flattened row from DuckDB. Nested fields use dlt's ``__``
                separator (e.g. ``custom_properties__publisher``).
        children: dlt 1:n child tables keyed by field name (e.g. ``tags``).
        publisher: I14Y publisher identifier (e.g. "Basel-Stadt").
                   Configured globally via I14Y_PUBLISHER_IDENTIFIER env var.
    """
    tags = children.get("tags", [])
    distributions = [
        _map_distribution(d)
        for d in children.get("distributions", [])
        if d.get("access_url")
    ]
    return DcatDatasetInputModel(
        title=dcat.multi_language(record.get("label")),
        description=dcat.multi_language(record.get("description")),
        identifiers=[record["id"]],
        publisher=IdentifierInputModel(identifier=publisher),
        access_rights=CodeInputModel(code="PUBLIC"),
        issued=dcat.epoch_ms_to_datetime(
            record.get("custom_properties__publication_date")
        ),
        modified=dcat.epoch_ms_to_datetime(record.get("custom_properties__last_update")),
        frequency=dcat.frequency(record.get("accrual_periodicity")),
        keywords=dcat.keywords(tags),
        spatial=[record["spatial"]] if record.get("spatial") else None,
        temporal_coverage=dcat.temporal_coverage(record.get("temporal_start")),
        contact_points=dcat.contact_points(record.get("created_by")),
        landing_pages=dcat.landing_pages(
            record.get("custom_properties__ods_dataportal_link")
        ),
        languages=[CodeInputModel(code="de")],
        distributions=distributions or None,
    )


def _map_distribution(raw: dict) -> DcatDistributionInputModel:
    """Map a Dataspot distribution row to a DcatDistributionInputModel."""
    label = raw.get("label")
    return DcatDistributionInputModel(
        title=dcat.multi_language(label),
        description=dcat.multi_language(raw.get("description") or label),
        access_url=ResourceModel(uri=raw.get("access_url") or ""),
        identifier=raw.get("id"),
        issued=dcat.epoch_ms_to_datetime(raw.get("date_created")),
        format=dcat.file_format(raw.get("format")),
    )
