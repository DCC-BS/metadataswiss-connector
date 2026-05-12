"""Dataspot-specific field mapping to I14Y DCAT format."""

from datetime import datetime, timezone

from metadataswiss_connector.dcat import builders as dcat
from metadataswiss_connector.sources.dataspot import mappings
from metadataswiss_connector.sources.dataspot.structure import (
    build_dataset_shacl_turtle,
)
from metadataswiss_connector.sources.dataspot.constants import (
    DATASPOT_VALID_FROM_SENTINEL,
    DATASPOT_VALID_TO_SENTINEL,
    DEFAULT_CODE_LIST_VALUE_MAX_LENGTH,
    DEFAULT_CODE_LIST_VALUE_TYPE,
    EMAIL_RE,
    FALLBACK_CONTACT_EMAIL,
)
from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    CodeListConceptInput,
    CodeListEntrySortProperty,
    CodeListEntryValueType,
    DcatDatasetInputModel,
    DcatDistributionInputModel,
    EmailInputModel,
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
) -> tuple[DcatDatasetInputModel, dict] | DcatDatasetInputModel:
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
    structure_ttl = build_dataset_shacl_turtle(
        dataset_id=record["id"],
        dataset_label=record.get("label"),
        components=[
            c for c in lookups.get("dataset_structure_components", [])
            if c.get("dataset_id") == record["id"]
        ],
    )
    model = DcatDatasetInputModel(
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
        landing_pages=dcat.landing_pages(record.get("custom_properties__landing_page")) or None,
        version=record.get("custom_properties__version"),
        version_notes=dcat.multi_language(record.get("custom_properties__version_notes")),
        distributions=distributions or None,
    )
    if structure_ttl:
        return model, {"structure": structure_ttl}
    return model


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


def transform_to_concept(
    record: dict,
    children: dict[str, list],
    *,
    lookups: dict[str, list[dict]],
    publisher: str,
) -> tuple[CodeListConceptInput, dict]:
    """Map a Dataspot enumeration row to a CodeListConceptInput.

    Returns ``(model, extras)`` where ``extras["entries"]`` carries the
    code-list entry payloads. The concept itself is created via
    ``POST /concepts``; the entries are uploaded as a follow-up via
    ``POST /concepts/{id}/codelist-entries/imports/Json`` once the
    concept's I14Y UUID is known.
    """
    contact = _resolve_concept_contact(record)
    # Enumerations expose no customProperties — ``date_created`` is the
    # only timestamp available in the source payload.
    valid_from = (
        dcat.epoch_ms_to_datetime(record.get("date_created"))
        or datetime.now(timezone.utc)
    )
    raw_entries = children.get("code_list_entries", [])
    model = CodeListConceptInput(
        identifier=record["id"],
        name=dcat.multi_language(record.get("label")),
        description=(
            dcat.multi_language(dcat.html_to_plain_text(record.get("description")))
            or dcat.multi_language(record.get("label"))
        ),
        publisher=IdentifierInputModel(identifier=publisher),
        responsible_person=EmailInputModel(email=contact),
        responsible_deputy=EmailInputModel(email=contact),
        valid_from=valid_from,
        version="1.0.0",
        code_list_entry_value_type=_value_type_from_entries(raw_entries),
        code_list_entry_value_max_length=_max_code_length(raw_entries),
        code_list_entry_default_sort_property=CodeListEntrySortProperty.code,
    )
    extras = {"entries": _build_entries(raw_entries)}
    return model, extras


def _build_entries(raw_entries: list[dict]) -> list[dict]:
    """Map Dataspot literals to CodeListEntryModel JSON payloads.

    Skips entries without a ``code`` (I14Y rejects empty codes). The
    ``conceptId`` is left out and injected by the I14Y client at upload
    time, since it's only known after the parent concept is created.
    """
    code_by_id = {e.get("id"): e.get("code") for e in raw_entries if e.get("id")}
    out: list[dict] = []
    for entry in raw_entries:
        code = entry.get("code")
        if not code:
            continue
        name_text = entry.get("long_text") or entry.get("short_text")
        payload: dict = {
            "code": str(code),
            "name": _multi_lang_dict(name_text),
        }
        description = dcat.html_to_plain_text(entry.get("description"))
        if description:
            payload["description"] = _multi_lang_dict(description)
        valid_from = _entry_validity(entry.get("valid_from"), DATASPOT_VALID_FROM_SENTINEL)
        if valid_from:
            payload["validFrom"] = valid_from
        valid_to = _entry_validity(entry.get("valid_to"), DATASPOT_VALID_TO_SENTINEL)
        if valid_to:
            payload["validTo"] = valid_to
        parent_id = entry.get("parent_id")
        if parent_id and parent_id in code_by_id and code_by_id[parent_id]:
            payload["parentCode"] = str(code_by_id[parent_id])
        out.append(payload)
    return out


def _multi_lang_dict(text: str) -> dict:
    """Inline equivalent of dcat.multi_language for raw JSON dicts."""
    return {"de": text}


def _entry_validity(epoch_ms: int | float | None, sentinel: int) -> str | None:
    """Convert epoch ms to ISO-8601, skipping Dataspot's open-bound sentinels."""
    if epoch_ms is None or epoch_ms == sentinel:
        return None
    dt = dcat.epoch_ms_to_datetime(epoch_ms)
    return dt.isoformat() if dt else None


def _value_type_from_entries(entries: list[dict]) -> CodeListEntryValueType:
    """Infer code-list value type from the ``code`` of each entry.

    I14Y only allows ``Numeric`` if every code parses as a number;
    otherwise we fall back to ``String``.
    """
    if not entries:
        return DEFAULT_CODE_LIST_VALUE_TYPE
    for entry in entries:
        code = entry.get("code")
        if code is None:
            return CodeListEntryValueType.string
        try:
            float(str(code))
        except ValueError:
            return CodeListEntryValueType.string
    return CodeListEntryValueType.numeric


def _max_code_length(entries: list[dict]) -> int:
    """Tightest valid max length for the given codes, with a sane floor."""
    if not entries:
        return DEFAULT_CODE_LIST_VALUE_MAX_LENGTH
    longest = max((len(str(e.get("code") or "")) for e in entries), default=0)
    return max(longest, 1)


def _resolve_concept_contact(record: dict) -> str:
    """Pick an email for responsiblePerson/Deputy on a code-list concept.

    Dataspot's ``created_by`` is typically an email; if it isn't, we
    fall back to a constant so the concept still validates. The user
    can refine this once the source surfaces a stable contact field.
    """
    candidate = (record.get("created_by") or "").strip()
    if candidate and EMAIL_RE.match(candidate):
        return candidate
    return FALLBACK_CONTACT_EMAIL


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
