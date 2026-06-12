"""Dataspot-specific field mapping to I14Y DCAT format."""

from metadataswiss_connector.dcat import builders as dcat
from metadataswiss_connector.dcat.lookups import Lookups, published_ids_table
from metadataswiss_connector.sources.dataspot import mappings
from metadataswiss_connector.sources.dataspot.structure import (
    build_dataset_shacl_turtle,
)
from metadataswiss_connector.sources.dataspot.constants import (
    CONCEPT_RESPONSIBLE_DEPUTY_EMAIL,
    CONCEPT_RESPONSIBLE_PERSON_EMAIL,
    CONCEPT_VERSION,
    DATASPOT_VALID_FROM_SENTINEL,
    DATASPOT_VALID_TO_SENTINEL,
    DEFAULT_CODE_LIST_VALUE_MAX_LENGTH,
    DEFAULT_CODE_LIST_VALUE_TYPE,
)
from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    CodeListConceptInput,
    CodeListEntrySortProperty,
    CodeListEntryValueType,
    DataServiceInputModel,
    DcatDatasetInputModel,
    DcatDistributionInputModel,
    EmailInputModel,
    IdentifierInputModel,
    IdModel,
    ResourceModel,
    VCardModel,
)


def transform_to_dataset(
    record: dict,
    children: dict[str, list],
    *,
    lookups: Lookups,
    publisher: str,
) -> tuple[DcatDatasetInputModel, dict] | DcatDatasetInputModel:
    """Map a flattened Dataspot dataset row to a DcatDatasetInputModel.

    Args:
        record: Flattened row from DuckDB. Nested fields use dlt's ``__``
                separator (e.g. ``custom_properties__publisher``).
        children: dlt 1:n child tables keyed by field name (e.g. ``tags``).
        lookups: per-run view over the cross-reference tables (e.g.
                 ``collections``, ``dataset_collection_path``,
                 ``collection_agencies``, ``collection_data_owners``).
        publisher: I14Y publisher identifier (e.g. "Basel-Stadt").
                   Configured globally via I14Y_PUBLISHER_IDENTIFIER env var.
    """
    tags = children.get("tags", [])
    # Sort by the stable distribution id: ``distributions`` is loaded from a
    # sibling table whose SELECT has no inherent order, so without this the
    # list order — and thus the payload hash — can vary between runs.
    distributions = [
        _map_distribution(d)
        for d in sorted(
            children.get("distributions", []),
            key=lambda d: str(d.get("id") or ""),
        )
        if d.get("access_url")
    ]
    contact_points = _organizational_unit_contact_points(record["id"], lookups)
    data_owner = _resolve_data_owner(record["id"], lookups)
    structure_ttl = build_dataset_shacl_turtle(
        dataset_id=record["id"],
        dataset_label=record.get("label"),
        components=lookups.by("dataset_structure_components", "dataset_id").get(
            record["id"], []
        ),
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
        landing_pages=dcat.landing_pages(record.get("custom_properties__i14y_dataset_landing_page")) or None,
        version=record.get("custom_properties__i14y_dataset_version"),
        version_notes=dcat.multi_language(record.get("custom_properties__i14y_dataset_version_notes")),
        distributions=distributions or None,
    )
    if structure_ttl:
        return model, {"structure": structure_ttl}
    return model


def _ancestor_paths(dataset_id: str, lookups: Lookups) -> list[dict]:
    """The dataset's collection ancestry (``dataset_collection_path``),
    nearest-first.

    collection_id breaks depth ties deterministically: equal-depth
    ancestors otherwise fall back to the unordered lookup row order,
    which can pick a different ancestor between runs and flip the
    payload hash.
    """
    paths = lookups.by("dataset_collection_path", "dataset_id").get(dataset_id, [])
    return sorted(
        paths, key=lambda p: (p.get("depth", 0), str(p.get("collection_id") or ""))
    )


def _resolve_data_owner(dataset_id: str, lookups: Lookups) -> str | None:
    """Resolve the dataset's data-owner name from its collection ancestry.

    Walks the ancestry nearest-first and returns the first ancestor's
    data-owner name from ``collection_data_owners`` (already resolved in
    the source via attribution → post → postAgents label).
    """
    owners = lookups.unique_by("collection_data_owners", "collection_id")
    for path in _ancestor_paths(dataset_id, lookups):
        owner = owners.get(path.get("collection_id"))
        if owner and owner.get("name"):
            return owner["name"]
    return None


def _organizational_unit_contact_points(
    dataset_id: str, lookups: Lookups
) -> list[VCardModel]:
    """Build VCardModel contact points from the dataset's owning agency.

    Walks the dataset's collection ancestry from nearest to furthest,
    picking the first ancestor whose collection has a matching row in
    ``collection_agencies`` (i.e. a collection with stereotype
    ``organizationalUnit`` resolved to a Staatskalender agency).
    The agency's ``email`` becomes ``hasEmail``; the collection's ``label``
    populates ``fn``.
    """
    agencies = lookups.unique_by("collection_agencies", "collection_id")
    collections = lookups.unique_by("collections", "id")
    for path in _ancestor_paths(dataset_id, lookups):
        collection_id = path.get("collection_id")
        agency = agencies.get(collection_id)
        if not agency:
            continue
        email = agency.get("email")
        if not email:
            continue
        collection = collections.get(collection_id, {})
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


def transform_to_dataservice(
    record: dict,
    children: dict[str, list],
    *,
    lookups: Lookups,
    publisher: str,
) -> DataServiceInputModel:
    """Map a flattened Dataspot API record to a DataServiceInputModel.

    Dataspot exposes APIs as data products with stereotype ``API``. They
    share most metadata with OGD/GEO datasets but publish to I14Y under
    ``/dataservices`` with a different input model (no distributions, no
    SHACL structure). Endpoint metadata comes from dedicated
    ``i14y_api_*`` custom properties on the dataspot record.
    """
    tags = children.get("tags", [])
    contact_points = _organizational_unit_contact_points(record["id"], lookups)
    return DataServiceInputModel(
        title=dcat.multi_language(record.get("label")),
        description=dcat.multi_language(
            dcat.html_to_plain_text(record.get("description"))
        ),
        identifiers=[record["id"]],
        publisher=IdentifierInputModel(identifier=publisher),
        serves_datasets=_resolve_serves_datasets(record["id"], lookups) or None,
        access_rights=_dataservice_access_rights(record),
        issued=dcat.epoch_ms_to_datetime(
            record.get("custom_properties__publication_date")
        ),
        modified=dcat.epoch_ms_to_datetime(record.get("custom_properties__last_update")),
        keywords=dcat.keywords(tags),
        contact_points=contact_points or None,
        landing_pages=dcat.landing_pages(
            record.get("custom_properties__i14y_api_landing_page")
        )
        or None,
        endpoint_urls=_dataservice_resource_list(
            record, children, "custom_properties__i14y_api_endpoint_url"
        )
        or None,
        endpoint_descriptions=_dataservice_resource_list(
            record, children, "custom_properties__i14y_api_endpoint_description"
        )
        or None,
    )


def _resolve_serves_datasets(
    dataservice_id: str, lookups: Lookups
) -> list[IdModel]:
    """I14Y datasets this API produces (Dataspot SPEZ2 derivations).

    The source emits ``(dataservice_id, dataspot dataset_id)`` pairs into
    the ``dataservice_serves_datasets`` lookup; each dataspot id is
    resolved to its published I14Y UUID via the datasets' published-ID
    lookup, which the pipeline injects from sync state (see
    ``pipeline.published_id_lookups``). Datasets not yet published (no
    UUID) are skipped — a brand-new dataset+API pair therefore links on
    the run *after* the dataset is first published. Sorted for a stable
    payload hash.
    """
    rows = lookups.by("dataservice_serves_datasets", "dataservice_id").get(
        dataservice_id, []
    )
    dataspot_ids = sorted(
        {row["dataset_id"] for row in rows if row.get("dataset_id")}
    )
    published = lookups.unique_by(published_ids_table("data_products"), "source_id")
    return [
        IdModel(id=published[dataspot_id]["i14y_id"])
        for dataspot_id in dataspot_ids
        if dataspot_id in published
    ]


def _dataservice_access_rights(record: dict) -> CodeInputModel:
    """Map ``custom_properties__i14y_api_access_rights`` to an I14Y code.

    The source carries the value as a URL whose final path segment is the
    I14Y code (e.g. ``.../PUBLIC`` → ``PUBLIC``).
    """
    raw = str(record.get("custom_properties__i14y_api_access_rights") or "").rstrip("/")
    code = raw.rsplit("/", 1)[-1]
    return CodeInputModel(code=code)


def _dataservice_resource_list(
    record: dict, children: dict[str, list], field: str
) -> list[ResourceModel]:
    """Build a list of ResourceModel URIs from a custom-property field.

    dlt flattens scalar custom properties to a column on the parent record
    and list-valued ones to a child table; we accept either shape so the
    transform doesn't need to know which form dataspot returns for these
    endpoint fields.
    """
    child_values = children.get(field, [])
    if child_values:
        return [ResourceModel(uri=str(v)) for v in child_values if v]
    scalar = record.get(field)
    if scalar:
        return [ResourceModel(uri=str(scalar))]
    return []


def transform_to_concept(
    record: dict,
    children: dict[str, list],
    *,
    lookups: Lookups,
    publisher: str,
) -> tuple[CodeListConceptInput, dict]:
    """Map a Dataspot enumeration row to a CodeListConceptInput.

    Returns ``(model, extras)`` where ``extras["entries"]`` carries the
    code-list entry payloads. The concept itself is created via
    ``POST /concepts``; the entries are uploaded as a follow-up via
    ``POST /concepts/{id}/codelist-entries/imports/Json`` once the
    concept's I14Y UUID is known.
    """
    # Enumerations expose no customProperties — ``date_created`` is the
    # only timestamp available in the source payload. Fall back to the
    # fixed open-lower-bound sentinel rather than ``datetime.now()``: a
    # wall-clock default would change the payload hash on every run and
    # trigger a spurious re-publish for any concept missing date_created.
    valid_from = (
        dcat.epoch_ms_to_datetime(record.get("date_created"))
        or dcat.epoch_ms_to_datetime(DATASPOT_VALID_FROM_SENTINEL)
    )
    # Sort by a stable entry key: ``code_list_entries`` is a dlt child
    # table whose read order isn't guaranteed, so without this the entries
    # list order — and thus the payload hash — can vary between runs.
    raw_entries = sorted(
        children.get("code_list_entries", []),
        key=lambda e: (str(e.get("code") or ""), str(e.get("id") or "")),
    )
    model = CodeListConceptInput(
        identifier=record["id"],
        name=dcat.multi_language(record.get("label")),
        description=(
            dcat.multi_language(dcat.html_to_plain_text(record.get("description")))
            or dcat.multi_language(record.get("label"))
        ),
        publisher=IdentifierInputModel(identifier=publisher),
        responsible_person=EmailInputModel(email=CONCEPT_RESPONSIBLE_PERSON_EMAIL),
        responsible_deputy=EmailInputModel(email=CONCEPT_RESPONSIBLE_DEPUTY_EMAIL),
        valid_from=valid_from,
        version=CONCEPT_VERSION,
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
    otherwise we fall back to ``String``. Codeless entries are ignored
    here just as ``_build_entries`` drops them, so they don't force the
    type to ``String`` for a list that is otherwise fully numeric. When no
    entry carries a code, the default value type applies.
    """
    codes = [entry.get("code") for entry in entries if entry.get("code")]
    if not codes:
        return DEFAULT_CODE_LIST_VALUE_TYPE
    for code in codes:
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
