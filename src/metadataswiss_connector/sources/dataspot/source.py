import logging
from urllib.parse import urlparse

import dlt
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException
from dlt.sources.rest_api import rest_api_resources

logger = logging.getLogger(__name__)

from metadataswiss_connector.sources.dataspot.auth import DataspotAuth
from metadataswiss_connector.sources.dataspot.constants import (
    DATA_SERVICE_STEREOTYPES,
    DATASET_STEREOTYPES,
    PUBLIC_STATE,
    STEREOTYPE_ORGANIZATIONAL_UNIT,
)
from metadataswiss_connector.sources.dataspot.enrichment import DataspotEnrichment


def _is_public(item: dict) -> bool:
    return item.get("publicState") == PUBLIC_STATE


def _publish_on_i14y(item: dict) -> bool:
    """Whether a record opts in to I14Y publication.

    Gated by the ``publish_on_i14y`` custom property: only an explicit
    ``"yes"`` (case-insensitive) opts in; a missing property or any other
    value (``"no"`` included) keeps the record off I14Y. The flag surfaces
    on the list item for data products *and* code lists (it is simply
    absent until set on at least one record).

    Applied to ``data_products``, ``data_services`` and ``code_lists``.
    Their raw tables use ``write_disposition="replace"`` (not ``merge``)
    so that a record which loses its ``"yes"`` actually drops out of the
    raw table — a merge table would retain the stale row and keep
    publishing it — and is then decommissioned on the next sync (``sync``
    deletes records absent from the source).
    """
    value = item.get("customProperties", {}).get("publish_on_i14y")
    return isinstance(value, str) and value.strip().lower() == "yes"


# Skips recorded by ``_skip_responses`` during the current extract. Drained
# (and thereby cleared) by the Dagster extract asset via
# ``drain_extract_skips`` after ``pipeline.run`` — from there they flow into
# the quality-issues alert email. Plain module state is safe here: Dagster
# runs each extract asset in its own subprocess, and list.append is atomic.
_extract_skips: list[dict] = []

# id → label of every data product that streamed through the current
# extract. The skip hook only sees the failing HTTP response (child URL),
# so this is how a skip gets a human-readable title into the alert email.
_parent_labels: dict[str, str | None] = {}


def _remember_label(item: dict) -> dict:
    if item.get("id"):
        _parent_labels[item["id"]] = item.get("label") or item.get("title")
    return item


def drain_extract_skips() -> list[dict]:
    """Return and clear the skips recorded by the last extract.

    Each dict matches the ``invalid_details`` shape the alert email
    consumes (``id``/``title``/``stage``/``kind``/``field_errors``) plus
    ``resource`` so the caller can attach it to the right raw asset.
    """
    skips = list(_extract_skips)
    _extract_skips.clear()
    return skips


def _skip_responses(name: str, statuses: tuple[int, ...]):
    """Response hook that skips (instead of fails on) the given statuses.

    Equivalent to dlt's built-in ``{"action": "ignore"}`` response action,
    except that dlt only logs the skip at INFO on its own logger (silent at
    the default WARNING level) — here it surfaces as a WARNING in the run
    logs and is recorded for the quality-issues email. 404 stays quiet: it
    just means the item has no children, which is routine. Statuses not
    listed fall through to dlt's raise_for_status.
    """

    def _hook(response, *args, **kwargs):
        if response.status_code not in statuses:
            return
        if response.status_code != 404:
            detail = response.text[:200]
            logger.warning(
                "Skipping %s (HTTP %s): %s", response.url, response.status_code, detail
            )
            # Child paths end in .../{parent_id}/{name}, so the owning
            # dataset is the second-to-last path segment.
            parent_id = urlparse(response.url).path.rstrip("/").split("/")[-2]
            _extract_skips.append({
                "resource": name,
                "id": parent_id,
                "title": _parent_labels.get(parent_id),
                "stage": "extract",
                "kind": "dataset",
                "field_errors": [{
                    "field": name,
                    "message": (
                        f"{name} could not be extracted (HTTP"
                        f" {response.status_code}): {detail}"
                    ),
                }],
            })
        raise IgnoreResponseException

    return _hook


def _public_resource(
    name: str,
    path: str,
    data_selector: str,
    *,
    parent: bool = False,
    ignore_statuses: tuple[int, ...] = (),
) -> dict:
    """Build a dlt rest_api resource spec for a public Dataspot endpoint.

    All Dataspot endpoints we consume share the same shape: a single page
    of HAL-style ``_embedded`` entries that we filter to ``publicState ==
    PUBLIC``. ``parent=True`` marks the spec as a child resource so dlt's
    rest_api transformer wires up ``include_from_parent``;
    ``ignore_statuses`` skips per-item error responses (missing children,
    Dataspot server-side bugs) instead of failing the run.
    """
    endpoint: dict = {
        "path": path,
        "paginator": "single_page",
        "data_selector": data_selector,
    }
    if ignore_statuses:
        endpoint["response_actions"] = [_skip_responses(name, ignore_statuses)]
    spec: dict = {
        "name": name,
        "endpoint": endpoint,
        "processing_steps": [{"filter": _is_public}],
    }
    if parent:
        spec["primary_key"] = "id"
        spec["write_disposition"] = "merge"
        spec["include_from_parent"] = ["id"]
    return spec


def _as_list(items):
    return [items] if isinstance(items, dict) else items


@dlt.source(name="dataspot", section="dataspot")
def dataspot_source(
    base_url: str = dlt.config.value,
    database_name: str = dlt.config.value,
    exposed_client_id: str = dlt.config.value,
    tenant_id: str = dlt.secrets.value,
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    dataspot_access_key: str = dlt.secrets.value,
):
    """dlt source for the Dataspot metadata catalog REST API.

    Arguments not passed explicitly are injected by dlt's config/secrets
    system from the ``sources.dataspot.*`` section — i.e. env vars under
    the ``SOURCES__DATASPOT__*`` naming (loaded from ``.env``; see
    ``.env.example``). The explicit ``section`` pins that lookup path
    independently of this module's name.
    """
    auth = DataspotAuth(
        access_token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data={"scope": f"api://{exposed_client_id}/.default"},
        dataspot_access_key=dataspot_access_key,
    )

    api_base_url = f"{base_url}/rest/{database_name}"

    # Broad fetch over every public data product regardless of
    # stereotype. Downstream filter transformers split this into
    # ``data_products`` (OGD/GEO → I14Y Dataset) and
    # ``data_services`` (API → I14Y DataService). Ancestry walks
    # also consume this broad stream so contact points / data
    # owners get resolved for API records too.
    products_spec = _public_resource(
        "data_products_all",
        "schemes/Datenprodukte/datasets",
        "_embedded.datasets",
    )
    # Record id → label so skip entries (see _skip_responses) can carry a
    # human-readable title into the alert email.
    products_spec["processing_steps"].append({"map": _remember_label})

    # Code lists opt in to I14Y via the same ``publish_on_i14y`` custom
    # property as data products (it surfaces on the enumeration list item).
    # ``replace`` — like data_products/data_services — so a code list that
    # loses its ``"yes"`` drops out and is decommissioned; a merge table
    # would keep publishing the stale row. Filtering the parent also stops
    # ``code_list_entries`` (its child) from fetching literals for lists
    # that won't be published.
    code_lists_spec = _public_resource(
        "code_lists",
        "schemes/Referenzdaten/enumerations",
        "_embedded.enumerations",
    )
    code_lists_spec["processing_steps"].append({"filter": _publish_on_i14y})
    code_lists_spec["write_disposition"] = "replace"

    config = {
        "client": {"base_url": api_base_url, "auth": auth},
        "resource_defaults": {"primary_key": "id", "write_disposition": "merge"},
        "resources": [
            products_spec,
            code_lists_spec,
            _public_resource(
                "code_list_entries",
                "schemes/Referenzdaten/enumerations/{resources.code_lists.id}/literals",
                "_embedded.literals",
                parent=True,
                ignore_statuses=(404,),
            ),
            _public_resource(
                "distributions",
                "datasets/{resources.data_products_all.id}/distributions",
                "_embedded.distributions",
                parent=True,
                ignore_statuses=(404,),
            ),
            _public_resource(
                "compositions",
                "datasets/{resources.data_products_all.id}/compositions",
                "_embedded.compositions",
                parent=True,
                # Dataspot server-side bugs on compositions for some
                # datasets: 500 for some, 400 ("BusinessAttribute ...
                # forbidden") when a composition links an attribute the
                # API user may not see. Skip the dataset's structure
                # rather than fail the whole run.
                ignore_statuses=(400, 404, 500),
            ),
        ],
    }

    resources = list(rest_api_resources(config))
    data_products_all = next(r for r in resources if r.name == "data_products_all")
    compositions = next(r for r in resources if r.name == "compositions")

    enrichment = DataspotEnrichment(
        base_url=base_url, database_name=database_name, auth=auth
    )

    def _iter_ancestor_pairs(items):
        """Yield (item, depth, href, collection) for every (dataset, ancestor)."""
        for item in _as_list(items):
            for depth, (href, collection) in enumerate(enrichment.walk_ancestors(item)):
                yield item, depth, href, collection

    @dlt.transformer(
        data_from=data_products_all,
        name="data_products",
        primary_key="id",
        # ``replace`` (not ``merge``) so the ``publish_on_i14y`` gate below
        # actually removes records: a merge table retains rows no longer
        # yielded, so a dataset that loses its ``"yes"`` would keep being
        # published. Replace rebuilds the table as exactly the opted-in set
        # each run, letting sync decommission the rest.
        write_disposition="replace",
    )
    def data_products(items):
        """OGD/GEO data products opted in to I14Y (published as Datasets)."""
        for item in _as_list(items):
            if (
                item.get("stereotype") in DATASET_STEREOTYPES
                and _publish_on_i14y(item)
            ):
                yield item

    @dlt.transformer(
        data_from=data_products_all,
        name="data_services",
        primary_key="id",
        # ``replace`` for the same reason as ``data_products`` above: the
        # gate must be able to drop an API that loses its ``"yes"``.
        write_disposition="replace",
    )
    def data_services(items):
        """API data products opted in to I14Y (published as DataServices)."""
        for item in _as_list(items):
            if (
                item.get("stereotype") in DATA_SERVICE_STEREOTYPES
                and _publish_on_i14y(item)
            ):
                yield item

    @dlt.transformer(
        data_from=data_products_all,
        name="dataservice_serves_datasets",
        primary_key=["dataservice_id", "dataset_id"],
        write_disposition="replace",
    )
    def dataservice_serves_datasets(items):
        """Per (API, produced-dataset) row for I14Y ``servesDatasets``.

        Only API records carry the ``derivedFrom`` link we care about; for
        each we emit one row per SPEZ2-derived dataset. ``replace`` so a
        derivation removed in Dataspot also drops out of the lookup.
        """
        for item in _as_list(items):
            if item.get("stereotype") not in DATA_SERVICE_STEREOTYPES:
                continue
            for dataset_id in enrichment.fetch_derived_dataset_ids(item["id"]):
                yield {"dataservice_id": item["id"], "dataset_id": dataset_id}

    @dlt.transformer(
        data_from=data_products_all,
        name="collections",
        primary_key="id",
        write_disposition="merge",
    )
    def collections(items):
        emitted: set[str] = set()
        for _item, _depth, href, collection in _iter_ancestor_pairs(items):
            if href in emitted:
                continue
            emitted.add(href)
            yield collection

    @dlt.transformer(
        data_from=data_products_all,
        name="dataset_collection_path",
        primary_key=["dataset_id", "depth"],
        write_disposition="merge",
    )
    def dataset_collection_path(items):
        for item, depth, _href, collection in _iter_ancestor_pairs(items):
            yield {
                "dataset_id": item.get("id"),
                "collection_id": collection.get("id"),
                "depth": depth,
            }

    @dlt.transformer(
        data_from=collections,
        name="collection_agencies",
        primary_key="collection_id",
        write_disposition="merge",
    )
    def collection_agencies(collection_items):
        for collection in _as_list(collection_items):
            if collection.get("stereotype") != STEREOTYPE_ORGANIZATIONAL_UNIT:
                continue
            state_calendar_id = collection.get("customProperties", {}).get(
                "stateCalendarId"
            )
            if state_calendar_id is None:
                continue
            agency = enrichment.fetch_agency(int(state_calendar_id))
            if agency is None:
                continue
            data = {
                entry.get("name"): entry.get("value")
                for entry in agency.get("data", []) or []
                if entry.get("name")
            }
            yield {
                "collection_id": collection.get("id"),
                "state_calendar_id": int(state_calendar_id),
                "agency_href": agency.get("href"),
                **data,
            }

    @dlt.transformer(
        data_from=data_products_all,
        name="collection_data_owners",
        primary_key="collection_id",
        write_disposition="merge",
    )
    def collection_data_owners(items):
        # Walks ancestors directly from data_products_all rather than from
        # ``collections``: dlt pipes are single-consumer, and
        # ``collection_agencies`` already drains that one.
        emitted: set[str] = set()
        for _item, _depth, _href, collection in _iter_ancestor_pairs(items):
            collection_id = collection.get("id")
            if not collection_id or collection_id in emitted:
                continue
            emitted.add(collection_id)
            attr_href = (
                collection.get("_links", {}).get("attributedTo", {}).get("href")
            )
            if not attr_href:
                continue
            name = enrichment.fetch_data_owner_name(attr_href)
            if not name:
                continue
            yield {"collection_id": collection_id, "name": name}

    @dlt.transformer(
        data_from=compositions,
        name="dataset_structure_components",
        primary_key="composition_id",
        write_disposition="replace",
    )
    def dataset_structure_components(items):
        """Per (dataset, attribute) row with everything SHACL needs.

        Joins composition → attribute → datatype eagerly so the dataset
        transform can build a Turtle SHACL document without further API
        calls. ``replace`` semantics: a structure that loses an attribute
        in Dataspot must lose it on the I14Y side too.
        """
        for composition in _as_list(items):
            attr_href = (
                composition.get("_links", {}).get("composedOf", {}).get("href")
            )
            if not attr_href:
                continue
            attribute = enrichment.fetch_attribute(attr_href)
            if not _is_public(attribute):
                continue
            # Some compositions point back at a Dataset rather than at a
            # real attribute (Dataspot GEO stereotype quirk). Those would
            # produce nonsensical sh:property rows, so drop them.
            if attribute.get("_type") not in {"UmlAttribute", "BusinessAttribute"}:
                continue
            dt_href = (
                attribute.get("_links", {}).get("hasRange", {}).get("href")
            )
            datatype = enrichment.fetch_datatype(dt_href) if dt_href else {}
            if datatype and not _is_public(datatype):
                datatype = {}
            # Enumerations sit at /enumerations/{id} and have
            # _type=ReferenceObject; datatypes sit at /datatypes/{id} with
            # _type=DataDomain. We surface the kind so the SHACL builder
            # can emit dcterms:conformsTo for code-list-typed attributes.
            datatype_kind = (datatype or {}).get("_type")
            yield {
                "dataset_id": composition.get("componentOf"),
                "composition_id": composition.get("id"),
                "order": composition.get("order"),
                "composition_label": composition.get("label"),
                "composition_title": composition.get("title"),
                "composition_description": composition.get("description"),
                "composition_required": (
                    composition.get("customProperties", {}).get("required")
                ),
                "attribute_id": attribute.get("id"),
                "attribute_label": attribute.get("label"),
                "attribute_description": attribute.get("description"),
                "attribute_required": attribute.get("required"),
                "attribute_cardinality": attribute.get("cardinality"),
                "attribute_min_inclusive": attribute.get("minInclusive"),
                "attribute_max_inclusive": attribute.get("maxInclusive"),
                "datatype_id": (datatype or {}).get("id"),
                "datatype_kind": datatype_kind,
                "datatype_base_type": (datatype or {}).get("baseType"),
                "datatype_label": (datatype or {}).get("label"),
            }

    return [
        *resources,
        data_products,
        data_services,
        dataservice_serves_datasets,
        collections,
        dataset_collection_path,
        collection_agencies,
        collection_data_owners,
        dataset_structure_components,
    ]
