import logging

import dlt
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


def _public_resource(
    name: str,
    path: str,
    data_selector: str,
    *,
    parent: bool = False,
    ignore_404: bool = False,
    ignore_500: bool = False,
) -> dict:
    """Build a dlt rest_api resource spec for a public Dataspot endpoint.

    All Dataspot endpoints we consume share the same shape: a single page
    of HAL-style ``_embedded`` entries that we filter to ``publicState ==
    PUBLIC``. ``parent=True`` marks the spec as a child resource so dlt's
    rest_api transformer wires up ``include_from_parent``; ``ignore_404``
    silences missing children (collections without distributions etc.).
    """
    endpoint: dict = {
        "path": path,
        "paginator": "single_page",
        "data_selector": data_selector,
    }
    response_actions: list[dict] = []
    if ignore_404:
        response_actions.append({"status_code": 404, "action": "ignore"})
    if ignore_500:
        response_actions.append({"status_code": 500, "action": "ignore"})
    if response_actions:
        endpoint["response_actions"] = response_actions
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


@dlt.source(name="dataspot")
def dataspot_source(
    base_url: str | None = None,
    database_name: str | None = None,
    exposed_client_id: str | None = None,
    tenant_id: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    dataspot_access_key: str | None = None,
):
    """dlt source for the Dataspot metadata catalog REST API.

    Any argument left as ``None`` is resolved from dlt's config/secrets
    system via env vars under the ``SOURCES__DATASPOT__*`` naming (loaded
    from ``.env``; see ``.env.example``). Explicit arguments take precedence.
    """
    base_url = base_url or dlt.config["sources.dataspot.base_url"]
    database_name = database_name or dlt.config["sources.dataspot.database_name"]
    exposed_client_id = (
        exposed_client_id or dlt.config["sources.dataspot.exposed_client_id"]
    )
    tenant_id = tenant_id or dlt.secrets["sources.dataspot.tenant_id"]
    client_id = client_id or dlt.secrets["sources.dataspot.client_id"]
    client_secret = client_secret or dlt.secrets["sources.dataspot.client_secret"]
    dataspot_access_key = (
        dataspot_access_key or dlt.secrets["sources.dataspot.dataspot_access_key"]
    )

    auth = DataspotAuth(
        access_token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        client_id=client_id,
        client_secret=client_secret,
        access_token_request_data={"scope": f"api://{exposed_client_id}/.default"},
        dataspot_access_key=dataspot_access_key,
    )

    api_base_url = f"{base_url}/rest/{database_name}"

    config = {
        "client": {"base_url": api_base_url, "auth": auth},
        "resource_defaults": {"primary_key": "id", "write_disposition": "merge"},
        "resources": [
            # Broad fetch over every public data product regardless of
            # stereotype. Downstream filter transformers split this into
            # ``data_products`` (OGD/GEO → I14Y Dataset) and
            # ``data_services`` (API → I14Y DataService). Ancestry walks
            # also consume this broad stream so contact points / data
            # owners get resolved for API records too.
            _public_resource(
                "data_products_all",
                "schemes/Datenprodukte/datasets",
                "_embedded.datasets",
            ),
            _public_resource(
                "code_lists",
                "schemes/Referenzdaten/enumerations",
                "_embedded.enumerations",
            ),
            _public_resource(
                "code_list_entries",
                "schemes/Referenzdaten/enumerations/{resources.code_lists.id}/literals",
                "_embedded.literals",
                parent=True,
                ignore_404=True,
            ),
            _public_resource(
                "distributions",
                "datasets/{resources.data_products_all.id}/distributions",
                "_embedded.distributions",
                parent=True,
                ignore_404=True,
            ),
            _public_resource(
                "compositions",
                "datasets/{resources.data_products_all.id}/compositions",
                "_embedded.compositions",
                parent=True,
                ignore_404=True,
                # Dataspot returns 500 on compositions for some datasets
                # (server-side bug). Skip rather than fail the whole run.
                ignore_500=True,
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
        write_disposition="merge",
    )
    def data_products(items):
        """Pass through OGD/GEO data products (published as I14Y Datasets)."""
        for item in _as_list(items):
            if item.get("stereotype") in DATASET_STEREOTYPES:
                yield item

    @dlt.transformer(
        data_from=data_products_all,
        name="data_services",
        primary_key="id",
        write_disposition="merge",
    )
    def data_services(items):
        """Pass through API data products (published as I14Y DataServices)."""
        for item in _as_list(items):
            if item.get("stereotype") in DATA_SERVICE_STEREOTYPES:
                yield item

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
        collections,
        dataset_collection_path,
        collection_agencies,
        collection_data_owners,
        dataset_structure_components,
    ]
