import logging
import time
from email.utils import parsedate_to_datetime

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.rest_api import rest_api_resources

logger = logging.getLogger(__name__)

from metadataswiss_connector.sources.dataspot.auth import DataspotAuth


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
    system (``.dlt/config.toml``, ``.dlt/secrets.toml``, or env vars under
    ``sources.dataspot.*``). Explicit arguments take precedence.
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
        "client": {
            "base_url": api_base_url,
            "auth": auth,
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "data_products",
                "endpoint": {
                    "path": "schemes/Datenprodukte/datasets",
                    "paginator": "single_page",
                    "data_selector": "_embedded.datasets",
                },
                "processing_steps": [
                    {
                        "filter": lambda x: x["publicState"] == "PUBLIC"
                    },
                ],
            },
            {
                "name": "distributions",
                "primary_key": "id",
                "write_disposition": "merge",
                "endpoint": {
                    "path": "datasets/{resources.data_products.id}/distributions",
                    "paginator": "single_page",
                    "data_selector": "_embedded.distributions",
                    "response_actions": [
                        {"status_code": 404, "action": "ignore"},
                    ],
                },
                "processing_steps": [
                    {
                        "filter": lambda x: x["publicState"] == "PUBLIC"
                    },
                ],
                "include_from_parent": ["id"],
            },
        ],
    }

    resources = list(rest_api_resources(config))
    data_products = next(r for r in resources if r.name == "data_products")

    client = RESTClient(base_url=base_url, auth=auth)
    staatskalender_client = RESTClient(base_url="https://staatskalender.bs.ch/api")
    parent_cache: dict[str, dict] = {}
    agency_cache: dict[int, dict | None] = {}
    data_owner_name_cache: dict[str, str | None] = {}
    post_agent_label_cache: dict[str, str | None] = {}

    # Role UUID identifying the "data owner" attribution in Dataspot.
    # The collection's ``attributedTo`` endpoint returns Attribution
    # objects whose ``attributedTo`` value points to a Post (a role-
    # binding), not directly to a Person. The Post's ``postAgents`` link
    # resolves to the Person(s) currently holding that post.
    DATA_OWNER_ROLE_UUID = "02222f05-5690-4cb8-8d90-c27ca57e98e9"

    def _fetch_post_agent_label(post_id: str) -> str | None:
        if post_id in post_agent_label_cache:
            return post_agent_label_cache[post_id]
        try:
            payload = client.get(
                f"/rest/{database_name}/posts/{post_id}/postAgents"
            ).json()
        except Exception as exc:
            logger.warning("postAgents fetch failed for %s: %s", post_id, exc)
            post_agent_label_cache[post_id] = None
            return None
        for person in payload.get("_embedded", {}).get("postAgents", []) or []:
            if person.get("publicState") and person["publicState"] != "PUBLIC":
                continue
            label = person.get("label")
            if label:
                post_agent_label_cache[post_id] = label
                return label
        post_agent_label_cache[post_id] = None
        return None

    def _fetch_data_owner_name(href: str) -> str | None:
        if href in data_owner_name_cache:
            return data_owner_name_cache[href]
        try:
            payload = client.get(href).json()
        except Exception as exc:
            logger.warning("attributedTo fetch failed for %s: %s", href, exc)
            data_owner_name_cache[href] = None
            return None
        name: str | None = None
        for entry in payload.get("_embedded", {}).get("attributedTo", []) or []:
            if entry.get("attributedAs") != DATA_OWNER_ROLE_UUID:
                continue
            post_id = entry.get("attributedTo")
            if not post_id:
                continue
            name = _fetch_post_agent_label(post_id)
            if name:
                break
        data_owner_name_cache[href] = name
        return name

    def _fetch_parent(href: str) -> dict:
        if href not in parent_cache:
            parent_cache[href] = client.get(href).json()
        return parent_cache[href]

    def _staatskalender_get(url: str, max_retries: int = 3) -> dict | None:
        for attempt in range(max_retries):
            response = staatskalender_client.get(url)
            if response.status_code == 429:
                reset = response.headers.get("x-ratelimit-reset")
                wait = 60.0
                if reset:
                    try:
                        delta = parsedate_to_datetime(reset).timestamp() - time.time()
                        wait = max(1.0, min(delta + 1.0, 900.0))
                    except (TypeError, ValueError):
                        pass
                logger.warning(
                    "staatskalender rate limited on %s; sleeping %.0fs (attempt %d/%d)",
                    url, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue
            if response.status_code >= 400:
                logger.warning(
                    "staatskalender %s returned %d; skipping",
                    url, response.status_code,
                )
                return None
            return response.json()
        logger.warning("staatskalender %s still rate-limited after retries; skipping", url)
        return None

    def _fetch_agency(state_calendar_id: int) -> dict | None:
        if state_calendar_id in agency_cache:
            return agency_cache[state_calendar_id]
        payload = _staatskalender_get(f"agencies/{state_calendar_id}")
        item: dict | None = None
        if payload is not None:
            items = payload.get("collection", {}).get("items") or []
            if items:
                item = items[0]
        agency_cache[state_calendar_id] = item
        return item

    def _walk_ancestors(item):
        href = item.get("_links", {}).get("inCollection", {}).get("href")
        while href:
            parent = _fetch_parent(href)
            if (
                parent.get("publicState")
                and parent["publicState"] != "PUBLIC"
            ):
                return
            yield href, parent
            href = (
                parent.get("_links", {}).get("inCollection", {}).get("href")
            )

    @dlt.transformer(
        data_from=data_products,
        name="collections",
        primary_key="id",
        write_disposition="merge",
    )
    def collections(items):
        if isinstance(items, dict):
            items = [items]
        emitted: set[str] = set()
        for item in items:
            for href, parent in _walk_ancestors(item):
                if href in emitted:
                    continue
                emitted.add(href)
                yield parent

    @dlt.transformer(
        data_from=data_products,
        name="dataset_collection_path",
        primary_key=["dataset_id", "depth"],
        write_disposition="merge",
    )
    def dataset_collection_path(items):
        if isinstance(items, dict):
            items = [items]
        for item in items:
            dataset_id = item.get("id")
            for depth, (_href, parent) in enumerate(_walk_ancestors(item)):
                yield {
                    "dataset_id": dataset_id,
                    "collection_id": parent.get("id"),
                    "depth": depth,
                }

    @dlt.transformer(
        data_from=collections,
        name="collection_agencies",
        primary_key="collection_id",
        write_disposition="merge",
    )
    def collection_agencies(collection_items):
        if isinstance(collection_items, dict):
            collection_items = [collection_items]
        for collection in collection_items:
            if collection.get("stereotype") != "organizationalUnit":
                continue
            state_calendar_id = (
                collection.get("customProperties", {}).get("stateCalendarId")
            )
            if state_calendar_id is None:
                continue
            agency = _fetch_agency(int(state_calendar_id))
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
        data_from=data_products,
        name="collection_data_owners",
        primary_key="collection_id",
        write_disposition="merge",
    )
    def collection_data_owners(items):
        # Walk ancestors directly from data_products instead of consuming
        # the ``collections`` transformer: dlt's pipe is single-consumer,
        # and ``collection_agencies`` already drains it.
        if isinstance(items, dict):
            items = [items]
        emitted: set[str] = set()
        for item in items:
            for _href, collection in _walk_ancestors(item):
                collection_id = collection.get("id")
                if not collection_id or collection_id in emitted:
                    continue
                emitted.add(collection_id)
                attr_href = (
                    collection.get("_links", {}).get("attributedTo", {}).get("href")
                )
                if not attr_href:
                    continue
                name = _fetch_data_owner_name(attr_href)
                if not name:
                    continue
                yield {
                    "collection_id": collection_id,
                    "name": name,
                }

    return [
        *resources,
        collections,
        dataset_collection_path,
        collection_agencies,
        collection_data_owners,
    ]
