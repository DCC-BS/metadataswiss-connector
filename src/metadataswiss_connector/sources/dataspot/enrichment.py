"""Enrichment helpers for the Dataspot source.

Wraps the Dataspot REST client and the Staatskalender lookup behind a
single class so the source-level dlt transformers stay declarative.
Caches and retry logic live here so they're independently testable.
"""

import logging
import os
import time
from collections.abc import Callable, Iterator
from email.utils import parsedate_to_datetime
from typing import Any

import requests
from dlt.sources.helpers.rest_client import RESTClient
from requests import PreparedRequest
from requests.auth import AuthBase, HTTPBasicAuth

from metadataswiss_connector.sources.dataspot.constants import (
    DATA_OWNER_ROLE_UUID,
    DERIVATION_QUALIFIER_PRODUCES,
    PUBLIC_STATE,
    STAATSKALENDER_BASE_URL,
    STAATSKALENDER_DEFAULT_WAIT_SECONDS,
    STAATSKALENDER_MAX_RETRIES,
    STAATSKALENDER_MAX_WAIT_SECONDS,
)

logger = logging.getLogger(__name__)


class _StaatskalenderAuth(AuthBase):
    """Exchange an API key for a token, then Basic-auth with that token."""

    def __init__(self, access_key: str, authenticate_url: str) -> None:
        self._access_key = access_key
        self._authenticate_url = authenticate_url
        self._token: str | None = None

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        if not self._token:
            response = requests.get(
                self._authenticate_url,
                auth=HTTPBasicAuth(self._access_key, ""),
            )
            response.raise_for_status()
            self._token = response.json()["token"]
        return HTTPBasicAuth(self._token, "")(request)


class DataspotEnrichment:
    """Resolves ancestry, data-owner names, and staatskalender agencies.

    All fetch methods are memoized for the lifetime of the instance.
    Pass an existing ``client`` (and optional ``staatskalender_client``)
    to inject test doubles; otherwise both are constructed from
    ``base_url``/``database_name``/``auth``.
    """

    def __init__(
        self,
        *,
        base_url: str,
        database_name: str,
        auth,
        client: RESTClient | None = None,
        staatskalender_client: RESTClient | None = None,
    ):
        self.database_name = database_name
        self.client = client or RESTClient(base_url=base_url, auth=auth)
        if staatskalender_client is not None:
            self.staatskalender_client = staatskalender_client
        else:
            key = os.environ.get("HTTPS_ACCESS_KEY_STAATSKALENDER")
            if key:
                sk_auth = _StaatskalenderAuth(
                    key, f"{STAATSKALENDER_BASE_URL}/authenticate"
                )
            else:
                sk_auth = None
            self.staatskalender_client = RESTClient(
                base_url=STAATSKALENDER_BASE_URL,
                auth=sk_auth,
            )
        self._parent_cache: dict[str, dict] = {}
        self._agency_cache: dict[int, dict | None] = {}
        self._data_owner_name_cache: dict[str, str | None] = {}
        self._post_agent_label_cache: dict[str, str | None] = {}
        self._attribute_cache: dict[str, dict] = {}
        self._datatype_cache: dict[str, dict] = {}
        self._derived_dataset_cache: dict[str, list[str] | None] = {}

    # --- caching helpers -------------------------------------------------

    @staticmethod
    def _safe_cached(
        cache: dict,
        key,
        loader: Callable[[], Any],
        log_label: str,
    ):
        """Memoize ``loader`` under ``key``; on exception cache ``None``."""
        if key in cache:
            return cache[key]
        try:
            result = loader()
        except Exception as exc:
            logger.warning("%s fetch failed for %s: %s", log_label, key, exc)
            cache[key] = None
            return None
        cache[key] = result
        return result

    # --- dataspot fetches ------------------------------------------------

    def fetch_parent(self, href: str) -> dict:
        """Resolve and cache an ``inCollection`` href to its parent payload.

        Errors propagate: an unreachable parent should fail loudly rather
        than silently truncating the ancestry chain.
        """
        if href not in self._parent_cache:
            self._parent_cache[href] = self.client.get(href).json()
        return self._parent_cache[href]

    def fetch_attribute(self, href: str) -> dict:
        """Resolve and cache a composition's ``composedOf`` href to its attribute payload."""
        if href not in self._attribute_cache:
            self._attribute_cache[href] = self.client.get(href).json()
        return self._attribute_cache[href]

    def fetch_datatype(self, href: str) -> dict:
        """Resolve and cache an attribute's ``hasRange`` href to its datatype payload."""
        if href not in self._datatype_cache:
            self._datatype_cache[href] = self.client.get(href).json()
        return self._datatype_cache[href]

    def fetch_derived_dataset_ids(self, dataset_id: str) -> list[str]:
        """Dataspot dataset IDs an API produces, per its SPEZ2 derivations.

        An API's ``datasets/{id}/derivedFrom`` link returns ``Derivation``
        objects in ``_embedded.derivedFrom``; each carries a ``derivedFrom``
        target ID and a ``qualifier``. We keep only ``SPEZ2`` derivations
        (the "produces" relation) and return their target dataset IDs.
        """
        def _load() -> list[str]:
            payload = self.client.get(
                f"/rest/{self.database_name}/datasets/{dataset_id}/derivedFrom"
            ).json()
            return [
                target
                for derivation in payload.get("_embedded", {}).get("derivedFrom", [])
                or []
                if derivation.get("qualifier") == DERIVATION_QUALIFIER_PRODUCES
                and (target := derivation.get("derivedFrom"))
            ]

        return (
            self._safe_cached(
                self._derived_dataset_cache, dataset_id, _load, "derivedFrom"
            )
            or []
        )

    def fetch_post_agent_label(self, post_id: str) -> str | None:
        def _load() -> str | None:
            payload = self.client.get(
                f"/rest/{self.database_name}/posts/{post_id}/postAgents"
            ).json()
            for person in payload.get("_embedded", {}).get("postAgents", []) or []:
                if person.get("publicState") and person["publicState"] != PUBLIC_STATE:
                    continue
                label = person.get("label")
                if label:
                    return label
            return None

        return self._safe_cached(
            self._post_agent_label_cache, post_id, _load, "postAgents"
        )

    def fetch_data_owner_name(self, href: str) -> str | None:
        def _load() -> str | None:
            payload = self.client.get(href).json()
            for entry in payload.get("_embedded", {}).get("attributedTo", []) or []:
                if entry.get("attributedAs") != DATA_OWNER_ROLE_UUID:
                    continue
                post_id = entry.get("attributedTo")
                if not post_id:
                    continue
                name = self.fetch_post_agent_label(post_id)
                if name:
                    return name
            return None

        return self._safe_cached(
            self._data_owner_name_cache, href, _load, "attributedTo"
        )

    # --- staatskalender --------------------------------------------------

    def staatskalender_get(
        self, url: str, max_retries: int = STAATSKALENDER_MAX_RETRIES
    ) -> dict | None:
        for attempt in range(max_retries):
            response = self.staatskalender_client.get(url)
            if response.status_code == 429:
                wait = self._compute_rate_limit_wait(
                    response.headers.get("x-ratelimit-reset")
                )
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
        logger.warning(
            "staatskalender %s still rate-limited after retries; skipping", url
        )
        return None

    @staticmethod
    def _compute_rate_limit_wait(reset_header: str | None) -> float:
        if not reset_header:
            return STAATSKALENDER_DEFAULT_WAIT_SECONDS
        try:
            delta = parsedate_to_datetime(reset_header).timestamp() - time.time()
        except (TypeError, ValueError):
            return STAATSKALENDER_DEFAULT_WAIT_SECONDS
        return max(1.0, min(delta + 1.0, STAATSKALENDER_MAX_WAIT_SECONDS))

    def fetch_agency(self, state_calendar_id: int) -> dict | None:
        def _load() -> dict | None:
            payload = self.staatskalender_get(f"agencies/{state_calendar_id}")
            if payload is None:
                return None
            items = payload.get("collection", {}).get("items") or []
            return items[0] if items else None

        return self._safe_cached(
            self._agency_cache, state_calendar_id, _load, "agency"
        )

    # --- ancestry --------------------------------------------------------

    def walk_ancestors(self, item: dict) -> Iterator[tuple[str, dict]]:
        """Yield ``(href, parent)`` tuples for each public ancestor."""
        href = item.get("_links", {}).get("inCollection", {}).get("href")
        while href:
            parent = self.fetch_parent(href)
            if parent.get("publicState") and parent["publicState"] != PUBLIC_STATE:
                return
            yield href, parent
            href = parent.get("_links", {}).get("inCollection", {}).get("href")
