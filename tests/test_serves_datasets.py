"""Tests for the ``servesDatasets`` link of I14Y DataServices.

A Dataspot API references the datasets it produces via ``derivedFrom``
derivations (qualifier ``SPEZ2``). These become ``servesDatasets`` on the
I14Y DataService. The path consists of three layers, each tested in
isolation here:

* ``DataspotEnrichment.fetch_derived_dataset_ids`` — filters the
  derivations to ``SPEZ2`` and returns the Dataspot dataset IDs,
* ``pipeline.published_id_lookups`` — reads the sync state files and
  injects a ``published_ids_*`` lookup table per resource
  (Dataspot ID → I14Y UUID) into the transform,
* ``transform._resolve_serves_datasets`` / ``transform_to_dataservice``
  — combines both and builds the ``IdModel`` list.
"""

import json

import pytest

from metadataswiss_connector.dcat.lookups import Lookups
from metadataswiss_connector.sources.dataspot import constants, transform
from metadataswiss_connector.sources.dataspot.enrichment import DataspotEnrichment

PUBLISHER = "Basel-Stadt"

# Stable example UUIDs (match the format of the I14Y registrations).
UUID_A = "a4b0f459-326b-4703-96dc-4f55fbcf225c"
UUID_B = "22222222-2222-2222-2222-222222222222"
UUID_Z = "11111111-1111-1111-1111-111111111111"


# --- Layer 1: SPEZ2 derivations from Dataspot ---------------------------


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeClient:
    """Minimal RESTClient replacement: returns a fixed payload or raises."""

    def __init__(self, payload=None, error=None):
        self._payload = payload
        self._error = error
        self.calls: list[str] = []

    def get(self, path):
        self.calls.append(path)
        if self._error is not None:
            raise self._error
        return _FakeResponse(self._payload)


def _enrichment(payload=None, error=None) -> tuple[DataspotEnrichment, _FakeClient]:
    client = _FakeClient(payload=payload, error=error)
    enrichment = DataspotEnrichment(
        base_url="https://dataspot.example",
        database_name="db",
        auth=None,
        client=client,
        staatskalender_client=_FakeClient(),
    )
    return enrichment, client


class TestFetchDerivedDatasetIds:
    def test_keeps_only_spez2_targets(self):
        q = constants.DERIVATION_QUALIFIER_PRODUCES
        payload = {
            "_embedded": {
                "derivedFrom": [
                    {"derivedFrom": "ds-a", "qualifier": q},
                    # Other qualifiers are not a "produces" relation.
                    {"derivedFrom": "ds-other", "qualifier": "SPEZ1"},
                    {"derivedFrom": "ds-c", "qualifier": q},
                    # SPEZ2 without a target ID is skipped.
                    {"qualifier": q},
                ]
            }
        }
        enrichment, client = _enrichment(payload=payload)
        assert enrichment.fetch_derived_dataset_ids("api-1") == ["ds-a", "ds-c"]
        # The correct endpoint was requested.
        assert client.calls == ["/rest/db/datasets/api-1/derivedFrom"]

    def test_no_derivations_returns_empty(self):
        enrichment, _ = _enrichment(payload={"_embedded": {}})
        assert enrichment.fetch_derived_dataset_ids("api-1") == []

    def test_missing_embedded_returns_empty(self):
        enrichment, _ = _enrichment(payload={})
        assert enrichment.fetch_derived_dataset_ids("api-1") == []

    def test_fetch_error_is_swallowed_and_cached(self):
        # An error must not abort the run; the result (no link) is cached so
        # that no further request is made.
        enrichment, client = _enrichment(error=RuntimeError("boom"))
        assert enrichment.fetch_derived_dataset_ids("api-1") == []
        assert enrichment.fetch_derived_dataset_ids("api-1") == []
        assert len(client.calls) == 1


# --- Layer 2: Dataspot ID → I14Y UUID from the sync state ---------------


def _published_ids(rows: list[dict]) -> dict[str, list[dict]]:
    """Lookup table as injected by ``pipeline.published_id_lookups``."""
    return {"published_ids_data_products": rows}


class TestPublishedIdLookups:
    @staticmethod
    def _patch_state_dir(monkeypatch, state_dir):
        # ``config.state_dir()`` reads the env variable on every call.
        monkeypatch.setenv("CONNECTOR_DATA_DIR", str(state_dir))

    def test_maps_active_entries_and_excludes_deleted(self, monkeypatch, tmp_path):
        from metadataswiss_connector.pipeline import published_id_lookups
        from metadataswiss_connector.sources.dataspot import dataspot_catalog_source

        self._patch_state_dir(monkeypatch, tmp_path)
        (tmp_path / "dataspot_data_products_ids.json").write_text(
            json.dumps(
                {
                    "ds-1": {"id": UUID_A, "payload_hash": "h"},
                    # Soft-deleted -> no longer referenceable.
                    "ds-2": {"id": UUID_B, "deleted_at": "2026-01-01T00:00:00Z"},
                    # Without an I14Y id (not yet published) -> left out.
                    "ds-3": {"payload_hash": "h"},
                }
            )
        )
        lookups = published_id_lookups(dataspot_catalog_source)
        assert lookups == _published_ids([{"source_id": "ds-1", "i14y_id": UUID_A}])

    def test_missing_state_files_yield_no_tables(self, monkeypatch, tmp_path):
        from metadataswiss_connector.pipeline import published_id_lookups
        from metadataswiss_connector.sources.dataspot import dataspot_catalog_source

        self._patch_state_dir(monkeypatch, tmp_path / "absent")
        assert published_id_lookups(dataspot_catalog_source) == {}


# --- Layer 3: Resolution + full transformation --------------------------


class TestResolveServesDatasets:
    def test_empty_lookup_yields_empty(self):
        assert transform._resolve_serves_datasets("api-1", Lookups()) == []

    def test_filters_by_dataservice_and_skips_unpublished(self):
        lookups = Lookups({
            "dataservice_serves_datasets": [
                {"dataservice_id": "api-1", "dataset_id": "ds-pub"},
                # Not yet published (not in the map) -> skipped.
                {"dataservice_id": "api-1", "dataset_id": "ds-unpub"},
                # Belongs to a different API -> ignored.
                {"dataservice_id": "api-2", "dataset_id": "ds-pub"},
            ],
            **_published_ids([{"source_id": "ds-pub", "i14y_id": UUID_A}]),
        })
        result = transform._resolve_serves_datasets("api-1", lookups)
        assert [str(r.id) for r in result] == [UUID_A]

    def test_deterministic_order_by_dataspot_id(self):
        # Ordered by Dataspot ID, independent of the lookup order, so that
        # the payload hash stays stable.
        lookups = Lookups({
            "dataservice_serves_datasets": [
                {"dataservice_id": "api-1", "dataset_id": "ds-zebra"},
                {"dataservice_id": "api-1", "dataset_id": "ds-alpha"},
            ],
            **_published_ids([
                {"source_id": "ds-zebra", "i14y_id": UUID_Z},
                {"source_id": "ds-alpha", "i14y_id": UUID_B},
            ]),
        })
        result = transform._resolve_serves_datasets("api-1", lookups)
        assert [str(r.id) for r in result] == [UUID_B, UUID_Z]


class TestTransformToDataserviceServesDatasets:
    def test_serves_datasets_serialised_under_alias(self):
        record = {"id": "api-1", "label": "Tiefbau API", "description": "x"}
        lookups = Lookups({
            "dataservice_serves_datasets": [
                {"dataservice_id": "api-1", "dataset_id": "ds-1"},
            ],
            **_published_ids([{"source_id": "ds-1", "i14y_id": UUID_A}]),
        })
        svc = transform.transform_to_dataservice(
            record, {}, lookups=lookups, publisher=PUBLISHER
        )
        assert [str(d.id) for d in svc.serves_datasets] == [UUID_A]
        dumped = svc.model_dump(by_alias=True, exclude_none=True, mode="json")
        assert dumped["servesDatasets"] == [{"id": UUID_A}]

    def test_no_link_omits_field(self):
        # No matching row -> servesDatasets stays None and drops out of the
        # payload (exclude_none).
        record = {"id": "api-1", "label": "Tiefbau API", "description": "x"}
        lookups = Lookups({
            "dataservice_serves_datasets": [
                {"dataservice_id": "api-other", "dataset_id": "ds-1"},
            ],
            **_published_ids([{"source_id": "ds-1", "i14y_id": UUID_A}]),
        })
        svc = transform.transform_to_dataservice(
            record, {}, lookups=lookups, publisher=PUBLISHER
        )
        assert svc.serves_datasets is None
