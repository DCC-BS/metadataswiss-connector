"""Tests für die ``servesDatasets``-Verknüpfung von I14Y-DataServices.

Eine Dataspot-API verweist über ``derivedFrom``-Derivationen (Qualifier
``SPEZ2``) auf die Datasets, die sie produziert. Diese werden zu
``servesDatasets`` auf dem I14Y-DataService. Der Pfad besteht aus drei
Schichten, die hier je isoliert getestet werden:

* ``DataspotEnrichment.fetch_derived_dataset_ids`` — filtert die
  Derivationen auf ``SPEZ2`` und liefert die Dataspot-Dataset-IDs,
* ``pipeline.published_id_lookups`` — liest die Sync-State-Dateien und
  injiziert pro Resource eine ``published_ids_*``-Lookup-Tabelle
  (Dataspot-ID → I14Y-UUID) in den Transform,
* ``transform._resolve_serves_datasets`` / ``transform_to_dataservice``
  — fügt beides zusammen und baut die ``IdModel``-Liste.
"""

import json

import pytest

from metadataswiss_connector.dcat.lookups import Lookups
from metadataswiss_connector.sources.dataspot import constants, transform
from metadataswiss_connector.sources.dataspot.enrichment import DataspotEnrichment

PUBLISHER = "Basel-Stadt"

# Stabile Beispiel-UUIDs (entsprechen dem Format der I14Y-Registrierungen).
UUID_A = "a4b0f459-326b-4703-96dc-4f55fbcf225c"
UUID_B = "22222222-2222-2222-2222-222222222222"
UUID_Z = "11111111-1111-1111-1111-111111111111"


# --- Schicht 1: SPEZ2-Derivationen aus Dataspot -------------------------


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeClient:
    """Minimaler RESTClient-Ersatz: liefert eine feste Payload oder wirft."""

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
                    # Andere Qualifier sind keine "produziert"-Relation.
                    {"derivedFrom": "ds-other", "qualifier": "SPEZ1"},
                    {"derivedFrom": "ds-c", "qualifier": q},
                    # SPEZ2 ohne Ziel-ID wird übersprungen.
                    {"qualifier": q},
                ]
            }
        }
        enrichment, client = _enrichment(payload=payload)
        assert enrichment.fetch_derived_dataset_ids("api-1") == ["ds-a", "ds-c"]
        # Korrekter Endpunkt wurde angefragt.
        assert client.calls == ["/rest/db/datasets/api-1/derivedFrom"]

    def test_no_derivations_returns_empty(self):
        enrichment, _ = _enrichment(payload={"_embedded": {}})
        assert enrichment.fetch_derived_dataset_ids("api-1") == []

    def test_missing_embedded_returns_empty(self):
        enrichment, _ = _enrichment(payload={})
        assert enrichment.fetch_derived_dataset_ids("api-1") == []

    def test_fetch_error_is_swallowed_and_cached(self):
        # Ein Fehler darf den Lauf nicht abbrechen; das Ergebnis (kein Link)
        # wird gecacht, sodass nicht erneut angefragt wird.
        enrichment, client = _enrichment(error=RuntimeError("boom"))
        assert enrichment.fetch_derived_dataset_ids("api-1") == []
        assert enrichment.fetch_derived_dataset_ids("api-1") == []
        assert len(client.calls) == 1


# --- Schicht 2: Dataspot-ID → I14Y-UUID aus dem Sync-State --------------


def _published_ids(rows: list[dict]) -> dict[str, list[dict]]:
    """Lookup-Tabelle, wie sie ``pipeline.published_id_lookups`` injiziert."""
    return {"published_ids_data_products": rows}


class TestPublishedIdLookups:
    @staticmethod
    def _patch_state_dir(monkeypatch, state_dir):
        # ``config.state_dir()`` liest die Env-Variable bei jedem Aufruf.
        monkeypatch.setenv("CONNECTOR_DATA_DIR", str(state_dir))

    def test_maps_active_entries_and_excludes_deleted(self, monkeypatch, tmp_path):
        from metadataswiss_connector.pipeline import published_id_lookups
        from metadataswiss_connector.sources.dataspot import dataspot_catalog_source

        self._patch_state_dir(monkeypatch, tmp_path)
        (tmp_path / "dataspot_data_products_ids.json").write_text(
            json.dumps(
                {
                    "ds-1": {"id": UUID_A, "payload_hash": "h"},
                    # Soft-deleted -> nicht mehr referenzierbar.
                    "ds-2": {"id": UUID_B, "deleted_at": "2026-01-01T00:00:00Z"},
                    # Ohne I14Y-id (noch nicht publiziert) -> ausgelassen.
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


# --- Schicht 3: Auflösung + Gesamttransformation ------------------------


class TestResolveServesDatasets:
    def test_empty_lookup_yields_empty(self):
        assert transform._resolve_serves_datasets("api-1", Lookups()) == []

    def test_filters_by_dataservice_and_skips_unpublished(self):
        lookups = Lookups({
            "dataservice_serves_datasets": [
                {"dataservice_id": "api-1", "dataset_id": "ds-pub"},
                # Noch nicht publiziert (nicht im Map) -> übersprungen.
                {"dataservice_id": "api-1", "dataset_id": "ds-unpub"},
                # Gehört zu einer anderen API -> ignoriert.
                {"dataservice_id": "api-2", "dataset_id": "ds-pub"},
            ],
            **_published_ids([{"source_id": "ds-pub", "i14y_id": UUID_A}]),
        })
        result = transform._resolve_serves_datasets("api-1", lookups)
        assert [str(r.id) for r in result] == [UUID_A]

    def test_deterministic_order_by_dataspot_id(self):
        # Reihenfolge nach Dataspot-ID, unabhängig von der Lookup-Reihenfolge,
        # damit der Payload-Hash stabil bleibt.
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
        # Keine passende Zeile -> servesDatasets bleibt None und fällt aus
        # dem Payload (exclude_none) heraus.
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
