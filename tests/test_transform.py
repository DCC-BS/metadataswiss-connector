"""Unit-Tests für die Dataspot→I14Y-Transformation (transform.py).

Getestet wird die reine Abbildung ``flacher Record + Children + Lookups
→ I14Y-InputModel``. Die Eingaben stammen aus versionierten JSON-Fixtures
(``tests/fixtures/``), sodass Dataspot nicht angesprochen werden muss.
"""

from metadataswiss_connector.dcat.lookups import Lookups
from metadataswiss_connector.sources.dataspot import transform
from metadataswiss_connector.sources.dataspot import constants
from metadataswiss_connector.dcat.i14y_models import (
    CodeListConceptInput,
    CodeListEntryValueType,
    DataServiceInputModel,
    DcatDatasetInputModel,
)


PUBLISHER = "Basel-Stadt"


class TestTransformToDataset:
    def _build(self, load_fixture):
        fx = load_fixture("dataset_full.json")
        return transform.transform_to_dataset(
            fx["record"],
            fx["children"],
            lookups=fx["lookups"],
            publisher=PUBLISHER,
        )

    def test_returns_model_without_structure(self, load_fixture):
        # Keine structure-components -> nur das Modell, kein Tupel.
        result = self._build(load_fixture)
        assert isinstance(result, DcatDatasetInputModel)

    def test_core_fields(self, load_fixture):
        ds = self._build(load_fixture)
        assert ds.title.de == "Wohnbevölkerung nach Alter"
        # HTML wird zu Klartext.
        assert ds.description.de == "Jährliche Wohnbevölkerung des Kantons."
        assert ds.identifiers == ["ds-001"]
        assert ds.publisher.identifier == PUBLISHER
        assert ds.access_rights.code == "PUBLIC"
        assert ds.version == "1.2.0"
        assert ds.version_notes.de == "Korrektur der Altersklassen"

    def test_keywords_and_themes(self, load_fixture):
        ds = self._build(load_fixture)
        assert [k.label.de for k in ds.keywords] == ["bevölkerung", "demografie"]
        # topic 001 -> 117, geoCategory society -> 106.
        assert [c.code for c in ds.themes] == ["117", "106"]

    def test_confidentiality_strongest_wins(self, load_fixture):
        # natural_person schlägt none.
        ds = self._build(load_fixture)
        assert ds.confidentiality_person.code == "person"

    def test_distributions_sorted_by_id_and_filtered(self, load_fixture):
        ds = self._build(load_fixture)
        # dist-c hat keine access_url -> herausgefiltert; Rest nach id sortiert.
        ids = [d.identifier for d in ds.distributions]
        assert ids == ["dist-a", "dist-b"]
        # Fehlende Distribution-Beschreibung fällt auf den Titel zurück.
        json_dist = next(d for d in ds.distributions if d.identifier == "dist-a")
        assert json_dist.description.de == "JSON-Export"
        assert json_dist.format.code == "JSON"

    def test_contact_point_from_nearest_agency(self, load_fixture):
        ds = self._build(load_fixture)
        assert len(ds.contact_points) == 1
        cp = ds.contact_points[0]
        assert cp.has_email == "statistik@bs.ch"
        assert cp.fn.de == "Fachstelle Statistik"
        assert cp.has_address.de == "Binningerstrasse 6\n4051 Basel"

    def test_data_owner_from_ancestor(self, load_fixture):
        ds = self._build(load_fixture)
        assert ds.data_owner == "Präsidialdepartement"

    def test_retention_complement_combined(self, load_fixture):
        ds = self._build(load_fixture)
        assert ds.retention_period_complement.de == (
            "Aufbewahrungsfrist (Jahre): 10\nBegründung: gesetzliche Aufbewahrung"
        )


class TestTransformToDatasetEdgeCases:
    def test_minimal_record(self):
        # Nur die Pflichtfelder; alle optionalen Quellen leer. description ist
        # im I14Y-Modell zwingend, daher hier gesetzt.
        record = {"id": "ds-min", "label": "Minimal", "description": "Beschreibung"}
        ds = transform.transform_to_dataset(
            record, {}, lookups=Lookups(), publisher=PUBLISHER
        )
        assert ds.title.de == "Minimal"
        assert ds.distributions is None
        assert ds.contact_points is None
        assert ds.themes is None
        assert ds.data_owner is None

    def test_data_owner_depth_tie_broken_by_collection_id(self):
        # Zwei Owner auf gleicher Tiefe -> kleinste collection_id gewinnt
        # (deterministischer Hash).
        record = {"id": "ds-tie", "label": "Tie", "description": "x"}
        lookups = {
            "dataset_collection_path": [
                {"dataset_id": "ds-tie", "collection_id": "col-z", "depth": 0},
                {"dataset_id": "ds-tie", "collection_id": "col-a", "depth": 0},
            ],
            "collection_data_owners": [
                {"collection_id": "col-z", "name": "Z-Amt"},
                {"collection_id": "col-a", "name": "A-Amt"},
            ],
        }
        ds = transform.transform_to_dataset(
            record, {}, lookups=Lookups(lookups), publisher=PUBLISHER
        )
        assert ds.data_owner == "A-Amt"


class TestTransformToDatasetWithStructure:
    def _build(self, load_fixture):
        fx = load_fixture("dataset_with_structure.json")
        return transform.transform_to_dataset(
            fx["record"], fx["children"], lookups=fx["lookups"], publisher=PUBLISHER
        )

    def test_returns_tuple_with_structure_turtle(self, load_fixture):
        result = self._build(load_fixture)
        assert isinstance(result, tuple)
        model, extras = result
        assert isinstance(model, DcatDatasetInputModel)
        assert "structure" in extras
        assert isinstance(extras["structure"], str) and extras["structure"]

    def test_structure_contains_attribute_paths(self, load_fixture):
        _, extras = self._build(load_fixture)
        ttl = extras["structure"]
        # Beide eigenen Attribute, aber nicht das zu ds-other gehörende.
        assert "attributes/attr-jahr" in ttl
        assert "attributes/attr-geschlecht" in ttl
        assert "attr-fremd" not in ttl

    def test_enumeration_component_conforms_to_concept(self, load_fixture):
        _, extras = self._build(load_fixture)
        # ReferenceObject + datatype_id -> conformsTo auf I14Y-Concept-IRI.
        assert "concept-001" in extras["structure"]


class TestTransformToDataservice:
    def _build(self, load_fixture):
        fx = load_fixture("dataservice_full.json")
        return transform.transform_to_dataservice(
            fx["record"], fx["children"], lookups=fx["lookups"], publisher=PUBLISHER
        )

    def test_core_fields(self, load_fixture):
        svc = self._build(load_fixture)
        assert isinstance(svc, DataServiceInputModel)
        assert svc.title.de == "Bevölkerungs-API"
        assert svc.description.de == "REST-Schnittstelle zur Wohnbevölkerung."
        assert svc.identifiers == ["api-001"]
        assert svc.access_rights.code == "PUBLIC"

    def test_endpoint_url_from_scalar(self, load_fixture):
        svc = self._build(load_fixture)
        assert [r.uri for r in svc.endpoint_urls] == [
            "https://api.example.org/v1/population"
        ]

    def test_endpoint_descriptions_from_child_list(self, load_fixture):
        svc = self._build(load_fixture)
        assert [r.uri for r in svc.endpoint_descriptions] == [
            "https://api.example.org/v1/openapi.json",
            "https://api.example.org/v1/docs",
        ]

    def test_contact_point_resolved(self, load_fixture):
        svc = self._build(load_fixture)
        assert svc.contact_points[0].has_email == "statistik@bs.ch"

    def test_no_serves_datasets_when_lookup_empty(self, load_fixture):
        # Ohne dataservice_serves_datasets-Zeilen kein servesDatasets
        # (und kein Zugriff auf den Sync-State).
        svc = self._build(load_fixture)
        assert svc.serves_datasets is None


class TestTransformToConcept:
    def _build(self, load_fixture):
        fx = load_fixture("concept_codelist.json")
        return transform.transform_to_concept(
            fx["record"], fx["children"], lookups=Lookups(), publisher=PUBLISHER
        )

    def test_returns_model_and_entries(self, load_fixture):
        model, extras = self._build(load_fixture)
        assert isinstance(model, CodeListConceptInput)
        assert "entries" in extras

    def test_concept_core_fields(self, load_fixture):
        model, _ = self._build(load_fixture)
        assert model.identifier == "concept-001"
        assert model.name.de == "Geschlecht"
        assert model.description.de == "Codeliste der Geschlechter."
        assert model.version == constants.CONCEPT_VERSION
        # Codes "1", "2", "2.1" sind alle numerisch parsebar; der codelose
        # Eintrag e4 wird (wie in den Entries) ignoriert und erzwingt
        # daher kein String.
        assert model.code_list_entry_value_type == CodeListEntryValueType.numeric
        # Längster Code "2.1" -> 3.
        assert model.code_list_entry_value_max_length == 3

    def test_entries_skip_missing_code_and_resolve_parent(self, load_fixture):
        _, extras = self._build(load_fixture)
        entries = {e["code"]: e for e in extras["entries"]}
        # Eintrag e4 ohne Code wird übersprungen.
        assert set(entries) == {"1", "2", "2.1"}
        assert entries["2.1"]["parentCode"] == "2"
        assert entries["1"]["name"] == {"de": "männlich"}

    def test_entry_validity_sentinels_dropped(self, load_fixture):
        _, extras = self._build(load_fixture)
        entries = {e["code"]: e for e in extras["entries"]}
        # valid_to ist der 3000-01-01-Sentinel -> weggelassen.
        assert "validTo" not in entries["1"]
        # valid_from von e2 ist der 1900-01-01-Sentinel -> weggelassen.
        assert "validFrom" not in entries["2"]
        # valid_from von e1 ist real -> vorhanden.
        assert "validFrom" in entries["1"]


class TestBuildEntries:
    def test_skips_entries_without_code(self):
        raw = [
            {"id": "1", "code": "A", "long_text": "Alpha"},
            {"id": "2", "code": None, "long_text": "kein Code"},
        ]
        entries = transform._build_entries(raw)
        assert [e["code"] for e in entries] == ["A"]
        assert entries[0]["name"] == {"de": "Alpha"}

    def test_long_text_preferred_over_short(self):
        raw = [{"id": "1", "code": "A", "long_text": "Lang", "short_text": "L"}]
        assert transform._build_entries(raw)[0]["name"] == {"de": "Lang"}

    def test_parent_code_resolved_from_parent_id(self):
        raw = [
            {"id": "p", "code": "PARENT", "short_text": "P"},
            {"id": "c", "code": "CHILD", "short_text": "C", "parent_id": "p"},
        ]
        entries = {e["code"]: e for e in transform._build_entries(raw)}
        assert entries["CHILD"]["parentCode"] == "PARENT"
        assert "parentCode" not in entries["PARENT"]


class TestValueTypeInference:
    def test_empty_uses_default(self):
        from metadataswiss_connector.sources.dataspot.constants import (
            DEFAULT_CODE_LIST_VALUE_TYPE,
        )

        assert transform._value_type_from_entries([]) == DEFAULT_CODE_LIST_VALUE_TYPE

    def test_all_numeric(self):
        raw = [{"code": "1"}, {"code": "2.5"}]
        assert transform._value_type_from_entries(raw) == CodeListEntryValueType.numeric

    def test_any_non_numeric_is_string(self):
        raw = [{"code": "1"}, {"code": "A"}]
        assert transform._value_type_from_entries(raw) == CodeListEntryValueType.string

    def test_codeless_entries_ignored(self):
        # Einträge ohne Code dürfen einen sonst numerischen Typ nicht kippen.
        raw = [{"code": "1"}, {"code": None}, {"code": ""}, {"code": "2"}]
        assert transform._value_type_from_entries(raw) == CodeListEntryValueType.numeric

    def test_only_codeless_entries_use_default(self):
        from metadataswiss_connector.sources.dataspot.constants import (
            DEFAULT_CODE_LIST_VALUE_TYPE,
        )

        raw = [{"code": None}, {"code": ""}]
        assert transform._value_type_from_entries(raw) == DEFAULT_CODE_LIST_VALUE_TYPE


class TestMaxCodeLength:
    def test_longest_code_wins(self):
        assert transform._max_code_length([{"code": "1"}, {"code": "123"}]) == 3

    def test_floor_of_one(self):
        assert transform._max_code_length([{"code": ""}]) == 1


class TestDataserviceAccessRights:
    def test_code_from_url_tail(self):
        record = {
            "custom_properties__i14y_api_access_rights": "https://example.org/PUBLIC/"
        }
        assert transform._dataservice_access_rights(record).code == "PUBLIC"

    def test_missing_yields_empty_code(self):
        assert transform._dataservice_access_rights({}).code == ""
