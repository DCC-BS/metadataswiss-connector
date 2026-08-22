"""Unit tests for the Dataspot→I14Y transformation (transform.py).

Tests the pure mapping ``flat record + children + lookups
→ I14Y input model``. The inputs come from versioned JSON fixtures
(``tests/fixtures/``), so Dataspot does not need to be contacted.
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
        # No structure components -> just the model, not a tuple.
        result = self._build(load_fixture)
        assert isinstance(result, DcatDatasetInputModel)

    def test_core_fields(self, load_fixture):
        ds = self._build(load_fixture)
        assert ds.title.de == "Wohnbevölkerung nach Alter"
        # HTML is converted to plain text.
        assert ds.description.de == "Jährliche Wohnbevölkerung des Kantons."
        assert ds.identifiers == ["ds-001"]
        assert ds.publisher.identifier == PUBLISHER
        assert ds.access_rights.code == "PUBLIC"
        assert ds.version == "1.2.0"
        assert ds.version_notes.de == "Korrektur der Altersklassen"

    def test_responsible_contacts_from_constants(self, load_fixture):
        ds = self._build(load_fixture)
        assert ds.responsible_person.email == constants.RESPONSIBLE_PERSON_EMAIL
        assert ds.responsible_deputy.email == constants.RESPONSIBLE_DEPUTY_EMAIL

    def test_keywords_and_themes(self, load_fixture):
        ds = self._build(load_fixture)
        assert [k.label.de for k in ds.keywords] == ["bevölkerung", "demografie"]
        # topic 001 -> 117, geoCategory society -> 106.
        assert [c.code for c in ds.themes] == ["117", "106"]

    def test_confidentiality_strongest_wins(self, load_fixture):
        # natural_person beats none.
        ds = self._build(load_fixture)
        assert ds.confidentiality_person.code == "person"

    def test_distributions_sorted_by_id_and_filtered(self, load_fixture):
        ds = self._build(load_fixture)
        # dist-c has no access_url -> filtered out; the rest sorted by id.
        ids = [d.identifier for d in ds.distributions]
        assert ids == ["dist-a", "dist-b"]
        # Missing distribution description falls back to the title.
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
        # Only the required fields; all optional sources empty. description is
        # mandatory in the I14Y model, so it is set here.
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
        # Two owners at the same depth -> smallest collection_id wins
        # (deterministic hash).
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


class TestKontaktstelleOverride:
    """``custom_properties__i14y_kontaktstelle_sk_id`` overrides the
    ancestor-walk contact point with a specific Staatskalender agency."""

    def _lookups_with_valid_ancestor(self) -> dict:
        # An ancestor with a valid, emailed agency -> would win if the
        # override didn't take priority (or didn't take priority strictly).
        return {
            "dataset_collection_path": [
                {"dataset_id": "ds-override", "collection_id": "col-child", "depth": 0}
            ],
            "collections": [{"id": "col-child", "label": "Fachstelle Statistik"}],
            "collection_agencies": [
                {"collection_id": "col-child", "email": "statistik@bs.ch"}
            ],
        }

    def test_override_used_when_set(self):
        record = {
            "id": "ds-override",
            "label": "Override-Datensatz",
            "description": "x",
            "custom_properties__i14y_kontaktstelle_sk_id": 1012,
        }
        lookups = {
            **self._lookups_with_valid_ancestor(),
            "kontaktstelle_agencies": [
                {
                    "state_calendar_id": 1012,
                    "title": "DCC Data Competence Center",
                    "email": "dcc@bs.ch",
                    "phone": "+41 61 111 11 11",
                }
            ],
        }
        ds = transform.transform_to_dataset(
            record, {}, lookups=Lookups(lookups), publisher=PUBLISHER
        )
        assert len(ds.contact_points) == 1
        cp = ds.contact_points[0]
        assert cp.has_email == "dcc@bs.ch"
        assert cp.fn.de == "DCC Data Competence Center"
        assert cp.has_telephone == "+41 61 111 11 11"

    def test_override_strict_no_fallback(self):
        # The override's agency has no email; even though the ancestor
        # path resolves to a perfectly valid agency, it must not be used.
        record = {
            "id": "ds-override",
            "label": "Override-Datensatz",
            "description": "x",
            "custom_properties__i14y_kontaktstelle_sk_id": 1012,
        }
        lookups = {
            **self._lookups_with_valid_ancestor(),
            "kontaktstelle_agencies": [
                {"state_calendar_id": 1012, "title": "DCC Data Competence Center", "email": ""}
            ],
        }
        ds = transform.transform_to_dataset(
            record, {}, lookups=Lookups(lookups), publisher=PUBLISHER
        )
        assert ds.contact_points is None


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
        # Both of its own attributes, but not the one belonging to ds-other.
        assert "attributes/attr-jahr" in ttl
        assert "attributes/attr-geschlecht" in ttl
        assert "attr-fremd" not in ttl

    def test_enumeration_component_conforms_to_concept(self, load_fixture):
        _, extras = self._build(load_fixture)
        # ReferenceObject + datatype_id -> conformsTo on the I14Y concept IRI.
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

    def test_responsible_contacts_from_constants(self, load_fixture):
        svc = self._build(load_fixture)
        assert svc.responsible_person.email == constants.RESPONSIBLE_PERSON_EMAIL
        assert svc.responsible_deputy.email == constants.RESPONSIBLE_DEPUTY_EMAIL

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
        # Without dataservice_serves_datasets rows there is no servesDatasets
        # (and no access to the sync state).
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
        assert model.identifiers == ["concept-001"]
        assert model.name.de == "Geschlecht"
        assert model.description.de == "Codeliste der Geschlechter."
        assert model.version == constants.CONCEPT_VERSION
        # Codes "1", "2", "2.1" are all numerically parsable; the codeless
        # entry e4 is ignored (as in the entries) and therefore does not
        # force a string type.
        assert model.code_list_entry_value_type == CodeListEntryValueType.numeric
        # Longest code "2.1" -> 3.
        assert model.code_list_entry_value_max_length == 3

    def test_entries_skip_missing_code_and_resolve_parent(self, load_fixture):
        _, extras = self._build(load_fixture)
        entries = {e["code"]: e for e in extras["entries"]}
        # Entry e4 without a code is skipped.
        assert set(entries) == {"1", "2", "2.1"}
        assert entries["2.1"]["parentCode"] == "2"
        assert entries["1"]["name"] == {"de": "männlich"}

    def test_entry_validity_sentinels_dropped(self, load_fixture):
        _, extras = self._build(load_fixture)
        entries = {e["code"]: e for e in extras["entries"]}
        # valid_to is the 3000-01-01 sentinel -> omitted.
        assert "validTo" not in entries["1"]
        # valid_from of e2 is the 1900-01-01 sentinel -> omitted.
        assert "validFrom" not in entries["2"]
        # valid_from of e1 is real -> present.
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
        # Codeless entries must not flip an otherwise numeric type.
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
