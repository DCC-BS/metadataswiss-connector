"""Unit tests for the Dataspot→I14Y vocabulary mappings (mappings.py)."""

import pytest

from metadataswiss_connector.sources.dataspot import mappings


class TestThemes:
    def test_no_input_returns_none(self):
        assert mappings.themes(None, None) is None
        assert mappings.themes([], None) is None

    def test_topic_mapped_to_theme_code(self):
        result = mappings.themes(["001"], None)
        assert [c.code for c in result] == ["117"]

    def test_geo_category_mapped(self):
        result = mappings.themes(None, "health")
        assert [c.code for c in result] == ["114"]

    def test_unknown_values_dropped(self):
        assert mappings.themes(["999"], "nope") is None

    def test_duplicates_collapsed_order_preserved(self):
        # 010, 017 and 022 all map to 115 (Wirtschaft); 001 -> 117.
        result = mappings.themes(["010", "001", "017", "022"], None)
        assert [c.code for c in result] == ["115", "117"]

    def test_topic_and_geo_combined_without_duplicate(self):
        # topic 003 -> 122 and geoCategory boundaries -> 122 -> only once.
        result = mappings.themes(["003"], "boundaries")
        assert [c.code for c in result] == ["122"]


class TestConfidentialityPerson:
    def test_none_or_empty(self):
        assert mappings.confidentiality_person(None) is None
        assert mappings.confidentiality_person([]) is None

    @pytest.mark.parametrize(
        "values,expected",
        [
            (["none"], "no_person"),
            (["natural_person"], "person"),
            (["legal_person"], "person"),
            (["particularly_protected"], "protect_person"),
        ],
    )
    def test_single_value(self, values, expected):
        assert mappings.confidentiality_person(values).code == expected

    def test_strongest_classification_wins(self):
        result = mappings.confidentiality_person(
            ["none", "natural_person", "particularly_protected"]
        )
        assert result.code == "protect_person"

    def test_unknown_value_returns_none(self):
        assert mappings.confidentiality_person(["mystery"]) is None


class TestRetentionPeriodComplement:
    def test_nothing_returns_none(self):
        assert mappings.retention_period_complement(None, None) is None
        assert mappings.retention_period_complement("", None) is None

    def test_period_only(self):
        result = mappings.retention_period_complement(10, None)
        assert result.de == "Aufbewahrungsfrist (Jahre): 10"

    def test_justification_only(self):
        result = mappings.retention_period_complement(None, "gesetzliche Vorgabe")
        assert result.de == "Begründung: gesetzliche Vorgabe"

    def test_both_combined(self):
        result = mappings.retention_period_complement(5, "weil")
        assert result.de == "Aufbewahrungsfrist (Jahre): 5\nBegründung: weil"
