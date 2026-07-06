"""Unit tests for the generic I14Y DCAT builders (dcat/builders.py).

Pure functions ``loose primitive → typed model`` — no mocking needed,
since no network/IO is involved.
"""

from datetime import datetime, timezone

import pytest

from metadataswiss_connector.dcat import builders as dcat


class TestHtmlToPlainText:
    def test_none_and_empty_pass_through(self):
        assert dcat.html_to_plain_text(None) is None
        assert dcat.html_to_plain_text("") == ""

    def test_plain_text_without_markup_unchanged(self):
        assert dcat.html_to_plain_text("Ein einfacher Satz.") == "Ein einfacher Satz."

    def test_strips_tags_and_collapses_whitespace(self):
        assert dcat.html_to_plain_text("<p>Hallo</p><p>Welt</p>") == "Hallo Welt"

    def test_unescapes_entities(self):
        assert dcat.html_to_plain_text("Tom &amp; Jerry") == "Tom & Jerry"

    def test_markup_reducing_to_empty_returns_none(self):
        assert dcat.html_to_plain_text("<p>   </p>") is None


class TestMultiLanguage:
    def test_none_returns_none(self):
        assert dcat.multi_language(None) is None
        assert dcat.multi_language("") is None

    def test_default_language_is_de(self):
        model = dcat.multi_language("Titel")
        assert model.de == "Titel"
        assert model.en is None

    def test_custom_language(self):
        model = dcat.multi_language("Title", lang="en")
        assert model.en == "Title"
        assert model.de is None


class TestFileFormat:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("csv", "CSV"),
            ("CSV", "CSV"),
            ("  Pdf ", "PDF"),
            ("htm", "HTML"),
            ("geojson", "GEOJSON"),
        ],
    )
    def test_known_formats_normalised(self, value, expected):
        assert dcat.file_format(value).code == expected

    @pytest.mark.parametrize("value", [None, "", "docx", "unknown"])
    def test_unknown_or_missing_returns_none(self, value):
        assert dcat.file_format(value) is None


class TestFrequency:
    def test_uri_tail_uppercased(self):
        assert dcat.frequency("http://.../frequency/MONTHLY").code == "MONTHLY"

    def test_bare_code(self):
        assert dcat.frequency("daily").code == "DAILY"

    def test_unknown_falls_back_to_other(self):
        assert dcat.frequency("http://.../frequency/NOPE").code == "OTHER"

    def test_none_returns_none(self):
        assert dcat.frequency(None) is None


class TestKeywords:
    def test_empty_returns_empty_list(self):
        assert dcat.keywords(None) == []
        assert dcat.keywords([]) == []

    def test_each_tag_becomes_keyword_model(self):
        result = dcat.keywords(["bevölkerung", "statistik"])
        assert [k.label.de for k in result] == ["bevölkerung", "statistik"]


class TestTemporalCoverage:
    def test_no_start_returns_empty(self):
        assert dcat.temporal_coverage(None, 123) == []

    def test_start_only(self):
        result = dcat.temporal_coverage(0, None)
        assert len(result) == 1
        assert result[0].start == datetime(1970, 1, 1, tzinfo=timezone.utc)
        assert result[0].end is None

    def test_start_and_end(self):
        result = dcat.temporal_coverage(0, 1000)
        assert result[0].end == datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)


class TestLandingPages:
    def test_none_returns_empty(self):
        assert dcat.landing_pages(None) == []

    def test_uri_wrapped(self):
        assert dcat.landing_pages("https://example.org").pop().uri == "https://example.org"


class TestEpochMsToDatetime:
    def test_none_returns_none(self):
        assert dcat.epoch_ms_to_datetime(None) is None

    def test_converts_to_utc(self):
        # 1_700_000_000_000 ms = 2023-11-14T22:13:20Z
        assert dcat.epoch_ms_to_datetime(1_700_000_000_000) == datetime(
            2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc
        )
