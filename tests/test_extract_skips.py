"""Skip handling for Dataspot endpoints that error per item.

Covers the response hook built by ``_skip_responses`` (skip vs. raise vs.
pass-through) and the drain contract the extract asset and the
quality-issues email rely on.
"""

from types import SimpleNamespace

import pytest
from dlt.sources.helpers.rest_client.exceptions import IgnoreResponseException

from metadataswiss_connector.dagster_defs.invalid_records_report import (
    build_issues,
    group_issues,
    render_text,
)
from metadataswiss_connector.sources.dataspot import source as source_mod
from metadataswiss_connector.sources.dataspot.source import (
    _remember_label,
    _skip_responses,
    drain_extract_skips,
)

BAD_ID = "9514f6f1-13cf-4f47-9f1d-e01ca4bd8ecd"


def _response(status: int, text: str = "boom") -> SimpleNamespace:
    return SimpleNamespace(
        status_code=status,
        url=f"https://example.org/rest/db/datasets/{BAD_ID}/compositions",
        text=text,
    )


@pytest.fixture(autouse=True)
def _clean_module_state():
    drain_extract_skips()
    source_mod._parent_labels.clear()
    yield
    drain_extract_skips()
    source_mod._parent_labels.clear()


class TestSkipResponses:
    def test_unlisted_status_passes_through(self):
        hook = _skip_responses("compositions", (400, 404, 500))
        assert hook(_response(200)) is None
        assert drain_extract_skips() == []

    def test_listed_status_skips_and_records(self):
        _remember_label({"id": BAD_ID, "label": "Archäologische Zonen"})
        hook = _skip_responses("compositions", (400, 404, 500))
        with pytest.raises(IgnoreResponseException):
            hook(_response(400, "BusinessAttribute 'x' forbidden"))
        (skip,) = drain_extract_skips()
        assert skip["resource"] == "compositions"
        assert skip["id"] == BAD_ID
        assert skip["title"] == "Archäologische Zonen"
        assert skip["stage"] == "extract"
        assert skip["kind"] == "dataset"
        (fe,) = skip["field_errors"]
        assert fe["field"] == "compositions"
        assert "HTTP 400" in fe["message"]
        assert "forbidden" in fe["message"]

    def test_404_skips_silently_without_recording(self):
        hook = _skip_responses("compositions", (400, 404, 500))
        with pytest.raises(IgnoreResponseException):
            hook(_response(404))
        assert drain_extract_skips() == []

    def test_drain_clears(self):
        hook = _skip_responses("compositions", (400,))
        with pytest.raises(IgnoreResponseException):
            hook(_response(400))
        assert len(drain_extract_skips()) == 1
        assert drain_extract_skips() == []


class TestSkipInEmailReport:
    def test_skip_renders_as_extraction_issue(self):
        hook = _skip_responses("compositions", (400,))
        with pytest.raises(IgnoreResponseException):
            hook(_response(400, "BusinessAttribute 'x' forbidden"))
        invalid = [
            {k: v for k, v in skip.items() if k != "resource"}
            | {"asset": "dataspot_compositions_raw"}
            for skip in drain_extract_skips()
        ]
        groups = group_issues(build_issues(invalid))
        text = render_text(groups, len(invalid))
        assert "compositions (Extraction)" in text
        assert "HTTP 400" in text
        assert BAD_ID in text
