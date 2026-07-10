"""Sibling-table wiring between raw dlt tables and the transform step.

Regression tests for the ``data_products`` → ``data_products_all`` rename:
dlt names the parent-ref column on resolved child resources after the
*extract* resource (``_data_products_all_id``), while the transform reads
the filtered ``data_products`` transformer table. Without an explicit
``sibling_parent``, the ref column doesn't match and sibling rows
(distributions) silently fall into the lookups bucket instead of reaching
the transform's ``children`` — datasets then publish without
distributions and no error surfaces anywhere.
"""

from contextlib import contextmanager

import duckdb
import pytest
from pydantic import BaseModel

from metadataswiss_connector.dcat.dlt_schema import load_sibling_children
from metadataswiss_connector.dcat.transforms import _read_and_transform
from metadataswiss_connector.sources.dataspot import dataspot_catalog_source

RAW_SCHEMA = "dataspot_raw"


class _FakeSqlClient:
    """Minimal stand-in for dlt's sql_client over a raw duckdb connection."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def __enter__(self) -> "_FakeSqlClient":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    @contextmanager
    def execute_query(self, sql: str):
        yield self._con.execute(sql)


class _FakePipeline:
    """Duck-typed dlt pipeline: just ``dataset_name`` + ``sql_client()``."""

    dataset_name = RAW_SCHEMA

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def sql_client(self) -> _FakeSqlClient:
        return _FakeSqlClient(self._con)


@pytest.fixture
def raw_con():
    """In-memory duckdb mimicking the dlt raw schema after an extract.

    ``distributions`` carries ``_data_products_all_id`` — the ref column
    dlt derives from the broad extract resource the children resolve
    from, not from the ``data_products`` transformer table the transform
    reads. ``collections`` has no ref column (plain lookup table).
    """
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE SCHEMA {RAW_SCHEMA}")
    con.execute(f"USE memory.{RAW_SCHEMA}")
    con.execute(
        "CREATE TABLE data_products ("
        "id VARCHAR, label VARCHAR, _dlt_id VARCHAR)"
    )
    con.execute("INSERT INTO data_products VALUES ('ds-1', 'Dataset One', 'r1')")
    con.execute(
        "CREATE TABLE distributions ("
        "id VARCHAR, label VARCHAR, access_url VARCHAR, "
        "_data_products_all_id VARCHAR, _dlt_id VARCHAR)"
    )
    con.execute(
        "INSERT INTO distributions VALUES "
        "('dist-1', 'CSV', 'https://example.org/data.csv', 'ds-1', 'r2'), "
        "('dist-2', 'JSON', 'https://example.org/data.json', 'ds-1', 'r3'), "
        "('dist-x', 'Orphan', 'https://example.org/x.csv', 'ds-other', 'r4')"
    )
    con.execute("CREATE TABLE collections (id VARCHAR, label VARCHAR, _dlt_id VARCHAR)")
    con.execute("INSERT INTO collections VALUES ('col-1', 'Collection', 'r5')")
    yield con
    con.close()


class TestLoadSiblingChildren:
    def test_ref_parent_matches_extract_resource_column(self, raw_con):
        siblings, lookups = load_sibling_children(
            _FakeSqlClient(raw_con), RAW_SCHEMA, "data_products",
            parent_key="id", ref_parent="data_products_all",
        )
        by_parent = siblings["distributions"]
        assert [d["id"] for d in by_parent["ds-1"]] == ["dist-1", "dist-2"]
        assert [d["id"] for d in by_parent["ds-other"]] == ["dist-x"]
        # Ref and dlt system columns are stripped from the rows.
        assert "_data_products_all_id" not in by_parent["ds-1"][0]
        assert "_dlt_id" not in by_parent["ds-1"][0]
        # Unanchored tables still load as lookups.
        assert "collections" in lookups
        assert "distributions" not in lookups

    def test_without_ref_parent_siblings_are_lost_to_lookups(self, raw_con):
        # The pre-fix behaviour: ``_data_products_id`` never matches, so
        # distributions silently degrade to an (unused) lookup table.
        siblings, lookups = load_sibling_children(
            _FakeSqlClient(raw_con), RAW_SCHEMA, "data_products",
            parent_key="id",
        )
        assert "distributions" not in siblings
        assert "distributions" in lookups


class _StubModel(BaseModel):
    id: str


class TestReadAndTransformSiblingWiring:
    def _run(self, raw_con, sibling_parent):
        seen: dict[str, dict] = {}

        def stub_transform(record, children, *, lookups, publisher):
            seen[record["id"]] = children
            return _StubModel(id=record["id"])

        rows = list(
            _read_and_transform(
                _FakePipeline(raw_con), "data_products", stub_transform,
                publisher="test", sibling_parent=sibling_parent,
            )
        )
        assert [r["id"] for r in rows] == ["ds-1"]
        return seen

    def test_distributions_reach_transform_children(self, raw_con):
        spec = dataspot_catalog_source.resources["data_products"]
        seen = self._run(raw_con, spec.sibling_parent)
        dists = seen["ds-1"]["distributions"]
        assert [d["id"] for d in dists] == ["dist-1", "dist-2"]
        assert dists[0]["access_url"] == "https://example.org/data.csv"

    def test_without_sibling_parent_children_stay_empty(self, raw_con):
        # Documents the failure mode this module guards against.
        seen = self._run(raw_con, None)
        assert not seen["ds-1"].get("distributions")


def test_data_products_spec_declares_extract_parent():
    """The registry must name the broad extract resource the distribution
    (and composition) child tables resolve from — see source.py."""
    spec = dataspot_catalog_source.resources["data_products"]
    assert spec.sibling_parent == "data_products_all"
