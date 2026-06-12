"""dlt-schema introspection and re-nesting over DuckDB.

Single home for the dlt naming conventions: child tables named
``<parent>__<field>``, ``_dlt_*`` system columns, list order via
``_dlt_list_idx``, and ``a__b__c`` column flattening. Both read paths —
the transform step (dlt ``sql_client``) and the publish step (raw duckdb
connection) — share the loaders here through a small query adapter, so
rules like "ORDER BY ``_dlt_list_idx`` or the payload hash flips" live
in exactly one place.

Loaded shapes:

- *scalar children*: child tables with a single ``value`` data column
  (dlt's shape for ``list[str]`` fields like ``keywords``), yielded as
  bare values,
- *struct children*: child tables with real columns, yielded as
  re-nested dicts,
- *sibling tables*: top-level resources in the same dataset, either
  wired to a parent via ``include_from_parent`` (carrying a
  ``_<parent>_<key>`` ref column) or unanchored lookup tables.

Source-agnostic — only knows the dlt naming conventions.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Callable

import duckdb
from dlt.destinations.exceptions import DatabaseUndefinedRelation

# Run a SQL statement, return (column_names, rows). The loaders below are
# written against this adapter so they work over both a dlt sql_client
# and a raw duckdb connection.
QueryFn = Callable[[str], tuple[list[str], list[tuple]]]

# A table listed by information_schema can in principle vanish before we
# read it; both connection flavours raise their own "no such table" error.
_UNDEFINED_TABLE_ERRORS = (DatabaseUndefinedRelation, duckdb.CatalogException)

# dlt system columns, stripped from records before they reach transforms.
_DLT_COLUMNS = (
    "_dlt_parent_id",
    "_dlt_list_idx",
    "_dlt_id",
    "_dlt_root_id",
    "_dlt_load_id",
)


def duckdb_query(con: duckdb.DuckDBPyConnection) -> QueryFn:
    """Adapt a raw duckdb connection to ``QueryFn``."""

    def query(sql: str) -> tuple[list[str], list[tuple]]:
        rows = con.execute(sql).fetchall()
        return [col[0] for col in con.description], rows

    return query


def sql_client_query(client) -> QueryFn:
    """Adapt a dlt ``sql_client`` to ``QueryFn``."""

    def query(sql: str) -> tuple[list[str], list[tuple]]:
        with client.execute_query(sql) as cursor:
            return [col[0] for col in cursor.description], cursor.fetchall()

    return query


def _sql_str(value: str) -> str:
    """Quote a string as a SQL literal (identifiers come from our own
    schema introspection, but escape anyway)."""
    return "'" + value.replace("'", "''") + "'"


def _tables_in_schema(query: QueryFn, schema: str) -> list[str]:
    _columns, rows = query(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = {_sql_str(schema)}"
    )
    return [row[0] for row in rows]


def _strip_dlt_columns(rec: dict) -> dict:
    for key in _DLT_COLUMNS:
        rec.pop(key, None)
    return rec


def unflatten(flat: dict) -> dict:
    """Convert dlt's ``a__b__c`` flat keys back into nested dicts.

    Example: ``{"title__de": "Foo"}`` → ``{"title": {"de": "Foo"}}``

    Drops keys whose values are None.
    """
    nested: dict = {}
    for key, value in flat.items():
        if value is None:
            continue
        parts = key.split("__")
        target = nested
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = value
    return nested


def load_child_tables(
    query: QueryFn,
    schema: str,
    parent_table: str,
    *,
    scalars_only: bool = False,
) -> dict[str, dict[str, list]]:
    """Discover and load dlt child tables grouped by parent ``_dlt_id``.

    Returns ``{child_field: {parent_dlt_id: [item, ...]}}``. Scalar-list
    children yield bare values; struct children yield re-nested dicts
    (or are skipped entirely with ``scalars_only``). Rows are ordered by
    ``_dlt_list_idx`` so list elements keep their source order
    deterministically across runs — without it DuckDB's physical row
    order can vary, perturbing the payload hash and triggering spurious
    re-publishes.
    """
    prefix = f"{parent_table}__"
    result: dict[str, dict[str, list]] = {}
    for table in _tables_in_schema(query, schema):
        if not table.startswith(prefix):
            continue
        field_name = table[len(prefix):]
        try:
            columns, rows = query(
                f'SELECT * FROM "{schema}"."{table}" '
                "ORDER BY _dlt_parent_id, _dlt_list_idx"
            )
        except _UNDEFINED_TABLE_ERRORS:
            continue
        data_columns = [c for c in columns if c not in _DLT_COLUMNS]
        is_scalar_list = data_columns == ["value"]
        if scalars_only and not is_scalar_list:
            continue
        by_parent: dict[str, list] = defaultdict(list)
        for row in rows:
            rec = dict(zip(columns, row))
            parent_id = rec.get("_dlt_parent_id")
            if not parent_id:
                continue
            if is_scalar_list:
                by_parent[parent_id].append(rec["value"])
            else:
                by_parent[parent_id].append(unflatten(_strip_dlt_columns(rec)))
        result[field_name] = dict(by_parent)
    return result


def load_scalar_children(
    client, dataset_name: str, parent_table: str
) -> dict[str, dict[str, list[str]]]:
    """Load dlt scalar-list child tables grouped by parent, via sql_client.

    Returns ``{child_field: {parent_dlt_id: [values]}}``. Only child
    tables with a single ``value`` column (dlt's shape for ``list[str]``
    fields) are included — struct children are skipped here.
    """
    return load_child_tables(
        sql_client_query(client), dataset_name, parent_table, scalars_only=True
    )


def load_sibling_children(
    client, dataset_name: str, parent_table: str, *, parent_key: str
) -> tuple[dict[str, dict[str, list[dict]]], dict[str, list[dict]]]:
    """Load top-level sibling tables alongside the parent.

    Returns ``(siblings_by_parent, lookups)``:

    - ``siblings_by_parent``: tables wired via dlt ``resolve`` +
      ``include_from_parent``; rows carry a ``_<parent>_<key>`` column and
      are grouped by parent id so the caller can attach them under the
      sibling table's name in the ``children`` dict.
    - ``lookups``: tables in the same dataset with no parent ref column,
      loaded in full and keyed by table name. Used by transforms for
      cross-reference data (e.g. attribution → role/post/person).

    Sibling rows stay dlt-flattened (``custom_properties__x`` columns):
    transforms address raw records by their flat keys.
    """
    query = sql_client_query(client)
    ref_col = f"_{parent_table}_{parent_key}"

    result: dict[str, dict[str, list[dict]]] = {}
    lookups: dict[str, list[dict]] = {}
    for table in _tables_in_schema(query, dataset_name):
        if (
            table.startswith("_dlt")
            or table == parent_table
            or table.startswith(f"{parent_table}__")
        ):
            continue
        try:
            columns, rows = query(f'SELECT * FROM "{dataset_name}"."{table}"')
        except _UNDEFINED_TABLE_ERRORS:
            continue
        if ref_col in columns:
            rows_by_parent: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                rec = dict(zip(columns, row))
                parent_id = rec.pop(ref_col, None)
                if parent_id is None:
                    continue
                rows_by_parent[parent_id].append(_strip_dlt_columns(rec))
            if rows_by_parent:
                result[table] = dict(rows_by_parent)
        elif "__" not in table:
            recs = [_strip_dlt_columns(dict(zip(columns, row))) for row in rows]
            if recs:
                lookups[table] = recs
    return result, lookups
