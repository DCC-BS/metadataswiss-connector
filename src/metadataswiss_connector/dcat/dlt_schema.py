"""dlt-schema introspection over DuckDB.

Discovers and loads the child / sibling tables that dlt produces when it
normalises a nested resource:

- *scalar children*: tables with a single ``value`` column (dlt's shape
  for ``list[str]`` fields like ``keywords`` / ``identifiers``),
- *sibling tables*: top-level resources in the same dataset, either
  wired to a parent via ``include_from_parent`` (carrying a
  ``_<parent>_<key>`` ref column) or unanchored lookup tables.

Source-agnostic — only knows the dlt naming conventions.
"""

from __future__ import annotations

from collections import defaultdict

from dlt.destinations.exceptions import DatabaseUndefinedRelation


def load_scalar_children(
    client, dataset_name: str, parent_table: str
) -> dict[str, dict[str, list[str]]]:
    """Discover dlt scalar-list child tables and load them grouped by parent.

    Returns ``{child_field: {parent_dlt_id: [values]}}``. Only child
    tables with a single ``value`` column (dlt's shape for ``list[str]``
    fields) are included — struct children are skipped here.
    """
    prefix = f"{parent_table}__"
    with client.execute_query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name LIKE %s",
        dataset_name,
        f"{prefix}%",
    ) as cursor:
        child_tables = [row[0] for row in cursor.fetchall()]

    result: dict[str, dict[str, list[str]]] = {}
    for table in child_tables:
        field_name = table[len(prefix):]
        values_by_parent: dict[str, list[str]] = defaultdict(list)
        try:
            # ORDER BY _dlt_list_idx so list elements keep their source
            # order deterministically across runs — without it DuckDB's
            # physical row order can vary, perturbing the payload hash and
            # triggering spurious re-publishes.
            with client.execute_query(
                f'SELECT _dlt_parent_id, value FROM "{table}" '
                "ORDER BY _dlt_parent_id, _dlt_list_idx"
            ) as cursor:
                for parent_id, value in cursor.fetchall():
                    values_by_parent[parent_id].append(value)
        except DatabaseUndefinedRelation:
            continue
        except Exception:
            # Struct child table — no bare ``value`` column. Skipped here.
            continue
        result[field_name] = values_by_parent
    return result


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
    """
    ref_col = f"_{parent_table}_{parent_key}"
    with client.execute_query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name NOT LIKE %s",
        dataset_name,
        "_dlt_%",
    ) as cursor:
        tables = [row[0] for row in cursor.fetchall()]

    result: dict[str, dict[str, list[dict]]] = {}
    lookups: dict[str, list[dict]] = {}
    for table in tables:
        if table == parent_table or table.startswith(f"{parent_table}__"):
            continue
        try:
            with client.execute_query(f'SELECT * FROM "{table}"') as cursor:
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
        except DatabaseUndefinedRelation:
            continue
        if ref_col in columns:
            rows_by_parent: dict[str, list[dict]] = defaultdict(list)
            for row in rows:
                rec = dict(zip(columns, row))
                parent_id = rec.pop(ref_col, None)
                if parent_id is None:
                    continue
                for dlt_key in ("_dlt_id", "_dlt_load_id"):
                    rec.pop(dlt_key, None)
                rows_by_parent[parent_id].append(rec)
            if rows_by_parent:
                result[table] = rows_by_parent
        elif "__" not in table:
            recs: list[dict] = []
            for row in rows:
                rec = dict(zip(columns, row))
                for dlt_key in ("_dlt_id", "_dlt_load_id"):
                    rec.pop(dlt_key, None)
                recs.append(rec)
            if recs:
                lookups[table] = recs
    return result, lookups
