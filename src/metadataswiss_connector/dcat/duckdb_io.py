"""DuckDB read-path for transformed DCAT records.

Re-nests dlt's ``parent__child`` flat columns back into nested dicts and
re-attaches child tables so the result matches the I14Y camelCase input
contract and can be fed directly into ``DcatDatasetInputModel.model_validate()``.

Also owns the two constants describing how post-sync "extras" sidecar
payloads are carried through DuckDB (column name on write, key on read).
"""

from __future__ import annotations

import json
import logging

import duckdb

from metadataswiss_connector.resources import DUCKDB_PATH

logger = logging.getLogger(__name__)

# Column used to carry post-sync sidecar payloads through DuckDB. Stored
# as a JSON string so dlt doesn't normalise it into nested child tables.
EXTRAS_COLUMN = "_extras_json"

# Key under which extras are surfaced on records read back from DuckDB
# and consumed by sync. Underscored to keep it visually distinct from
# I14Y model fields.
EXTRAS_KEY = "__extras__"


def read_transformed(
    table_name: str, *, limit: int | None = None
) -> list[dict]:
    """Read transformed DCAT records from DuckDB and reconstruct nested dicts.

    dlt flattens nested models into ``parent__child`` columns.  This function
    re-nests them so the result matches the I14Y camelCase input contract and
    can be fed directly into ``DcatDatasetInputModel.model_validate()``.
    """
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        sql = f'SELECT * FROM i14y_dcat."{table_name}"'
        if limit:
            sql += f" LIMIT {limit}"
        rows = con.execute(sql).fetchall()
        columns = [col[0] for col in con.description]

        # Load child tables (one-to-many fields like keywords, identifiers)
        child_tables = _discover_child_tables(con, "i14y_dcat", table_name)

        results = []
        for row in rows:
            flat = dict(zip(columns, row))
            dlt_id = flat.pop("_dlt_id", None)
            flat.pop("_dlt_load_id", None)
            extras_raw = flat.pop(EXTRAS_COLUMN, None)
            record = _unflatten(flat)
            # Attach child table data
            for child_name, child_rows in child_tables.items():
                if dlt_id in child_rows:
                    record[child_name] = child_rows[dlt_id]
            if extras_raw:
                try:
                    record[EXTRAS_KEY] = json.loads(extras_raw)
                except (ValueError, TypeError):
                    logger.warning(
                        "Failed to decode extras column on %s id=%r",
                        table_name, record.get("identifier") or record.get("id"),
                    )
            results.append(record)
        return results
    finally:
        con.close()


def _discover_child_tables(
    con, schema: str, parent_table: str
) -> dict[str, dict[str, list[dict]]]:
    """Find dlt child tables and load their data grouped by parent ID."""
    tables = [
        row[0]
        for row in con.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' "
            f"AND table_name LIKE '{parent_table}\\_\\_%' ESCAPE '\\'"
        ).fetchall()
    ]

    child_data: dict[str, dict[str, list[dict]]] = {}
    for table in tables:
        # e.g. "data_products__keywords" → "keywords"
        field_name = table[len(parent_table) + 2 :]
        rows_by_parent: dict[str, list] = {}
        try:
            # ORDER BY _dlt_list_idx so list elements (keywords, themes,
            # distributions, …) read back in their stored order. Without it
            # DuckDB's row order can shift between runs, changing the
            # payload hash and causing spurious re-publishes.
            rows = con.execute(
                f'SELECT * FROM {schema}."{table}" '
                "ORDER BY _dlt_parent_id, _dlt_list_idx"
            ).fetchall()
            cols = [c[0] for c in con.description]
            data_cols = [
                c for c in cols
                if c not in ("_dlt_parent_id", "_dlt_list_idx", "_dlt_id", "_dlt_root_id")
            ]
            is_scalar_list = data_cols == ["value"]
            for row in rows:
                rec = dict(zip(cols, row))
                parent_id = rec.pop("_dlt_parent_id", None)
                if not parent_id:
                    continue
                if is_scalar_list:
                    # Simple list like identifiers: ["a", "b"]
                    rows_by_parent.setdefault(parent_id, []).append(rec["value"])
                else:
                    for dlt_key in ("_dlt_list_idx", "_dlt_id", "_dlt_root_id"):
                        rec.pop(dlt_key, None)
                    rec = _unflatten(rec)
                    rows_by_parent.setdefault(parent_id, []).append(rec)
        except Exception:
            pass
        child_data[field_name] = rows_by_parent

    return child_data


def _unflatten(flat: dict) -> dict:
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
