"""DuckDB read-path for transformed DCAT records.

Re-nests dlt's ``parent__child`` flat columns back into nested dicts and
re-attaches child tables so the result matches the I14Y camelCase input
contract and can be fed directly into ``DcatDatasetInputModel.model_validate()``.
The dlt-convention plumbing (child-table discovery, ``_dlt_list_idx``
ordering, unflattening) is shared with the transform read path via
``dlt_schema``.

Also owns the two constants describing how post-sync "extras" sidecar
payloads are carried through DuckDB (column name on write, key on read).
"""

from __future__ import annotations

import json
import logging

import duckdb

from metadataswiss_connector.config import duckdb_path
from metadataswiss_connector.dcat.dlt_schema import (
    duckdb_query,
    load_child_tables,
    unflatten,
)

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
    con = duckdb.connect(duckdb_path(), read_only=True)
    try:
        sql = f'SELECT * FROM i14y_dcat."{table_name}"'
        if limit:
            sql += f" LIMIT {limit}"
        rows = con.execute(sql).fetchall()
        columns = [col[0] for col in con.description]

        # Load child tables (one-to-many fields like keywords, identifiers)
        child_tables = load_child_tables(duckdb_query(con), "i14y_dcat", table_name)

        results = []
        for row in rows:
            flat = dict(zip(columns, row))
            dlt_id = flat.pop("_dlt_id", None)
            flat.pop("_dlt_load_id", None)
            extras_raw = flat.pop(EXTRAS_COLUMN, None)
            record = unflatten(flat)
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


