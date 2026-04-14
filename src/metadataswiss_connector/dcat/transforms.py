"""Generic raw-to-DCAT transform pipeline step.

Reads raw data from DuckDB (loaded by a source-specific dlt pipeline),
applies a source-specific transform function, and loads the result into
the ``i14y_dcat`` dataset.
"""

import logging
from collections import defaultdict
from typing import Callable, Generator

import dlt
import duckdb
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from pydantic import BaseModel, ValidationError

from metadataswiss_connector.resources import DUCKDB_PATH

logger = logging.getLogger(__name__)


def run_transform(
    pipeline_raw: dlt.Pipeline,
    resource_names: list[str],
    transform_fn: Callable[..., BaseModel],
    destination,
    *,
    publisher: str,
) -> None:
    """Transform raw resources to I14Y DCAT and load into DuckDB."""
    pipeline_dcat = dlt.pipeline(
        pipeline_name=f"{pipeline_raw.pipeline_name}_dcat",
        destination=destination,
        dataset_name="i14y_dcat",
    )

    for resource_name in resource_names:
        load_info = pipeline_dcat.run(
            _read_and_transform(
                pipeline_raw, resource_name, transform_fn, publisher=publisher
            ),
            table_name=resource_name,
            write_disposition="replace",
        )
        print(load_info)


def _read_and_transform(
    pipeline: dlt.Pipeline,
    table_name: str,
    transform_fn: Callable[..., BaseModel],
    *,
    publisher: str,
) -> Generator[dict, None, None]:
    """Read a raw resource table from DuckDB and yield transformed records.

    The transform function returns a typed Pydantic model; we serialise
    with ``by_alias=True`` so the DuckDB column names (and any future
    JSON POST body) match the I14Y camelCase contract exactly.
    """
    with pipeline.sql_client() as client:
        tags_by_parent = _load_child_values(client, f"{table_name}__tags")

        with client.execute_query(f'SELECT * FROM "{table_name}"') as cursor:
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                tags = tags_by_parent.get(record["_dlt_id"], [])
                try:
                    model = transform_fn(record, tags, publisher=publisher)
                except ValidationError as exc:
                    logger.warning(
                        "Skipping invalid %s record id=%r: %s",
                        table_name,
                        record.get("id"),
                        "; ".join(
                            f"{'.'.join(str(p) for p in e['loc'])}={e['msg']}"
                            for e in exc.errors()
                        ),
                    )
                    continue
                yield model.model_dump(by_alias=True, exclude_none=True, mode="json")


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
            record = _unflatten(flat)
            # Attach child table data
            for child_name, child_rows in child_tables.items():
                if dlt_id in child_rows:
                    record[child_name] = child_rows[dlt_id]
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

    child_data: dict[str, dict[str, list]] = {}
    for table in tables:
        # e.g. "data_products__keywords" → "keywords"
        field_name = table[len(parent_table) + 2 :]
        rows_by_parent: dict[str, list] = {}
        try:
            rows = con.execute(f'SELECT * FROM {schema}."{table}"').fetchall()
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


def _load_child_values(client, table_name: str) -> dict[str, list[str]]:
    """Load values from a dlt child table, grouped by parent _dlt_id."""
    values_by_parent: dict[str, list[str]] = defaultdict(list)
    try:
        with client.execute_query(
            f'SELECT _dlt_parent_id, value FROM "{table_name}"'
        ) as cursor:
            for parent_id, value in cursor.fetchall():
                values_by_parent[parent_id].append(value)
    except DatabaseUndefinedRelation:
        pass  # Child table may not exist if no records had this field
    return values_by_parent
