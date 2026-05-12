"""Generic raw-to-DCAT transform pipeline step.

Reads raw data from DuckDB (loaded by a source-specific dlt pipeline),
applies a source-specific transform function, and loads the result into
the ``i14y_dcat`` dataset.
"""

import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Callable, Generator

import dlt
import duckdb
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from pydantic import BaseModel, ValidationError

from metadataswiss_connector.resources import DUCKDB_PATH

if TYPE_CHECKING:
    from metadataswiss_connector.registry import ResourceSpec

logger = logging.getLogger(__name__)

# Column used to carry post-sync sidecar payloads through DuckDB. Stored
# as a JSON string so dlt doesn't normalise it into nested child tables.
EXTRAS_COLUMN = "_extras_json"

# Key under which extras are surfaced on records read back from DuckDB
# and consumed by sync. Underscored to keep it visually distinct from
# I14Y model fields.
EXTRAS_KEY = "__extras__"


def run_transform(
    pipeline_raw: dlt.Pipeline,
    resources: "dict[str, ResourceSpec]",
    destination,
    *,
    publisher: str,
) -> None:
    """Transform raw resources to I14Y input models and load into DuckDB.

    ``resources`` maps each raw resource/table name to its ResourceSpec
    (transform + target kind), allowing one source to carry multiple
    resource types.
    """
    pipeline_dcat = dlt.pipeline(
        pipeline_name=f"{pipeline_raw.pipeline_name}_dcat",
        destination=destination,
        dataset_name="i14y_dcat",
    )

    for resource_name, spec in resources.items():
        load_info = pipeline_dcat.run(
            _read_and_transform(
                pipeline_raw, resource_name, spec.transform, publisher=publisher
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
        children_by_parent = _load_scalar_children(
            client, pipeline.dataset_name, table_name
        )
        siblings_by_parent, lookups = _load_sibling_children(
            client, pipeline.dataset_name, table_name, parent_key="id"
        )

        with client.execute_query(f'SELECT * FROM "{table_name}"') as cursor:
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                children = {
                    field: values_by_parent.get(record["_dlt_id"], [])
                    for field, values_by_parent in children_by_parent.items()
                }
                for sibling_table, rows_by_parent_id in siblings_by_parent.items():
                    children[sibling_table] = rows_by_parent_id.get(
                        record.get("id"), []
                    )
                try:
                    result = transform_fn(
                        record, children, lookups=lookups, publisher=publisher
                    )
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
                if isinstance(result, tuple):
                    model, extras = result
                else:
                    model, extras = result, None
                out = model.model_dump(by_alias=True, exclude_none=True, mode="json")
                if extras:
                    out[EXTRAS_COLUMN] = json.dumps(extras)
                yield out


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


def _load_scalar_children(
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
            with client.execute_query(
                f'SELECT _dlt_parent_id, value FROM "{table}"'
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


def _load_sibling_children(
    client, dataset_name: str, parent_table: str, *, parent_key: str
) -> tuple[dict[str, dict[str, list[dict]]], dict[str, list[dict]]]:
    """Load top-level sibling tables alongside the parent.

    Returns ``(siblings_by_parent, lookups)``:

    - ``siblings_by_parent``: tables wired via dlt ``resolve`` +
      ``include_from_parent``; rows carry a ``_<parent>_<key>`` column and
      are grouped by parent id so ``_read_and_transform`` can attach them
      under the sibling table's name in the ``children`` dict.
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
