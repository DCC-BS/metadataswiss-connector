"""Generic raw-to-DCAT transform pipeline step.

Reads raw data from DuckDB (loaded by a source-specific dlt pipeline),
applies a source-specific transform function, and loads the result into
the ``i14y_dcat`` dataset.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Callable, Generator

import dlt
from pydantic import BaseModel, ValidationError

from metadataswiss_connector.dcat.duckdb_io import EXTRAS_COLUMN
from metadataswiss_connector.dcat.dlt_schema import (
    load_scalar_children,
    load_sibling_children,
)
from metadataswiss_connector.dcat.lookups import Lookups

if TYPE_CHECKING:
    from metadataswiss_connector.registry import ResourceSpec

logger = logging.getLogger(__name__)

# Raw-record keys that commonly hold a human-readable title, tried in
# order so the invalid-records report can name a record instead of only
# showing its (opaque) source UUID. Dataspot rows use ``label``; the
# fallbacks keep this generic for other sources.
_TITLE_KEYS = ("label", "title", "name")

# Cap the offending value we copy into invalid_details so a malformed
# record (or a missing-field error whose ``input`` is the whole row)
# can't bloat the materialization metadata / alert email.
_MAX_INPUT_REPR = 120


def _record_title(record: dict) -> str | None:
    for key in _TITLE_KEYS:
        value = record.get(key)
        if value:
            return str(value)
    return None


def _summarize_input(value: object) -> str:
    """Compact, truncated repr of a Pydantic error's offending input."""
    if value is None:
        return "(leer)"
    text = repr(value) if not isinstance(value, str) else value
    text = text.replace("\n", " ").strip()
    if not text:
        return "(leer)"
    if len(text) > _MAX_INPUT_REPR:
        return text[: _MAX_INPUT_REPR - 1] + "…"
    return text


def _field_errors(exc: ValidationError) -> list[dict]:
    """Structured per-field errors for grouping in the alert email."""
    out: list[dict] = []
    for e in exc.errors():
        out.append(
            {
                "field": ".".join(str(p) for p in e["loc"]),
                "message": e["msg"],
                "type": e.get("type"),
                "value": _summarize_input(e.get("input")),
            }
        )
    return out


def run_transform(
    pipeline_raw: dlt.Pipeline,
    resources: "dict[str, ResourceSpec]",
    destination,
    *,
    publisher: str,
    extra_lookups: dict[str, list[dict]] | None = None,
) -> None:
    """Transform raw resources to I14Y input models and load into DuckDB.

    ``resources`` maps each raw resource/table name to its ResourceSpec
    (transform + target kind), allowing one source to carry multiple
    resource types.
    """
    for resource_name, spec in resources.items():
        run_transform_resource(
            pipeline_raw=pipeline_raw,
            resource_name=resource_name,
            spec=spec,
            destination=destination,
            publisher=publisher,
            extra_lookups=extra_lookups,
        )


def run_transform_resource(
    pipeline_raw: dlt.Pipeline,
    resource_name: str,
    spec: "ResourceSpec",
    destination,
    *,
    publisher: str,
    extra_lookups: dict[str, list[dict]] | None = None,
) -> dict:
    """Transform a single raw resource and load into the i14y_dcat dataset.

    ``extra_lookups`` are synthetic lookup tables merged over the raw
    sibling tables before they reach the transform — used by the
    pipeline to expose e.g. the published-ID maps from sync state.

    Returns a stats dict ``{valid, invalid, raw_rows, loaded_tables}`` so
    callers (e.g. Dagster assets) can surface it as materialization
    metadata and run validation-rate checks against it.
    """
    pipeline_dcat = dlt.pipeline(
        pipeline_name=f"{pipeline_raw.pipeline_name}_dcat",
        destination=destination,
        dataset_name="i14y_dcat",
    )
    stats: dict = {"valid": 0, "invalid": 0, "invalid_details": []}
    load_info = pipeline_dcat.run(
        _read_and_transform(
            pipeline_raw, resource_name, spec.transform,
            publisher=publisher, stats=stats, extra_lookups=extra_lookups,
            sibling_parent=spec.sibling_parent,
        ),
        table_name=resource_name,
        write_disposition="replace",
    )
    logger.info("%s", load_info)
    row_counts = pipeline_dcat.last_trace.last_normalize_info.row_counts
    loaded_tables = {
        t: c for t, c in row_counts.items() if not t.startswith("_dlt")
    }
    return {
        "valid": stats["valid"],
        "invalid": stats["invalid"],
        "raw_rows": stats["valid"] + stats["invalid"],
        "loaded_tables": loaded_tables,
        "invalid_details": stats["invalid_details"],
    }


def _read_and_transform(
    pipeline: dlt.Pipeline,
    table_name: str,
    transform_fn: Callable[..., BaseModel],
    *,
    publisher: str,
    stats: dict | None = None,
    extra_lookups: dict[str, list[dict]] | None = None,
    sibling_parent: str | None = None,
) -> Generator[dict, None, None]:
    """Read a raw resource table from DuckDB and yield transformed records.

    The transform function returns a typed Pydantic model; we serialise
    with ``by_alias=True`` so the DuckDB column names (and any future
    JSON POST body) match the I14Y camelCase contract exactly.
    """
    with pipeline.sql_client() as client:
        children_by_parent = load_scalar_children(
            client, pipeline.dataset_name, table_name
        )
        siblings_by_parent, raw_lookups = load_sibling_children(
            client, pipeline.dataset_name, table_name, parent_key="id",
            ref_parent=sibling_parent,
        )
        # One Lookups view per run: its memoized indexes are shared by
        # every per-record transform call below.
        lookups = Lookups({**raw_lookups, **(extra_lookups or {})})

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
                    field_errors = _field_errors(exc)
                    error_summary = "; ".join(
                        f"{fe['field']}={fe['message']}" for fe in field_errors
                    )
                    if stats is not None:
                        stats["invalid"] = stats.get("invalid", 0) + 1
                        stats.setdefault("invalid_details", []).append({
                            "id": record.get("id"),
                            "title": _record_title(record),
                            "stage": "transform",
                            "errors": error_summary,
                            "field_errors": field_errors,
                        })
                    logger.warning(
                        "Skipping invalid %s record id=%r: %s",
                        table_name, record.get("id"), error_summary,
                    )
                    continue
                if isinstance(result, tuple):
                    model, extras = result
                else:
                    model, extras = result, None
                out = model.model_dump(by_alias=True, exclude_none=True, mode="json")
                if extras:
                    out[EXTRAS_COLUMN] = json.dumps(extras)
                if stats is not None:
                    stats["valid"] = stats.get("valid", 0) + 1
                yield out
