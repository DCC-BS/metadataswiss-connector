"""Cross-reference lookup tables for one transform run.

``Lookups`` wraps the ``{table: [rows]}`` dict produced by
``dlt_schema.load_sibling_children`` (plus any synthetic tables injected
by the pipeline) behind a read-only mapping with memoized per-key
indexes. Transforms are called once per record but share one instance
across the whole run, so ``by``/``unique_by`` replace repeated
full-table scans (O(records × rows)) with a single index build per
(table, key).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any


def published_ids_table(resource_name: str) -> str:
    """Name of the synthetic lookup carrying a resource's published I14Y ids.

    Injected by the pipeline (see ``pipeline.published_id_lookups``) with
    rows ``{"source_id": ..., "i14y_id": ...}`` so transforms can link to
    already-published sibling records without reading sync state.
    """
    return f"published_ids_{resource_name}"


class Lookups(Mapping[str, list[dict]]):
    """Read-only view over the lookup tables of one transform run."""

    def __init__(self, tables: Mapping[str, list[dict]] | None = None) -> None:
        self._tables: dict[str, list[dict]] = dict(tables or {})
        self._grouped: dict[tuple[str, str], dict[Any, list[dict]]] = {}
        self._unique: dict[tuple[str, str], dict[Any, dict]] = {}

    def __getitem__(self, table: str) -> list[dict]:
        return self._tables[table]

    def __iter__(self) -> Iterator[str]:
        return iter(self._tables)

    def __len__(self) -> int:
        return len(self._tables)

    def by(self, table: str, key: str) -> dict[Any, list[dict]]:
        """Rows of ``table`` grouped by ``row[key]`` (memoized).

        Rows without the key (or with a None value) are dropped; an
        unknown table yields an empty index.
        """
        cache_key = (table, key)
        grouped = self._grouped.get(cache_key)
        if grouped is None:
            grouped = {}
            for row in self._tables.get(table, []):
                value = row.get(key)
                if value is not None:
                    grouped.setdefault(value, []).append(row)
            self._grouped[cache_key] = grouped
        return grouped

    def unique_by(self, table: str, key: str) -> dict[Any, dict]:
        """One row of ``table`` per ``row[key]`` (memoized).

        For 1:1 reference tables (``collection_agencies``,
        ``collections``, …). Later rows win on duplicate keys, mirroring
        a dict comprehension over the raw rows.
        """
        cache_key = (table, key)
        unique = self._unique.get(cache_key)
        if unique is None:
            unique = {
                value: row
                for row in self._tables.get(table, [])
                if (value := row.get(key)) is not None
            }
            self._unique[cache_key] = unique
        return unique
