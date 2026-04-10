"""Generic raw-to-DCAT transform pipeline step.

Reads raw data from DuckDB (loaded by a source-specific dlt pipeline),
applies a source-specific transform function, and loads the result into
the ``i14y_dcat`` dataset.
"""

from collections import defaultdict
from typing import Callable, Generator

import dlt
from dlt.destinations.exceptions import DatabaseUndefinedRelation


def run_transform(
    pipeline_raw: dlt.Pipeline,
    resource_names: list[str],
    transform_fn: Callable[[dict, list[str] | None], dict],
    destination,
) -> None:
    """Transform raw resources to I14Y DCAT and load into DuckDB."""
    pipeline_dcat = dlt.pipeline(
        pipeline_name=f"{pipeline_raw.pipeline_name}_dcat",
        destination=destination,
        dataset_name="i14y_dcat",
    )

    for resource_name in resource_names:
        load_info = pipeline_dcat.run(
            _read_and_transform(pipeline_raw, resource_name, transform_fn),
            table_name=resource_name,
            write_disposition="replace",
        )
        print(load_info)


def _read_and_transform(
    pipeline: dlt.Pipeline,
    table_name: str,
    transform_fn: Callable[[dict, list[str] | None], dict],
) -> Generator[dict, None, None]:
    """Read a raw resource table from DuckDB and yield transformed records."""
    with pipeline.sql_client() as client:
        tags_by_parent = _load_child_values(client, f"{table_name}__tags")

        with client.execute_query(f'SELECT * FROM "{table_name}"') as cursor:
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                tags = tags_by_parent.get(record["_dlt_id"], [])
                yield transform_fn(record, tags)


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
