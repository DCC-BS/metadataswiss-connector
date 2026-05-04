"""Plugin registry for catalog sources.

Each source describes *what* to extract (a dlt source factory) and
*how* to map each of its resources to an I14Y DCAT model (a transform
per resource).
"""

from dataclasses import dataclass
from typing import Callable, Protocol

import dlt
from dlt.sources import DltSource
from pydantic import BaseModel

from metadataswiss_connector.dcat.transforms import run_transform


class TransformFn(Protocol):
    """Contract for mapping a raw record to an I14Y DCAT model.

    ``children`` carries dlt-flattened 1:n child tables keyed by the
    child field name (e.g. ``"tags"`` → ``["foo", "bar"]``). Sources
    that don't use children can ignore the argument.
    """

    def __call__(
        self,
        record: dict,
        children: dict[str, list],
        *,
        lookups: dict[str, list[dict]],
        publisher: str,
    ) -> BaseModel: ...


@dataclass(frozen=True)
class CatalogSource:
    """A registered catalog source.

    Attributes:
        name: Stable identifier; also used as dlt pipeline name.
        dlt_source_factory: Zero-arg callable returning a configured
              ``DltSource``. A factory (not an instance) lets each run
              build a fresh source with current credentials.
        resources: Maps dlt resource name → transform function.
        publisher: Optional publisher override. If ``None``, the caller's
              global publisher (e.g. from ``I14YConfig``) is used.
        transform_version: Bumped manually when transform logic changes.
              Records whose persisted version differs from this are
              re-published on the next sync, regardless of source
              modified date.
    """

    name: str
    dlt_source_factory: Callable[[], DltSource]
    resources: dict[str, TransformFn]
    publisher: str | None = None
    transform_version: int = 1


def run_source(
    source: CatalogSource,
    *,
    publisher: str,
    destination=None,
) -> None:
    """Extract + transform a single source end-to-end (no publish)."""
    from metadataswiss_connector.resources import duckdb_destination

    destination = destination or duckdb_destination()
    effective_publisher = source.publisher or publisher

    raw_pipeline = dlt.pipeline(
        pipeline_name=source.name,
        destination=destination,
        dataset_name=f"{source.name}_raw",
    )
    raw_pipeline.run(source.dlt_source_factory())

    run_transform(
        pipeline_raw=raw_pipeline,
        transform_fns=source.resources,
        destination=destination,
        publisher=effective_publisher,
    )
