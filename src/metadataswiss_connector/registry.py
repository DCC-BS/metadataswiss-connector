"""Plugin registry for catalog sources.

Each source describes *what* to extract (a dlt source factory) and
*how* to map each of its resources to an I14Y DCAT model (a transform
per resource).
"""

from dataclasses import dataclass
from typing import Callable, Literal, Protocol

import dlt
from dlt.sources import DltSource
from pydantic import BaseModel

from metadataswiss_connector.dcat.lookups import Lookups
from metadataswiss_connector.dcat.transforms import run_transform


# Which I14Y entity a transformed record targets. Drives sync routing
# (datasets endpoint vs concepts endpoint, state file).
ResourceKind = Literal["dataset", "concept", "dataservice"]


class TransformFn(Protocol):
    """Contract for mapping a raw record to an I14Y DCAT model.

    ``children`` carries dlt-flattened 1:n child tables keyed by the
    child field name (e.g. ``"tags"`` → ``["foo", "bar"]``). Sources
    that don't use children can ignore the argument.

    ``lookups`` is the per-run ``Lookups`` view over the source's
    cross-reference tables (plus pipeline-injected synthetic tables like
    the published-ID maps); one instance is shared across all records of
    a run so its memoized indexes amortise.

    The return value is either the typed Pydantic model alone, or a
    ``(model, extras)`` tuple. ``extras`` is a JSON-serialisable dict
    carrying sidecar payloads that the I14Y model itself can't hold —
    e.g. code-list entries, which I14Y publishes via a separate bulk
    endpoint after the parent concept is created. Extras are stored
    alongside the transformed record and replayed by sync as a
    post-create/update follow-up call.
    """

    def __call__(
        self,
        record: dict,
        children: dict[str, list],
        *,
        lookups: Lookups,
        publisher: str,
    ) -> BaseModel | tuple[BaseModel, dict]: ...


@dataclass(frozen=True)
class ResourceSpec:
    """How one dlt resource is mapped + published to I14Y.

    Attributes:
        transform: Function mapping a raw record to an I14Y input model.
        kind: Which I14Y entity the model targets — controls sync routing.
        lookups: Names of *other* raw dlt resources (in the same source)
                that this resource's transform reads via the ``children``
                or ``lookups`` argument. Declared explicitly so the
                Dagster lineage shows these cross-table reads as
                upstream deps on the transformed asset; runtime behaviour
                is unaffected (lookups are still discovered dynamically
                by ``load_sibling_children``).
        sibling_parent: Name of the *extract* dlt resource that this
                resource's sibling child tables resolve from. dlt names
                their parent-ref column ``_<extract resource>_id``, so
                this must be set whenever the transform reads a filtered
                transformer table whose children were extracted against
                a differently-named upstream resource (e.g.
                ``data_products`` reading ``distributions`` extracted
                via ``data_products_all``). ``None`` means the children
                reference this resource's own name.
    """

    transform: TransformFn
    kind: ResourceKind = "dataset"
    lookups: tuple[str, ...] = ()
    sibling_parent: str | None = None


@dataclass(frozen=True)
class CatalogSource:
    """A registered catalog source.

    Attributes:
        name: Stable identifier; also used as dlt pipeline name.
        dlt_source_factory: Zero-arg callable returning a configured
              ``DltSource``. A factory (not an instance) lets each run
              build a fresh source with current credentials.
        resources: Maps dlt resource name → ResourceSpec (transform + kind).
        publisher: Optional publisher override. If ``None``, the caller's
              global publisher (e.g. from ``I14YConfig``) is used.
        drain_extract_skips: Optional zero-arg callable returning (and
              clearing) the items the source skipped during the last
              extract (e.g. endpoints answering with server-side errors).
              Each dict must match the ``invalid_details`` shape consumed
              by the quality-issues email, plus a ``resource`` key naming
              the dlt resource it belongs to.
    """

    name: str
    dlt_source_factory: Callable[[], DltSource]
    resources: dict[str, ResourceSpec]
    publisher: str | None = None
    drain_extract_skips: Callable[[], list[dict]] | None = None


def run_source(
    source: CatalogSource,
    *,
    publisher: str,
    destination=None,
) -> None:
    """Extract + transform a single source end-to-end (no publish)."""
    # Deferred imports: registry sits below the orchestration layer
    # (pipeline imports registry), so pull these in lazily.
    from metadataswiss_connector.pipeline import published_id_lookups
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
        resources=source.resources,
        destination=destination,
        publisher=effective_publisher,
        extra_lookups=published_id_lookups(source),
    )
