"""Dagster asset factory for catalog sources.

For every registered ``CatalogSource`` we generate:

- one *raw-extraction* ``@multi_asset`` per dlt pipe-root group. dlt
  fans a single root fetch out to many tables in one run (e.g. the broad
  products fetch feeds the stereotype-filtered products *and* the
  ancestry/structure transformers), and those tables are only consistent
  with each other when extracted together. We therefore group resources
  by their pipe root and emit each group as one ``can_subset=False``
  multi-asset — every table still gets its own ``<source>_<resource>_raw``
  key (with dlt-provided metadata), but a group always materialises
  atomically, so cross-resource lookups (contact points, data owners,
  structure) can never drift from the products they describe,
- one ``<source>_<resource>_transformed`` asset per resource registered
  on the source,
- one ``<source>_<resource>_published`` asset per resource (with a
  ``no_failures`` asset check on the sync result).
"""

from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    AssetSpec,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
    multi_asset,
)
from dagster_dlt import DagsterDltResource

from metadataswiss_connector.pipeline import publish_resource, transform_resource
from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.resources import raw_pipeline_for

# Concurrency pool name for every asset that opens the local DuckDB file.
# DuckDB's file lock is cross-process and exclusive: a process holding the
# file open — even ``read_only=True`` — blocks any other process from
# opening it read-write. Under Dagster's multiprocess/subprocess execution
# each asset runs in its own process, so without a single-slot pool a
# read-only ``published`` asset overlapping a read-write ``transformed``
# asset crashes the writer with a "Conflicting lock" IOError. Every
# DuckDB-touching asset (extract, transformed, AND published) therefore
# shares this pool; its limit is set in dagster.yaml (default_limit: 1).
DUCKDB_WRITER_POOL = "duckdb_writer"


def _resource_groups(dlt_source) -> dict[str, list[str]]:
    """Group resource names by the root of their dlt pipe chain.

    Resources that share a pipe root come out of one fetch + fan-out and
    must be extracted together to stay mutually consistent (e.g. the broad
    products fetch and every ancestry/structure transformer derived from
    it). Resources with different roots — products vs. code lists — are
    independent extractions and become separate groups.
    """

    def _root(resource) -> str:
        pipe = resource._pipe  # noqa: SLF001
        while pipe.parent is not None:
            pipe = pipe.parent
        return pipe.name

    groups: dict[str, list[str]] = {}
    for resource in dlt_source.resources.values():
        groups.setdefault(_root(resource), []).append(resource.name)
    return groups


def _build_raw_group(
    source: CatalogSource, root: str, resource_names: list[str]
) -> AssetsDefinition:
    """One atomic raw-extraction asset for a dlt pipe-root group.

    Emitted as a ``can_subset=False`` multi-asset so the whole group always
    materialises in a single dlt run: cross-resource lookups (contact
    points, data owners, structure) can never go stale relative to the
    products they describe, and the broad root fetch happens exactly once.
    Each table keeps its own ``<source>_<resource>_raw`` key and dlt
    metadata; we drop the per-table parent edges because the group is now a
    single indivisible extraction.
    """
    # Each pipe-root group gets its own ``group_name`` so the Dagster asset
    # graph draws it as a labelled box — making the atomic extraction units
    # visible (a multi-asset alone only renders its members as loose nodes).
    group_name = f"{source.name}_{root}"
    specs = [
        AssetSpec(
            key=AssetKey(f"{source.name}_{name}_raw"),
            group_name=group_name,
            kinds={"dlt", "duckdb"},
        )
        for name in resource_names
    ]

    @multi_asset(
        name=f"{source.name}_{root}_extract",
        specs=specs,
        can_subset=False,
        pool=DUCKDB_WRITER_POOL,
    )
    def _extract(context, dlt: DagsterDltResource):
        # Fresh source each run (dlt generators are single-use); name every
        # table in the group so each one is written (dlt would otherwise run
        # a parent only to feed its children without persisting it).
        dlt_source = source.dlt_source_factory().with_resources(*resource_names)
        pipeline = raw_pipeline_for(source)
        if source.drain_extract_skips:
            source.drain_extract_skips()  # discard residue from prior runs
        load_info = pipeline.run(dlt_source)
        skips = source.drain_extract_skips() if source.drain_extract_skips else []
        for name in resource_names:
            resource = dlt_source.resources[name]
            metadata = dlt.extract_resource_metadata(
                context, resource, load_info, pipeline
            )
            resource_skips = [s for s in skips if s.get("resource") == name]
            if resource_skips:
                # Same key/shape as the transformed/published assets, so the
                # email_on_invalid_records sensor picks these up unchanged.
                metadata["invalid_details_json"] = MetadataValue.json(resource_skips)
            yield MaterializeResult(
                asset_key=AssetKey(f"{source.name}_{name}_raw"),
                metadata=metadata,
            )

    return _extract


def build_source_assets(source: CatalogSource) -> list[AssetsDefinition]:
    """Build the asset graph for one catalog source."""
    groups = _resource_groups(source.dlt_source_factory())
    assets: list[AssetsDefinition] = [
        _build_raw_group(source, root, sorted(names))
        for root, names in groups.items()
    ]

    for resource_name in source.resources:
        assets.append(_build_transformed(source, resource_name))
        assets.append(_build_published(source, resource_name))

    return assets


def _build_transformed(source: CatalogSource, resource_name: str) -> AssetsDefinition:
    transformed_name = f"{source.name}_{resource_name}_transformed"
    spec = source.resources[resource_name]
    raw_deps = [AssetKey(f"{source.name}_{resource_name}_raw")] + [
        AssetKey(f"{source.name}_{lk}_raw") for lk in spec.lookups
    ]

    @asset(
        name=transformed_name,
        deps=raw_deps,
        group_name=f"{source.name}_transformed",
        kinds={"dlt", "duckdb"},
        pool=DUCKDB_WRITER_POOL,
    )
    def _transformed(context):
        stats = transform_resource(source, resource_name)
        invalid_details = stats.get("invalid_details", [])
        # Tag each detail with the resource kind so the alert email can
        # build the right dataspot deep-link (datasets vs enumerations).
        for d in invalid_details:
            d["kind"] = spec.kind
        return MaterializeResult(
            metadata={
                "raw_rows": stats["raw_rows"],
                "valid_records": stats["valid"],
                "invalid_records": stats["invalid"],
                "loaded_tables": stats["loaded_tables"],
                # JSON-encoded so the invalid-records sensor can read it
                # back from the materialization event verbatim.
                "invalid_details_json": MetadataValue.json(invalid_details),
            },
        )

    return _transformed


def _build_published(source: CatalogSource, resource_name: str) -> AssetsDefinition:
    transformed_name = f"{source.name}_{resource_name}_transformed"
    published_name = f"{source.name}_{resource_name}_published"
    kind = source.resources[resource_name].kind

    @asset(
        name=published_name,
        deps=[transformed_name],
        group_name=f"{source.name}_published",
        kinds={"python", "i14y"},
        # Reads DuckDB (read_only) before syncing to I14Y. A read-only handle
        # still blocks a concurrent writer cross-process, so this asset shares
        # the writer pool — see DUCKDB_WRITER_POOL.
        pool=DUCKDB_WRITER_POOL,
        check_specs=[
            AssetCheckSpec(
                name="no_failures",
                asset=published_name,
                description="Sync result contains no failed records.",
            )
        ],
    )
    def _published(context):
        result = publish_resource(
            source, resource_name, limit=None, log=context.log,
        )
        invalid_details = [
            {
                "id": f.source_id,
                "title": f.title,
                "stage": "publish",
                "errors": f.error,
                "kind": kind,
            }
            for f in result.failed
        ]
        yield MaterializeResult(
            metadata={
                "created": len(result.created),
                "updated": len(result.updated),
                "unchanged": len(result.unchanged),
                "deleted": len(result.deleted),
                "failed": len(result.failed),
                "summary": result.summary(),
                # Surface publish-time failures to the email_on_invalid_records
                # sensor using the same shape as the transformed asset.
                "invalid_details_json": MetadataValue.json(invalid_details),
            },
        )
        yield AssetCheckResult(
            check_name="no_failures",
            passed=len(result.failed) == 0,
            metadata={
                "failed_count": len(result.failed),
                "failed_sample": [str(f) for f in result.failed[:10]],
            },
        )

    return _published
