"""Dagster jobs to purge a source's published records from I14Y.

Modelled as jobs (not assets) because purge is an imperative, destructive
side effect: assets imply additive state and would risk being triggered
by auto-materialization or backfills. One job per registered
``CatalogSource``, launched manually from the Dagster UI.

The op requires explicit confirmation in its run config: unless
``dry_run`` is true, ``confirm`` must equal ``"DELETE"``.
"""

from dagster import (
    Config,
    Failure,
    JobDefinition,
    OpExecutionContext,
    job,
    op,
)

from metadataswiss_connector.pipeline import state_path
from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.resources import i14y_client_from_env
from metadataswiss_connector.sync import SyncResult, purge

CONFIRM_TOKEN = "DELETE"


class PurgeConfig(Config):
    dry_run: bool = True
    confirm: str = ""


def build_purge_job(source: CatalogSource) -> JobDefinition:
    """Build a one-op job that purges every resource of ``source`` from I14Y."""

    op_name = f"{source.name}_purge_op"
    job_name = f"{source.name}_purge"

    @op(name=op_name)
    def _purge_op(context: OpExecutionContext, config: PurgeConfig) -> None:
        if not config.dry_run and config.confirm != CONFIRM_TOKEN:
            raise Failure(
                description=(
                    f"Refusing to purge {source.name!r}: set config.confirm "
                    f"to {CONFIRM_TOKEN!r} or enable dry_run."
                ),
            )

        targets = []
        for resource_name, spec in source.resources.items():
            path = state_path(source.name, resource_name)
            if path.exists():
                targets.append((resource_name, spec.kind, path))

        if not targets:
            context.log.warning(
                "no state files for %s — nothing to purge", source.name,
            )
            context.add_output_metadata({"deleted": 0, "failed": 0, "targets": 0})
            return

        aggregated = SyncResult()
        per_resource: dict[str, str] = {}
        with i14y_client_from_env(logger=context.log) as client:
            for resource_name, kind, path in targets:
                result = purge(client, kind=kind, state_path=path, dry_run=config.dry_run)
                per_resource[resource_name] = result.summary()
                aggregated.deleted.extend(result.deleted)
                aggregated.failed.extend(result.failed)
                context.log.info(
                    "%s%s.%s: %s",
                    "DRY RUN " if config.dry_run else "",
                    source.name, resource_name, result.summary(),
                )

        context.add_output_metadata(
            {
                "dry_run": config.dry_run,
                "deleted": len(aggregated.deleted),
                "failed": len(aggregated.failed),
                "per_resource": per_resource,
                "failed_sample": [str(f) for f in aggregated.failed[:10]],
            },
        )

    @job(name=job_name, description=f"Purge all published {source.name!r} records from I14Y.")
    def _purge_job() -> None:
        _purge_op()

    return _purge_job
