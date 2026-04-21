"""Command-line entrypoint for the metadataswiss connector.

Usage:
    metadataswiss-connector list
    metadataswiss-connector run <source> [--steps extract,transform,publish]
    metadataswiss-connector run --all [--steps ...]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from metadataswiss_connector.config import I14YConfig
from metadataswiss_connector.dcat.transforms import read_transformed, run_transform
from metadataswiss_connector.registry import CatalogSource
from metadataswiss_connector.resources import (
    duckdb_destination,
    i14y_client_from_env,
    raw_pipeline_for,
)
from metadataswiss_connector.sources import SOURCES
from metadataswiss_connector.sync import purge_all, sync_datasets

DEFAULT_SYNC_LIMIT = 5
ALL_STEPS = ("extract", "transform", "publish")
logger = logging.getLogger("metadataswiss_connector")


def _by_name(name: str) -> CatalogSource:
    for s in SOURCES:
        if s.name == name:
            return s
    raise SystemExit(f"unknown source: {name!r} (known: {[s.name for s in SOURCES]})")


def _parse_steps(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ALL_STEPS
    steps = tuple(s.strip() for s in raw.split(",") if s.strip())
    unknown = [s for s in steps if s not in ALL_STEPS]
    if unknown:
        raise SystemExit(f"unknown step(s): {unknown} (known: {list(ALL_STEPS)})")
    return steps


def _cmd_list(_: argparse.Namespace) -> int:
    for s in SOURCES:
        print(f"{s.name}\t{', '.join(s.resources)}")
    return 0


def _step_extract(source: CatalogSource) -> None:
    pipeline = raw_pipeline_for(source)
    load_info = pipeline.run(source.dlt_source_factory())
    row_counts = pipeline.last_trace.last_normalize_info.row_counts
    for table, count in sorted(row_counts.items()):
        if not table.startswith("_dlt"):
            logger.info("extracted %s rows into %s", count, table)
    logger.info("%s", load_info)


def _step_transform(source: CatalogSource) -> None:
    cfg = I14YConfig.from_env()
    publisher = source.publisher or cfg.publisher
    run_transform(
        pipeline_raw=raw_pipeline_for(source),
        transform_fns=source.resources,
        destination=duckdb_destination(),
        publisher=publisher,
    )


def _step_publish(source: CatalogSource) -> None:
    records: list[dict] = []
    for resource_name in source.resources:
        records.extend(read_transformed(resource_name, limit=DEFAULT_SYNC_LIMIT))
    if not records:
        logger.warning("no transformed records for %s — skipping publish", source.name)
        return
    state_path = Path(f"data/{source.name}_dataset_ids.json")
    with i14y_client_from_env() as client:
        result = sync_datasets(client, records, state_path=state_path)
    logger.info("publish complete for %s: %s", source.name, result.summary())


_STEP_FNS = {
    "extract": _step_extract,
    "transform": _step_transform,
    "publish": _step_publish,
}


def _run_one(source: CatalogSource, steps: tuple[str, ...]) -> None:
    logger.info("running source %s steps=%s", source.name, list(steps))
    for step in steps:
        _STEP_FNS[step](source)


def _cmd_purge(args: argparse.Namespace) -> int:
    source = _by_name(args.source)
    state_path = Path(f"data/{source.name}_dataset_ids.json")

    if not state_path.exists():
        logger.error("no state file at %s — nothing to purge", state_path)
        return 1

    if args.dry_run:
        with i14y_client_from_env() as client:
            result = purge_all(client, state_path=state_path, dry_run=True)
        logger.info("DRY RUN — would delete %d datasets from I14Y", len(result.deleted))
        for sid in result.deleted:
            print(sid)
        return 0

    if not args.yes:
        print(
            f"About to delete all active datasets for source {source.name!r} "
            f"(state file: {state_path}) from I14Y.",
            file=sys.stderr,
        )
        confirm = input("Type 'DELETE' to confirm: ")
        if confirm != "DELETE":
            logger.info("aborted")
            return 1

    with i14y_client_from_env() as client:
        result = purge_all(client, state_path=state_path)
    logger.info("purge complete for %s: %s", source.name, result.summary())
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    steps = _parse_steps(args.steps)
    targets = SOURCES if args.all else [_by_name(args.source)]
    for s in targets:
        _run_one(s, steps)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(prog="metadataswiss-connector")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="list registered sources").set_defaults(func=_cmd_list)

    run = sub.add_parser("run", help="run pipeline steps for one or all sources")
    target = run.add_mutually_exclusive_group(required=True)
    target.add_argument("source", nargs="?", help="source name (see `list`)")
    target.add_argument("--all", action="store_true", help="run every registered source")
    run.add_argument(
        "--steps",
        help=f"comma-separated subset of {','.join(ALL_STEPS)} (default: all)",
    )
    run.set_defaults(func=_cmd_run)

    purge = sub.add_parser(
        "purge",
        help="delete all active datasets of a source from I14Y (per state file)",
    )
    purge.add_argument("source", help="source name (see `list`)")
    purge.add_argument("--yes", action="store_true", help="skip interactive confirmation")
    purge.add_argument("--dry-run", action="store_true", help="list ids without deleting")
    purge.set_defaults(func=_cmd_purge)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
