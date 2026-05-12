"""Sync logic: reconcile local I14Y input records with remote state.

Generic core (`sync_records` / `purge_records`) reconciles a list of
transformed records against a per-resource state file via any I14Y
resource client that exposes the standard CRUD + lifecycle operations.
The public `sync` / `purge` entrypoints dispatch on a ``kind`` string
(``"dataset"`` / ``"concept"``) via the ``_KINDS`` registry.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol
from uuid import UUID

from pydantic import BaseModel

from i14y_client import I14YClient
from i14y_client.models import CodeListConceptInput, DcatDatasetInputModel
from metadataswiss_connector.dcat.transforms import EXTRAS_KEY

logger = logging.getLogger(__name__)


class ResourceClient(Protocol):
    """Duck-typed contract satisfied by ``client.datasets`` / ``client.concepts``."""

    def create(self, model: BaseModel) -> UUID: ...
    def update(self, id_: UUID | str, model: BaseModel) -> None: ...
    def publish_initial(self, id_: UUID | str) -> None: ...
    def decommission_and_delete(self, id_: UUID | str) -> None: ...
    def apply_extras(self, id_: UUID | str, extras: dict) -> None: ...


SourceIdFn = Callable[[dict], str]


def _dataset_source_id(record: dict) -> str:
    """First entry of ``identifiers`` is the stable cross-system key."""
    return str(record["identifiers"][0])


def _concept_source_id(record: dict) -> str:
    """``identifier`` (singular) is the stable cross-system key on concepts."""
    return str(record["identifier"])


@dataclass
class SyncResult:
    """Summary of a sync run."""

    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        parts = []
        if self.created:
            parts.append(f"{len(self.created)} created")
        if self.updated:
            parts.append(f"{len(self.updated)} updated")
        if self.unchanged:
            parts.append(f"{len(self.unchanged)} unchanged")
        if self.deleted:
            parts.append(f"{len(self.deleted)} deleted")
        if self.failed:
            parts.append(f"{len(self.failed)} failed")
        return ", ".join(parts) or "no changes"


class SyncState:
    """Persistent mapping of source identifier → I14Y UUID.

    Stored as a JSON file so the sync knows which records it owns on
    I14Y without having to query the remote API.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._data: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            with open(self.path) as f:
                self._data = json.load(f)
            logger.info("Loaded %d entries from state file", len(self._data))
        else:
            logger.info("No state file found at %s, starting fresh", self.path)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)
        logger.info("Saved %d entries to state file", len(self._data))

    def get_i14y_id(self, source_id: str) -> str | None:
        entry = self._data.get(source_id)
        if not entry or entry.get("deleted_at"):
            return None
        return entry["id"]

    def get_source_modified_at(self, source_id: str) -> str | None:
        entry = self._data.get(source_id)
        if not entry or entry.get("deleted_at"):
            return None
        return entry.get("source_modified_at")

    def get_transform_version(self, source_id: str) -> int | None:
        entry = self._data.get(source_id)
        if not entry or entry.get("deleted_at"):
            return None
        return entry.get("transform_version")

    def contains(self, source_id: str) -> bool:
        entry = self._data.get(source_id)
        return entry is not None and not entry.get("deleted_at")

    def add(
        self,
        source_id: str,
        i14y_id: str,
        source_modified_at: str | None = None,
        transform_version: int | None = None,
    ) -> None:
        existing = self._data.get(source_id) or {}
        # Re-creation after a soft-delete: the I14Y UUID is new, so
        # created_at must reflect the current incarnation, not the
        # previous one.
        is_recreation = bool(existing.get("deleted_at"))
        now = datetime.now(timezone.utc).isoformat()
        if is_recreation or not existing.get("created_at"):
            created_at = now
        else:
            created_at = existing["created_at"]
        entry: dict = {
            "id": i14y_id,
            "created_at": created_at,
            "updated_at": now,
        }
        if source_modified_at is not None:
            entry["source_modified_at"] = source_modified_at
        if transform_version is not None:
            entry["transform_version"] = transform_version
        self._data[source_id] = entry

    def remove(self, source_id: str) -> None:
        entry = self._data.get(source_id)
        if not entry:
            return
        entry["deleted_at"] = datetime.now(timezone.utc).isoformat()

    def all_source_ids(self) -> set[str]:
        return {sid for sid, entry in self._data.items() if not entry.get("deleted_at")}


def sync_records(
    resource: ResourceClient,
    source_records: list[dict],
    *,
    model_class: type[BaseModel],
    source_id_for: SourceIdFn,
    state_path: Path,
    dry_run: bool = False,
    force: bool = False,
    transform_version: int | None = None,
) -> SyncResult:
    """Synchronise transformed records to I14Y via the given resource client.

    Args:
        resource: I14Y resource accessor (``client.datasets``,
                 ``client.concepts``, …) implementing ``ResourceClient``.
        source_records: dicts produced by the transform step, each
                       validating as ``model_class``.
        model_class: Pydantic input model class for the resource.
        source_id_for: Pull the stable source identifier out of a record.
                       Datasets use ``identifiers[0]``, concepts use
                       ``identifier``.
        state_path: Per-resource state file (one file per source+resource).
        dry_run: Compute the diff without calling the API.
        force: Re-publish existing records regardless of modified date /
               transform version.
        transform_version: Current transform version. Records whose
                          persisted version differs are re-published.
    """
    state = SyncState(state_path)
    source_by_id = _build_source_index(source_records, source_id_for)
    to_create, to_check, to_delete = _compute_diff(set(source_by_id), state.all_source_ids())

    logger.info(
        "Sync plan: %d new, %d to check for updates, %d to delete",
        len(to_create), len(to_check), len(to_delete),
    )

    if dry_run:
        return _dry_run_result(
            to_create, to_check, to_delete, source_by_id, state,
            force=force, transform_version=transform_version,
        )

    result = SyncResult()
    # try/finally guarantees state is persisted even on unexpected errors,
    # so successfully-created I14Y records aren't orphaned by a crash.
    try:
        _run_create_phase(
            resource, model_class, sorted(to_create), source_by_id,
            state, result, transform_version=transform_version,
        )
        _run_update_phase(
            resource, model_class, sorted(to_check), source_by_id,
            state, result, force=force, transform_version=transform_version,
        )
        _run_delete_phase(resource, sorted(to_delete), state, result)
    finally:
        state.save()

    return result


def _build_source_index(
    source_records: list[dict], source_id_for: SourceIdFn,
) -> dict[str, dict]:
    source_by_id: dict[str, dict] = {}
    for record in source_records:
        try:
            source_id = source_id_for(record)
        except (KeyError, IndexError, TypeError):
            logger.warning("Record without source identifier, skipping: %r", record)
            continue
        source_by_id[source_id] = record
    return source_by_id


def _compute_diff(
    source_ids: set[str], previous_ids: set[str],
) -> tuple[set[str], set[str], set[str]]:
    """Return (to_create, to_check, to_delete) sets."""
    return (
        source_ids - previous_ids,
        source_ids & previous_ids,
        previous_ids - source_ids,
    )


def _needs_update(
    source_id: str, record: dict, state: SyncState,
    *, force: bool, transform_version: int | None,
) -> bool:
    if force:
        return True
    source_modified = _normalize_modified(record.get("modified"))
    state_modified = state.get_source_modified_at(source_id)
    state_version = state.get_transform_version(source_id)
    if _is_unchanged(source_modified, state_modified) and _version_matches(
        transform_version, state_version
    ):
        return False
    return True


def _dry_run_result(
    to_create: set[str], to_check: set[str], to_delete: set[str],
    source_by_id: dict[str, dict], state: SyncState,
    *, force: bool, transform_version: int | None,
) -> SyncResult:
    result = SyncResult()
    result.created = sorted(to_create)
    for source_id in sorted(to_check):
        if _needs_update(
            source_id, source_by_id[source_id], state,
            force=force, transform_version=transform_version,
        ):
            result.updated.append(source_id)
        else:
            result.unchanged.append(source_id)
    result.deleted = sorted(to_delete)
    return result


def _run_create_phase(
    resource: ResourceClient, model_class: type[BaseModel],
    source_ids: list[str], source_by_id: dict[str, dict],
    state: SyncState, result: SyncResult,
    *, transform_version: int | None,
) -> None:
    for source_id in source_ids:
        _sync_create(
            resource, model_class, source_id, source_by_id[source_id],
            state, result, transform_version=transform_version,
        )


def _run_update_phase(
    resource: ResourceClient, model_class: type[BaseModel],
    source_ids: list[str], source_by_id: dict[str, dict],
    state: SyncState, result: SyncResult,
    *, force: bool, transform_version: int | None,
) -> None:
    for source_id in source_ids:
        record = source_by_id[source_id]
        if not _needs_update(
            source_id, record, state,
            force=force, transform_version=transform_version,
        ):
            logger.debug(
                "Unchanged %s (modified=%s)",
                source_id, _normalize_modified(record.get("modified")),
            )
            result.unchanged.append(source_id)
            continue

        state_version = state.get_transform_version(source_id)
        if not _version_matches(transform_version, state_version):
            logger.info(
                "Re-publishing %s: transform_version %s → %s",
                source_id, state_version, transform_version,
            )

        i14y_id = state.get_i14y_id(source_id)
        if i14y_id is None:
            # Should be impossible: _compute_diff put this id in
            # ``to_check`` because state knew about it. Treat as a soft
            # failure rather than crashing the whole sync.
            logger.error(
                "State inconsistency: %s in update set but no i14y_id", source_id
            )
            result.failed.append((source_id, "missing i14y_id in state"))
            continue

        _sync_update(
            resource, model_class, source_id,
            i14y_id, record, state,
            _normalize_modified(record.get("modified")), result,
            transform_version=transform_version,
        )


def _run_delete_phase(
    resource: ResourceClient, source_ids: list[str],
    state: SyncState, result: SyncResult,
) -> None:
    for source_id in source_ids:
        _sync_delete(resource, source_id, state.get_i14y_id(source_id), state, result)


@dataclass(frozen=True)
class _KindSpec:
    """Binding from a resource ``kind`` string to its concrete plumbing."""

    resource_attr: str
    model_class: type[BaseModel]
    source_id_for: SourceIdFn


_KINDS: dict[str, _KindSpec] = {
    "dataset": _KindSpec("datasets", DcatDatasetInputModel, _dataset_source_id),
    "concept": _KindSpec("concepts", CodeListConceptInput, _concept_source_id),
}


def _resource_for(client: I14YClient, kind: str) -> tuple[ResourceClient, _KindSpec]:
    try:
        spec = _KINDS[kind]
    except KeyError:
        raise ValueError(
            f"unknown sync kind {kind!r} (known: {sorted(_KINDS)})"
        ) from None
    return getattr(client, spec.resource_attr), spec


def sync(
    client: I14YClient,
    source_records: list[dict],
    *,
    kind: str,
    state_path: Path,
    dry_run: bool = False,
    force: bool = False,
    transform_version: int | None = None,
) -> SyncResult:
    """Sync transformed records to I14Y for the given resource ``kind``."""
    resource, spec = _resource_for(client, kind)
    return sync_records(
        resource,
        source_records,
        model_class=spec.model_class,
        source_id_for=spec.source_id_for,
        state_path=state_path,
        dry_run=dry_run,
        force=force,
        transform_version=transform_version,
    )


def _normalize_modified(value: object) -> str | None:
    """Coerce a modified value (datetime or string) to an ISO-8601 string."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _version_matches(current: int | None, persisted: int | None) -> bool:
    """Return True iff the persisted transform version is acceptable.

    If the caller doesn't supply a current version, versioning is opt-in
    and we don't force re-publishes. If the state has no recorded
    version (older entries), we conservatively treat it as a mismatch.
    """
    if current is None:
        return True
    return persisted == current


def _is_unchanged(source_modified: str | None, state_modified: str | None) -> bool:
    """Return True iff the source provides no newer modified timestamp.

    If the source lacks a modified date but the state already has one
    (set to the create-time fallback), there is no change signal and we
    treat the record as unchanged. If the state lacks a timestamp, we
    conservatively treat the record as changed.
    """
    if state_modified is None:
        return False
    if source_modified is None:
        return True
    return source_modified <= state_modified


def purge_records(
    resource: ResourceClient,
    *,
    state_path: Path,
    dry_run: bool = False,
) -> SyncResult:
    """Delete every active record recorded in the state file from I14Y.

    Active = not yet marked ``deleted_at``. Soft-deleted entries are
    skipped (already considered gone on I14Y per our records).
    On success, entries are soft-deleted in the state file.
    """
    result = SyncResult()
    state = SyncState(state_path)
    source_ids = sorted(state.all_source_ids())

    logger.warning("PURGE: %d active records will be deleted from I14Y", len(source_ids))

    if dry_run:
        result.deleted = source_ids
        return result

    try:
        _run_delete_phase(resource, source_ids, state, result)
    finally:
        state.save()
    return result


def purge(
    client: I14YClient,
    *,
    kind: str,
    state_path: Path,
    dry_run: bool = False,
) -> SyncResult:
    """Delete every active record of the given resource ``kind`` from I14Y."""
    resource, _ = _resource_for(client, kind)
    return purge_records(resource, state_path=state_path, dry_run=dry_run)


def _split_extras(record: dict) -> tuple[dict, dict | None]:
    extras = record.get(EXTRAS_KEY)
    model_input = {k: v for k, v in record.items() if k != EXTRAS_KEY}
    return model_input, extras


def _apply_extras_safely(
    resource: ResourceClient, i14y_id: UUID | str, extras: dict, source_id: str, verb: str,
) -> None:
    try:
        resource.apply_extras(i14y_id, extras)
    except Exception as exc:
        logger.warning("%s %s but failed to apply extras: %s", verb, source_id, exc)


def _sync_create(
    resource: ResourceClient,
    model_class: type[BaseModel],
    source_id: str,
    record: dict,
    state: SyncState,
    result: SyncResult,
    *,
    transform_version: int | None = None,
) -> None:
    try:
        model_input, extras = _split_extras(record)
        model = model_class.model_validate(model_input)
        i14y_id = resource.create(model)
        logger.info("Created %s → %s", source_id, i14y_id)

        if extras:
            _apply_extras_safely(resource, i14y_id, extras, source_id, "Created")

        try:
            resource.publish_initial(i14y_id)
        except Exception as exc:
            logger.warning("Created %s but failed to publish: %s", source_id, exc)

        modified = _normalize_modified(record.get("modified"))
        if modified is None:
            modified = datetime.now(timezone.utc).isoformat()
            logger.debug(
                "No modified date from source for %s, using current time %s",
                source_id, modified,
            )
        state.add(source_id, str(i14y_id), modified, transform_version=transform_version)
        result.created.append(source_id)
    except Exception as exc:
        logger.error("Failed to create %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))


def _sync_update(
    resource: ResourceClient,
    model_class: type[BaseModel],
    source_id: str,
    i14y_id: str,
    record: dict,
    state: SyncState,
    source_modified: str | None,
    result: SyncResult,
    *,
    transform_version: int | None = None,
) -> None:
    try:
        model_input, extras = _split_extras(record)
        model = model_class.model_validate(model_input)
        resource.update(i14y_id, model)
        logger.info("Updated %s (%s)", source_id, i14y_id)
        if extras:
            _apply_extras_safely(resource, i14y_id, extras, source_id, "Updated")
        state.add(source_id, i14y_id, source_modified, transform_version=transform_version)
        result.updated.append(source_id)
    except Exception as exc:
        logger.error("Failed to update %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))


def _sync_delete(
    resource: ResourceClient,
    source_id: str,
    i14y_id: str,
    state: SyncState,
    result: SyncResult,
) -> None:
    try:
        resource.decommission_and_delete(i14y_id)
        logger.info("Deleted %s (%s)", source_id, i14y_id)
        state.remove(source_id)
        result.deleted.append(source_id)
    except Exception as exc:
        logger.error("Failed to delete %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))
