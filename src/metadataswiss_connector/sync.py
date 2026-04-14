"""Sync logic: reconcile local DCAT records with I14Y remote state.

Follows the same pattern as the official I14Y harvester template:
- A local state file tracks which source identifiers have been published
  and their corresponding I14Y UUIDs.
- New datasets are created, then set to Public / Recorded.
- Existing datasets are updated (only if modified since last sync).
- Datasets removed from the source are set to Internal, then deleted.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from i14y_client import I14YClient
from i14y_client.models import DcatDatasetInputModel

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = Path("data/dataset_ids.json")


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

    Stored as a JSON file so the sync knows which datasets it owns on
    I14Y without having to query the remote API.
    """

    def __init__(self, path: Path = DEFAULT_STATE_PATH) -> None:
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
        return entry["id"] if entry else None

    def contains(self, source_id: str) -> bool:
        return source_id in self._data

    def add(self, source_id: str, i14y_id: str) -> None:
        self._data[source_id] = {"id": i14y_id}

    def remove(self, source_id: str) -> None:
        self._data.pop(source_id, None)

    def all_source_ids(self) -> set[str]:
        return set(self._data.keys())


def sync_datasets(
    client: I14YClient,
    source_records: list[dict],
    *,
    state_path: Path = DEFAULT_STATE_PATH,
    dry_run: bool = False,
) -> SyncResult:
    """Synchronise source records to I14Y.

    Each source record must be a dict that validates as
    ``DcatDatasetInputModel``.  The first entry in ``identifiers`` is
    used as the stable key to match source ↔ I14Y.

    Args:
        client: Authenticated I14Y API client.
        source_records: Transformed DCAT records from the local store.
        state_path: Path to the JSON state file.
        dry_run: If True, compute the diff but don't call the API.

    Returns:
        SyncResult with lists of identifiers per action.
    """
    result = SyncResult()
    state = SyncState(state_path)

    # 1. Build source index: source_identifier → record dict
    source_by_id: dict[str, dict] = {}
    for record in source_records:
        identifiers = record.get("identifiers", [])
        if not identifiers:
            logger.warning("Record without identifiers, skipping: %s", record.get("title"))
            continue
        source_id = str(identifiers[0])
        source_by_id[source_id] = record

    # 2. Compute diff
    source_ids = set(source_by_id.keys())
    previous_ids = state.all_source_ids()

    to_create = source_ids - previous_ids
    to_check = source_ids & previous_ids
    to_delete = previous_ids - source_ids

    logger.info(
        "Sync plan: %d new, %d to check for updates, %d to delete",
        len(to_create), len(to_check), len(to_delete),
    )

    if dry_run:
        result.created = sorted(to_create)
        result.updated = sorted(to_check)
        result.deleted = sorted(to_delete)
        return result

    # 3. Create new datasets
    for source_id in sorted(to_create):
        _sync_create(client, source_id, source_by_id[source_id], state, result)

    # 4. Update existing datasets (always update to stay in sync)
    for source_id in sorted(to_check):
        i14y_id = state.get_i14y_id(source_id)
        _sync_update(client, source_id, i14y_id, source_by_id[source_id], result)

    # 5. Delete datasets no longer in source
    for source_id in sorted(to_delete):
        i14y_id = state.get_i14y_id(source_id)
        _sync_delete(client, source_id, i14y_id, state, result)

    # 6. Persist state
    state.save()

    return result


def _sync_create(
    client: I14YClient,
    source_id: str,
    record: dict,
    state: SyncState,
    result: SyncResult,
) -> None:
    try:
        model = DcatDatasetInputModel.model_validate(record)
        i14y_id = client.datasets.create(model)
        logger.info("Created %s → %s", source_id, i14y_id)

        # Set initial publication level and registration status
        try:
            client.datasets.set_publication_level(i14y_id, "Public")
            time.sleep(0.5)
            client.datasets.set_registration_status(i14y_id, "Recorded")
        except Exception as exc:
            logger.warning(
                "Created %s but failed to set status: %s", source_id, exc
            )

        state.add(source_id, str(i14y_id))
        result.created.append(source_id)
    except Exception as exc:
        logger.error("Failed to create %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))


def _sync_update(
    client: I14YClient,
    source_id: str,
    i14y_id: str,
    record: dict,
    result: SyncResult,
) -> None:
    try:
        model = DcatDatasetInputModel.model_validate(record)
        client.datasets.update(i14y_id, model)
        logger.info("Updated %s (%s)", source_id, i14y_id)
        result.updated.append(source_id)
    except Exception as exc:
        logger.error("Failed to update %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))


def _sync_delete(
    client: I14YClient,
    source_id: str,
    i14y_id: str,
    state: SyncState,
    result: SyncResult,
) -> None:
    try:
        # I14Y requires setting publication level to Internal before deletion
        client.datasets.set_publication_level(i14y_id, "Internal")
        time.sleep(0.5)
        client.datasets.delete(i14y_id)
        logger.info("Deleted %s (%s)", source_id, i14y_id)
        state.remove(source_id)
        result.deleted.append(source_id)
    except Exception as exc:
        logger.error("Failed to delete %s: %s", source_id, exc)
        result.failed.append((source_id, str(exc)))
