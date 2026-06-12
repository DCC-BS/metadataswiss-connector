"""Framework-free configuration objects and env accessors.

These live outside of the orchestrator so the connector can be used
as a plain library (CLI, tests, notebooks). All environment access of
the connector is gathered here (the dlt source credentials are the
exception — dlt injects those itself); the accessors read at *call*
time, so containerised deployments and tests can override via the
environment without import-order concerns.
"""

import os
from pathlib import Path

from pydantic import BaseModel


def state_dir() -> Path:
    """Directory holding the per-resource remote-ID state files.

    Override via ``CONNECTOR_DATA_DIR`` to put them on a persistent
    volume in a containerised deployment; defaults to ``data/state/``
    in the working directory for local use.
    """
    return Path(os.environ.get("CONNECTOR_DATA_DIR", "data/state"))


def duckdb_path() -> str:
    """Location of the local DuckDB warehouse.

    Override via ``DUCKDB_PATH`` to point at a persistent volume in a
    containerised deployment; defaults to a file in the working
    directory for local/CLI use.
    """
    return os.environ.get("DUCKDB_PATH", "data/metadata.duckdb")


def dataspot_web_base() -> tuple[str, str] | None:
    """(base_url, database_name) of the dataspot UI, or ``None`` if unset.

    Reuses the dlt source-connection env vars ``SOURCES__DATASPOT__*``
    so deep links (e.g. in alert emails) point at the same instance the
    data was extracted from.
    """
    base = os.environ.get("SOURCES__DATASPOT__BASE_URL")
    database = os.environ.get("SOURCES__DATASPOT__DATABASE_NAME")
    if not base or not database:
        return None
    return base.rstrip("/"), database


class I14YConfig(BaseModel):
    """Connection + publisher config for the I14Y Partner API."""

    base_url: str
    token_url: str
    client_id: str
    client_secret: str
    publisher: str
    user_agent: str

    @classmethod
    def from_env(cls) -> "I14YConfig":
        return cls(
            base_url=os.environ["I14Y_BASE_URL"],
            token_url=os.environ["I14Y_TOKEN_URL"],
            client_id=os.environ["I14Y_CLIENT_ID"],
            client_secret=os.environ["I14Y_CLIENT_SECRET"],
            publisher=os.environ["I14Y_PUBLISHER_IDENTIFIER"],
            user_agent=os.environ["I14Y_USER_AGENT"],
        )
