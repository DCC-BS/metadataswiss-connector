"""Dagster entrypoint for the metadataswiss connector.

Exposes one ``Definitions`` object that materialises the connector's
extract/transform/publish steps as assets, one set per registered
``CatalogSource``. Pointed to by ``[tool.dagster].module_name`` in
``pyproject.toml`` so ``dagster dev`` can discover it.
"""

from __future__ import annotations

# ``.env`` is loaded in ``metadataswiss_connector/__init__.py`` (the parent
# package), which Python imports before this module, so the environment is
# already populated by the time the source modules below are imported.

from dagster import Definitions
from dagster_dlt import DagsterDltResource

from metadataswiss_connector.dagster_defs.assets import build_source_assets
from metadataswiss_connector.dagster_defs.email_alerts import (
    email_on_invalid_records,
    email_on_run_failure,
)
from metadataswiss_connector.dagster_defs.purge_jobs import build_purge_job
from metadataswiss_connector.dagster_defs.schedules import full_sync_job, sync_schedule
from metadataswiss_connector.sources import SOURCES

defs = Definitions(
    assets=[asset for source in SOURCES for asset in build_source_assets(source)],
    jobs=[build_purge_job(source) for source in SOURCES] + [full_sync_job],
    schedules=[sync_schedule],
    resources={"dlt": DagsterDltResource()},
    sensors=[email_on_run_failure, email_on_invalid_records],
)
