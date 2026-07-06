"""Schedules for the connector.

The asset graph itself is deliberately schedule-free (see README) so the
library stays orchestrator-agnostic. For a running deployment we add one
job that materialises the full extract → transform → publish chain for
every registered source, plus a cron schedule that drives it.

The schedule ships **stopped** (``DefaultScheduleStatus.STOPPED``) so a
fresh deployment never publishes to I14Y until an operator deliberately
turns it on in the Dagster UI (*Automation* → *Schedules*). The cron
expression is read from ``CONNECTOR_SYNC_CRON`` (default: daily 03:00),
and the timezone from ``CONNECTOR_SYNC_TIMEZONE`` (default Europe/Zurich).
"""

from __future__ import annotations

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# One job covering every asset (raw → transformed → published) across all
# registered sources. Materialising the published assets pulls the whole
# upstream chain, so a single selection drives a complete sync.
full_sync_job = define_asset_job(
    name="full_sync",
    selection=AssetSelection.all(),
)

sync_schedule = ScheduleDefinition(
    name="full_sync_schedule",
    job=full_sync_job,
    cron_schedule=os.environ.get("CONNECTOR_SYNC_CRON", "0 3 * * *"),
    execution_timezone=os.environ.get("CONNECTOR_SYNC_TIMEZONE", "Europe/Zurich"),
    default_status=DefaultScheduleStatus.STOPPED,
)
