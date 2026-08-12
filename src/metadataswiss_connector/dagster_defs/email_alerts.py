"""Email-alert sensors for the Dagster deployment.

Two concerns live here: SMTP transport (configured via the standard
``SMTP_*`` env vars; if any required var is missing the sensors log a
warning and skip rather than breaking the run) and the sensors that
collect run logs / invalid-record metadata. Rendering of the
invalid-records mail lives in ``invalid_records_report``.
"""

from __future__ import annotations

import json
import os
import smtplib
from email.message import EmailMessage

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    run_failure_sensor,
    run_status_sensor,
)

from metadataswiss_connector.dagster_defs.invalid_records_report import (
    build_issues,
    group_issues,
    render_attachment,
    render_html,
    render_text,
)


SMTP_SECURITY_CHOICES = ("starttls", "ssl", "none")

# Metadata is synced to I14Y at most once per day, so the alert sensors only
# need to react shortly after a run finishes — not on Dagster's default 30s
# tick. Throttle them to 5 minutes to cut idle evaluations ~10x while keeping
# alert latency low.
SENSOR_MINIMUM_INTERVAL_SECONDS = 300


def _smtp_config() -> dict[str, str] | None:
    required = ["SMTP_HOST", "SMTP_FROM", "EMAIL_TO"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        return None
    security = os.environ.get("SMTP_SECURITY", "starttls").lower()
    if security not in SMTP_SECURITY_CHOICES:
        raise ValueError(
            f"SMTP_SECURITY must be one of {SMTP_SECURITY_CHOICES}, got {security!r}"
        )
    return {
        "host": os.environ["SMTP_HOST"],
        "port": os.environ.get("SMTP_PORT", "587"),
        "user": os.environ.get("SMTP_USER", ""),
        "password": os.environ.get("SMTP_PASSWORD", ""),
        "from_addr": os.environ["SMTP_FROM"],
        "to_addrs": os.environ["EMAIL_TO"],
        "security": security,
    }


def _collect_run_logs(context: RunFailureSensorContext) -> tuple[str, str]:
    """Return (summary_text, full_log_text) for the failed run."""
    instance = context.instance
    run = context.dagster_run
    failure_entries = instance.all_logs(
        run.run_id, of_type=DagsterEventType.PIPELINE_FAILURE
    )
    failure_msg = ""
    for entry in failure_entries:
        failure_event = entry.dagster_event
        if failure_event and failure_event.event_specific_data:
            err = getattr(failure_event.event_specific_data, "error", None)
            if err:
                failure_msg = str(err)
                break

    log_entries = instance.all_logs(run.run_id)
    lines: list[str] = []
    for entry in log_entries:
        ts = entry.timestamp
        level = entry.level
        msg = entry.user_message or (entry.dagster_event.message if entry.dagster_event else "")
        lines.append(f"[{ts}] {level} {msg}")
    full_log = "\n".join(lines)

    summary = (
        f"Run ID: {run.run_id}\n"
        f"Job: {run.job_name}\n"
        f"Status: {run.status.value}\n"
        f"Failure: {failure_msg or '(see attached logs)'}\n"
    )
    return summary, full_log


def _send_email(
    cfg: dict[str, str],
    subject: str,
    body: str,
    attachment: tuple[str, str],
    *,
    html: str | None = None,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["from_addr"]
    msg["To"] = cfg["to_addrs"]
    msg.set_content(body)
    if html:
        # multipart/alternative: clients that render HTML show ``html``,
        # the rest fall back to the plain-text ``body``.
        msg.add_alternative(html, subtype="html")

    filename, content = attachment
    msg.add_attachment(
        content.encode("utf-8"),
        maintype="text",
        subtype="plain",
        filename=filename,
    )

    recipients = [addr.strip() for addr in cfg["to_addrs"].split(",") if addr.strip()]
    port = int(cfg["port"])
    security = cfg["security"]
    smtp_cls = smtplib.SMTP_SSL if security == "ssl" else smtplib.SMTP
    local_hostname = os.environ.get("SMTP_LOCAL_HOSTNAME") or None
    with smtp_cls(cfg["host"], port, local_hostname=local_hostname, timeout=30) as smtp:
        smtp.ehlo()
        if security == "starttls":
            smtp.starttls()
            smtp.ehlo()
        if cfg["user"]:
            smtp.login(cfg["user"], cfg["password"])
        smtp.send_message(msg, from_addr=cfg["from_addr"], to_addrs=recipients)


@run_failure_sensor(
    name="email_on_run_failure",
    minimum_interval_seconds=SENSOR_MINIMUM_INTERVAL_SECONDS,
)
def email_on_run_failure(context: RunFailureSensorContext):
    cfg = _smtp_config()
    if cfg is None:
        context.log.warning(
            "SMTP env vars not configured (need SMTP_HOST, SMTP_FROM, EMAIL_TO)"
        )
        return

    summary, full_log = _collect_run_logs(context)
    subject = f"[metadataswiss-connector] Run failed: {context.dagster_run.job_name}"
    attachment_name = f"run_{context.dagster_run.run_id}.log"

    try:
        _send_email(cfg, subject, summary, (attachment_name, full_log or "(no log entries)"))
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"Failed to send failure email: {exc}")


def _collect_invalid_records(context: RunStatusSensorContext) -> list[dict]:
    """Scan ASSET_MATERIALIZATION events for the run and pull out
    ``invalid_details_json`` payloads emitted by transformed assets.

    Returns a list of ``{asset, id, errors}`` dicts (flattened across
    every asset in the run).
    """
    instance = context.instance
    run_id = context.dagster_run.run_id
    log_entries = instance.all_logs(
        run_id, of_type=DagsterEventType.ASSET_MATERIALIZATION
    )
    out: list[dict] = []
    for entry in log_entries:
        event = entry.dagster_event
        if not event:
            continue
        materialization = event.event_specific_data.materialization
        asset_key = "/".join(materialization.asset_key.path) if materialization.asset_key else "?"
        metadata_value = materialization.metadata.get("invalid_details_json")
        if metadata_value is None:
            continue
        try:
            details = (
                json.loads(metadata_value.value)
                if isinstance(metadata_value.value, str)
                else metadata_value.value
            )
        except (ValueError, TypeError):
            continue
        for d in details or []:
            out.append({"asset": asset_key, **d})
    return out


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="email_on_invalid_records",
    minimum_interval_seconds=SENSOR_MINIMUM_INTERVAL_SECONDS,
)
def email_on_invalid_records(context: RunStatusSensorContext):
    """Send a summary email when a successful run had invalid records.

    Failures are handled by ``email_on_run_failure``; this sensor fills
    the gap so dataspot maintainers see per-record validation errors
    even when the overall run succeeds. The mail groups issues by failing
    field so systemic problems are fixable in bulk in dataspot.
    """
    invalid = _collect_invalid_records(context)
    if not invalid:
        return

    cfg = _smtp_config()
    if cfg is None:
        context.log.warning(
            "SMTP env vars not configured (need SMTP_HOST, SMTP_FROM, EMAIL_TO)"
        )
        return

    run = context.dagster_run
    issues = build_issues(invalid)
    groups = group_issues(issues)
    total_records = len({(d.get("stage"), d.get("id")) for d in invalid})

    text_body = render_text(groups, total_records)
    html_body = render_html(groups, total_records)
    report = render_attachment(issues)
    subject = (
        f"[metadataswiss-connector] {total_records} records with "
        f"quality issues ({run.job_name})"
    )
    attachment_name = f"invalid_records_{run.run_id}.txt"

    try:
        _send_email(
            cfg, subject, text_body, (attachment_name, report), html=html_body
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"Failed to send invalid-records email: {exc}")
