"""Run-failure email alerting for the Dagster deployment.

Sends a summary email (with the run's full event log attached) to the
recipients listed in ``EMAIL_TO`` whenever a run fails. SMTP is
configured via the standard ``SMTP_*`` env vars; if any required var is
missing the sensor logs a warning and skips silently rather than
breaking the run.
"""

from __future__ import annotations

import html
import json
import os
import smtplib
from email.message import EmailMessage

from dagster import (
    DagsterEventType,
    DagsterRunStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    SkipReason,
    run_failure_sensor,
    run_status_sensor,
)


SMTP_SECURITY_CHOICES = ("starttls", "ssl")

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
    use_ssl = cfg["security"] == "ssl"
    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    local_hostname = os.environ.get("SMTP_LOCAL_HOSTNAME") or None
    with smtp_cls(cfg["host"], port, local_hostname=local_hostname, timeout=30) as smtp:
        smtp.ehlo()
        if not use_ssl:
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
        return SkipReason("SMTP env vars not configured (need SMTP_HOST, SMTP_FROM, EMAIL_TO)")

    summary, full_log = _collect_run_logs(context)
    subject = f"[metadataswiss-connector] Run failed: {context.dagster_run.job_name}"
    attachment_name = f"run_{context.dagster_run.run_id}.log"

    try:
        _send_email(cfg, subject, summary, (attachment_name, full_log or "(no log entries)"))
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"Failed to send failure email: {exc}")
        return SkipReason(f"Email send failed: {exc}")


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
        entry = materialization.metadata.get("invalid_details_json")
        if entry is None:
            continue
        try:
            details = json.loads(entry.value) if isinstance(entry.value, str) else entry.value
        except (ValueError, TypeError):
            continue
        for d in details or []:
            out.append({"asset": asset_key, **d})
    return out


# Human labels for the pipeline stage an issue surfaced at. ``transform``
# = the record failed local validation against the I14Y contract;
# ``publish`` = I14Y's API rejected the request. Both are fixed in
# dataspot, but the distinction tells maintainers where to look.
_STAGE_LABELS = {"transform": "Validierung", "publish": "I14Y-Publish"}

# dataspot UI path segment per resource kind. Data products (datasets and
# APIs/dataservices) live under ``datasets``; code-list concepts under
# ``enumerations``. Used to fill the ``{type}`` slot of the link template.
_KIND_URL_SEGMENT = {
    "dataset": "datasets",
    "dataservice": "datasets",
    "concept": "enumerations",
}


def _dataspot_link(record_id: object, kind: str | None) -> str | None:
    """Deep-link into the dataspot UI for a record.

    Built from the source connection env vars ``SOURCES__DATASPOT__BASE_URL``
    and ``SOURCES__DATASPOT__DATABASE_NAME`` plus dataspot's fixed UI route
    ``/web/{database}/{type}/{id}``, where ``{type}`` resolves per record
    kind (datasets vs enumerations). Returns ``None`` if either env var is
    unset so the mail degrades to title + UUID only.
    """
    base = os.environ.get("SOURCES__DATASPOT__BASE_URL")
    database = os.environ.get("SOURCES__DATASPOT__DATABASE_NAME")
    if not base or not database or not record_id:
        return None
    segment = _KIND_URL_SEGMENT.get(kind or "", "datasets")
    return f"{base.rstrip('/')}/web/{database}/{segment}/{record_id}"


def _group_heading(stage: str, field: str | None) -> str:
    stage_label = _STAGE_LABELS.get(stage, stage)
    if field:
        return f"{field} ({stage_label})"
    return f"Record abgelehnt ({stage_label})"


def _build_issues(invalid: list[dict]) -> list[dict]:
    """Flatten invalid records into one issue per failing field.

    Transform records carry structured ``field_errors`` (one issue each);
    publish failures (and any legacy entry) collapse to a single issue
    built from the ``errors`` string.
    """
    issues: list[dict] = []
    for row in invalid:
        base = {
            "id": row.get("id"),
            "title": row.get("title"),
            "asset": row.get("asset"),
            "stage": row.get("stage", "transform"),
            "kind": row.get("kind"),
        }
        field_errors = row.get("field_errors")
        if field_errors:
            for fe in field_errors:
                issues.append({
                    **base,
                    "field": fe.get("field"),
                    "message": fe.get("message", ""),
                    "value": fe.get("value"),
                })
        else:
            issues.append({
                **base,
                "field": None,
                "message": row.get("errors") or "(unbekannter Fehler)",
                "value": None,
            })
    return issues


def _group_issues(issues: list[dict]) -> list[tuple[tuple[str, str | None], list[dict]]]:
    """Group issues by (stage, field), most frequent first.

    Grouping by field (not the exact message) keeps all records that have
    a problem with the *same* field together — that's the unit a dataspot
    maintainer fixes. The specific message is shown per row instead.
    """
    groups: dict[tuple[str, str | None], list[dict]] = {}
    for it in issues:
        key = (it["stage"], it["field"])
        groups.setdefault(key, []).append(it)
    return sorted(
        groups.items(),
        key=lambda kv: (-len(kv[1]), kv[0][0], kv[0][1] or ""),
    )


def _datensaetze(n: int) -> str:
    return "1 Datensatz" if n == 1 else f"{n} Datensätze"


def _intro(total_records: int) -> str:
    if total_records == 1:
        return (
            "1 Datensatz wurde beim Sync nach I14Y übersprungen. Bitte in "
            "dataspot korrigieren, damit er beim nächsten Lauf publiziert wird."
        )
    return (
        f"{total_records} Datensätze wurden beim Sync nach I14Y übersprungen. "
        "Bitte in dataspot korrigieren, damit sie beim nächsten Lauf publiziert werden."
    )


def _render_text(groups, total_records: int) -> str:
    lines = [_intro(total_records), ""]
    for (stage, field), items in groups:
        lines.append(f"▌ {_group_heading(stage, field)} — {_datensaetze(len(items))}")
        for it in items:
            title = it["title"] or "(ohne Titel)"
            value = f" → Wert: {it['value']}" if it.get("value") else ""
            link = _dataspot_link(it["id"], it.get("kind"))
            link_txt = f"\n     {link}" if link else f"  [{it['id']}]"
            lines.append(f"   • {title} — {it['message']}{value}{link_txt}")
        lines.append("")
    return "\n".join(lines)


def _render_html(groups, total_records: int) -> str:
    esc = html.escape
    parts = [
        '<html><body style="font-family:Arial,Helvetica,sans-serif;color:#222;">',
        f"<p>{esc(_intro(total_records))}</p>",
        '<table cellpadding="6" cellspacing="0" border="0"'
        ' style="border-collapse:collapse;width:100%;">',
        '<tr style="background:#e8e8e8;text-align:left;">'
        "<th>Datensatz</th><th>Problem</th><th>Wert</th></tr>",
    ]
    for (stage, field), items in groups:
        # One section header row spanning the table, then the records.
        parts.append(
            '<tr><td colspan="3"'
            ' style="background:#f6f6f6;border-top:2px solid #ccc;'
            'padding-top:10px;font-weight:bold;">'
            f"{esc(_group_heading(stage, field))}"
            f' <span style="color:#888;font-weight:normal;">'
            f"— {_datensaetze(len(items))}</span></td></tr>"
        )
        for it in items:
            rid = esc(str(it["id"]))
            link = _dataspot_link(it["id"], it.get("kind"))
            title_text = esc(it["title"] or "(ohne Titel)")
            if link:
                title_cell = f'<a href="{esc(link)}">{title_text}</a>'
            else:
                title_cell = title_text
            value = esc(it["value"]) if it.get("value") else ""
            parts.append(
                '<tr style="border-bottom:1px solid #eee;">'
                f"<td>{title_cell}"
                f'<br><span style="font-family:monospace;font-size:85%;color:#999;">{rid}</span></td>'
                f"<td>{esc(it['message'])}</td>"
                f'<td style="font-family:monospace;">{value}</td></tr>'
            )
    parts.append("</table></body></html>")
    return "\n".join(parts)


def _render_attachment(issues: list[dict]) -> str:
    """Full, flat detail report attached for the record/audit trail."""
    lines: list[str] = []
    for it in issues:
        lines.append(
            f"[{it['stage']}] {it['asset']} | id={it['id']} | title={it.get('title')!r}"
        )
        lines.append(f"    field={it.get('field')} value={it.get('value')!r}")
        lines.append(f"    {it['message']}")
        lines.append("")
    return "\n".join(lines) or "(keine Details)"


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
        return SkipReason("No invalid records in this run")

    cfg = _smtp_config()
    if cfg is None:
        return SkipReason("SMTP env vars not configured (need SMTP_HOST, SMTP_FROM, EMAIL_TO)")

    run = context.dagster_run
    issues = _build_issues(invalid)
    groups = _group_issues(issues)
    total_records = len({(d.get("stage"), d.get("id")) for d in invalid})

    text_body = _render_text(groups, total_records)
    html_body = _render_html(groups, total_records)
    report = _render_attachment(issues)
    subject = (
        f"[metadataswiss-connector] {total_records} Datensätze mit "
        f"Qualitätsproblemen ({run.job_name})"
    )
    attachment_name = f"invalid_records_{run.run_id}.txt"

    try:
        _send_email(
            cfg, subject, text_body, (attachment_name, report), html=html_body
        )
    except Exception as exc:  # noqa: BLE001
        context.log.warning(f"Failed to send invalid-records email: {exc}")
        return SkipReason(f"Email send failed: {exc}")
