"""Rendering of the invalid-records alert email.

Pure data-to-text/HTML transformation, separated from the Dagster
sensors and SMTP transport in ``email_alerts``: takes the flattened
``invalid_details`` dicts collected from the run's materializations and
produces the German-language mail bodies plus the flat attachment
report. No Dagster or SMTP imports, so the rendering is unit-testable
with plain dicts.
"""

from __future__ import annotations

import html

from metadataswiss_connector.config import dataspot_web_base

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

    Built from ``config.dataspot_web_base()`` plus dataspot's fixed UI
    route ``/web/{database}/{type}/{id}``, where ``{type}`` resolves per
    record kind (datasets vs enumerations). Returns ``None`` if the
    source env vars are unset so the mail degrades to title + UUID only.
    """
    base = dataspot_web_base()
    if base is None or not record_id:
        return None
    base_url, database = base
    segment = _KIND_URL_SEGMENT.get(kind or "", "datasets")
    return f"{base_url}/web/{database}/{segment}/{record_id}"


def _group_heading(stage: str, field: str | None) -> str:
    stage_label = _STAGE_LABELS.get(stage, stage)
    if field:
        return f"{field} ({stage_label})"
    return f"Record abgelehnt ({stage_label})"


def build_issues(invalid: list[dict]) -> list[dict]:
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


def group_issues(issues: list[dict]) -> list[tuple[tuple[str, str | None], list[dict]]]:
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


def render_text(groups, total_records: int) -> str:
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


def render_html(groups, total_records: int) -> str:
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


def render_attachment(issues: list[dict]) -> str:
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
