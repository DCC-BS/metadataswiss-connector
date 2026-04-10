"""I14Y DCAT model builders.

Shared helper functions that construct I14Y-compatible DCAT structures
(MultiLanguageModel, VocabularyEntryModel, AgentModel, etc.).
These are independent of any specific source catalog.
"""

from datetime import datetime, timezone


def multi_language(value: str | None, lang: str = "de") -> dict | None:
    if not value:
        return None
    return {lang: value}


def publisher(name: str | None, lang: str = "de") -> dict | None:
    """Build an AgentModel from a publisher name."""
    if not name:
        return None
    return {
        "identifier": name,
        "name": {lang: name},
        "pref_label": {lang: name},
    }


def frequency(uri: str | None) -> dict | None:
    """Build a VocabularyEntryModel from an EU frequency URI."""
    if not uri:
        return None
    code = uri.rsplit("/", 1)[-1] if "/" in uri else uri
    return {"code": code, "uri": uri}


def keywords(tags: list[str] | None, lang: str = "de") -> list:
    """Build a list of KeywordModels from tag strings."""
    if not tags:
        return []
    return [{"label": {lang: tag}} for tag in tags]


def temporal_coverage(start_epoch_ms: int | float | None) -> list:
    """Build a PeriodOfTimeModel list from an epoch-ms start timestamp."""
    if start_epoch_ms is None:
        return []
    return [{"start": epoch_ms_to_iso(start_epoch_ms)}]


def contact_points(email: str | None) -> list:
    """Build a VCardModel list from an email address."""
    if not email:
        return []
    return [{"has_email": email}]


def landing_pages(uri: str | None) -> list:
    """Build a ResourceModel list from a URI."""
    if not uri:
        return []
    return [{"uri": uri}]


def epoch_ms_to_iso(epoch_ms: int | float | None) -> str | None:
    """Convert epoch milliseconds to an ISO 8601 datetime string."""
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()
