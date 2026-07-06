"""I14Y DCAT Input-Model builders.

Helpers that construct typed I14Y ``*InputModel`` instances. Each helper
takes the loose, source-side primitives (a string, an epoch timestamp,
a list of tags) and returns a Pydantic model that matches the I14Y
Partner API contract exactly. Validation happens here, not at POST time.

The model classes themselves are generated from the I14Y OpenAPI spec
into ``i14y_models.py`` — see that file's header for the regen command.

This module is source-agnostic: it knows the I14Y data model and the
EU-level controlled vocabularies, nothing about any specific catalog
source. Source-specific vocabulary mappings live next to the source
(e.g. ``sources/dataspot/mappings.py``).
"""

import re
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser

from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    KeywordModel,
    MultiLanguageModel,
    PeriodOfTimeModel,
    ResourceModel,
    VCardModel,
)


# Tags after which a whitespace separator is inserted so adjacent
# block-level chunks (e.g. ``<p>A</p><p>B</p>``) don't run together
# once tags are stripped.
_HTML_BREAK_TAGS = {
    "br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6",
    "blockquote", "pre", "section", "article", "header", "footer",
}


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in _HTML_BREAK_TAGS:
            self._parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in _HTML_BREAK_TAGS:
            self._parts.append(" ")

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def html_to_plain_text(value: str | None) -> str | None:
    """Strip HTML markup so the value can be sent as plain text.

    Source descriptions may contain inline HTML (``<p>…</p>``, ``<br>``,
    entity references). I14Y stores descriptions as unformatted plain
    text, so all tags are removed and whitespace is collapsed.
    """
    if not value:
        return value
    if "<" not in value and "&" not in value:
        return value
    parser = _HTMLStripper()
    parser.feed(value)
    parser.close()
    text = unescape(parser.get_text())
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def multi_language(value: str | None, lang: str = "de") -> MultiLanguageModel | None:
    if not value:
        return None
    return MultiLanguageModel(**{lang: value})


_FILE_FORMAT_CODES = {
    "html": "HTML",
    "htm": "HTML",
    "pdf": "PDF",
    "xls": "XLS",
    "xlsx": "XLSX",
    "csv": "CSV",
    "json": "JSON",
    "xml": "XML",
    "zip": "ZIP",
    "txt": "TXT",
    "geojson": "GEOJSON",
    "shp": "SHP",
}


def file_format(value: str | None) -> CodeInputModel | None:
    """Map a loose file-format string to an I14Y file-type vocabulary code.

    I14Y uses the EU file-type authority codes (uppercase).
    Returns ``None`` for unknown or missing values so the field is omitted.
    """
    if not value:
        return None
    code = _FILE_FORMAT_CODES.get(value.strip().lower())
    if not code:
        return None
    return CodeInputModel(code=code)


_FREQUENCY_CODES = {
    "ANNUAL", "ANNUAL_2", "ANNUAL_3", "BIDECENNIAL", "BIENNIAL", "BIHOURLY",
    "BIMONTHLY", "BIWEEKLY", "CONT", "DAILY", "DAILY_2", "DECENNIAL", "HOURLY",
    "IRREG", "MONTHLY", "MONTHLY_2", "MONTHLY_3", "NEVER", "OTHER", "QUARTERLY",
    "QUADRENNIAL", "QUINQUENNIAL", "TRIDECENNIAL", "TRIENNIAL", "TRIHOURLY",
    "UNKNOWN", "UPDATE_CONT", "WEEKLY", "WEEKLY_2", "WEEKLY_3",
}


def frequency(uri: str | None) -> CodeInputModel | None:
    """Build a CodeInputModel from an EU frequency URI.

    Maps to the I14Y-supported VOCAB_EU_FREQUENCY codes; falls back to
    ``OTHER`` when the value is outside the controlled vocabulary.
    """
    if not uri:
        return None
    code = uri.rsplit("/", 1)[-1].upper() if "/" in uri else uri.upper()
    if code not in _FREQUENCY_CODES:
        code = "OTHER"
    return CodeInputModel(code=code)


def keywords(tags: list[str] | None, lang: str = "de") -> list[KeywordModel]:
    if not tags:
        return []
    return [KeywordModel(label=MultiLanguageModel(**{lang: tag})) for tag in tags]


def temporal_coverage(
    start_epoch_ms: int | float | None,
    end_epoch_ms: int | float | None,
) -> list[PeriodOfTimeModel]:
    if start_epoch_ms is None:
        return []
    return [PeriodOfTimeModel(start=epoch_ms_to_datetime(start_epoch_ms), end=epoch_ms_to_datetime(end_epoch_ms))]


def contact_points(email: str | None) -> list[VCardModel]:
    """Build a VCardModel list from an email address."""
    if not email:
        return []
    return [VCardModel(has_email=email)]


def landing_pages(uri: str | None) -> list[ResourceModel]:
    if not uri:
        return []
    return [ResourceModel(uri=uri)]


def epoch_ms_to_datetime(epoch_ms: int | float | None) -> datetime | None:
    """Convert epoch milliseconds to a timezone-aware datetime."""
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
