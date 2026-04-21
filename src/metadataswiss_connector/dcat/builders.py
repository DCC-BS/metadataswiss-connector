"""I14Y DCAT Input-Model builders.

Helpers that construct typed I14Y ``*InputModel`` instances. Each helper
takes the loose, source-side primitives (a string, an epoch timestamp,
a list of tags) and returns a Pydantic model that matches the I14Y
Partner API contract exactly. Validation happens here, not at POST time.

The model classes themselves are generated from the I14Y OpenAPI spec
into ``i14y_models.py`` — see that file's header for the regen command.
"""

from datetime import datetime, timezone

from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    KeywordModel,
    MultiLanguageModel,
    PeriodOfTimeModel,
    ResourceModel,
    VCardModel,
)


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


def frequency(uri: str | None) -> CodeInputModel | None:
    """Build a CodeInputModel from an EU frequency URI.

    The I14Y input contract takes only a code; the URI is recovered
    server-side from the controlled vocabulary.
    """
    if not uri:
        return None
    code = uri.rsplit("/", 1)[-1] if "/" in uri else uri
    return CodeInputModel(code="OTHER")


def keywords(tags: list[str] | None, lang: str = "de") -> list[KeywordModel]:
    if not tags:
        return []
    return [KeywordModel(label=MultiLanguageModel(**{lang: tag})) for tag in tags]


def temporal_coverage(
    start_epoch_ms: int | float | None,
) -> list[PeriodOfTimeModel]:
    if start_epoch_ms is None:
        return []
    return [PeriodOfTimeModel(start=epoch_ms_to_datetime(start_epoch_ms))]


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
