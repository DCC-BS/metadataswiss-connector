"""Dataspot-specific vocabulary mappings to I14Y codes.

Kept separate from the generic I14Y builders so that vocabulary changes
on the Dataspot side don't ripple into shared code, and other sources
don't inherit Dataspot's input shapes.
"""

from metadataswiss_connector.dcat.i14y_models import (
    CodeInputModel,
    MultiLanguageModel,
)


# Dataspot topic code (BFS) → I14Y dataset-theme code.
# Comments show "<dataspot label> → <i14y label>" so the mapping is readable
# without consulting the vocabularies. I14Y theme codes 101–126 come from
# https://input-backend.i14y.a.c.bfs.admin.ch/api/Vocabulary/Concept_DATASET_THEME
_TOPIC_TO_THEME = {
    "001": "117",  # Bevölkerung → Einwohner
    "002": "123",  # Gesetzgebung → Rechtssammlung
    "003": "122",  # Geographie → Geoinformationen
    "004": "114",  # Gesundheit → Gesundheit
    "005": "108",  # Kultur, Medien, Informationsgesellschaft, Sport → Kultur
    "006": "103",  # Bildung, Wissenschaft → Bildung
    "007": "113",  # Raum und Umwelt → Umwelt
    "008": "116",  # Mobilität und Verkehr → Mobilität
    "009": "101",  # Arbeit, Erwerb → Arbeit
    "010": "115",  # Volkswirtschaft → Wirtschaft
    "011": "119",  # Verwaltung → Behörden
    "012": "105",  # Kriminalität, Strafrecht → Gerichtsbarkeit
    "013": "102",  # Bau- und Wohnungswesen → Bauen
    "014": "124",  # Energie → Energie
    "015": "126",  # Soziale Sicherheit → Soziale Sicherheit
    "016": "112",  # Finanzen → Steuern
    "017": "115",  # Handel → Wirtschaft
    "018": "118",  # Industrie, Dienstleistungen → Unternehmen
    "019": "109",  # Land- und Forstwirtschaft → Landwirtschaft
    "020": "111",  # Öffentliche Ordnung und Sicherheit → Sicherheit
    "021": "107",  # Politik → Politische Aktivitäten
    "022": "115",  # Preise → Wirtschaft
    "023": "125",  # Statistische Grundlagen → Öffentliche Statistik
    "024": "115",  # Tourismus → Wirtschaft
}

# Dataspot geoCategory (eCH-166) → I14Y dataset-theme code.
_GEOCATEGORY_TO_THEME = {
    "imageryBaseMapsEarthCover": "122",               # Basiskarten, Bodenbedeckung, Bilddaten → Geoinformationen
    "boundaries": "122",                              # Politische und administrative Grenzen → Geoinformationen
    "elevation": "122",                               # Höhen → Geoinformationen
    "location": "122",                                # Ortsangaben, Referenzsysteme → Geoinformationen
    "society": "106",                                 # Bevölkerung, Gesellschaft, Kultur → Gesellschaft
    "inlandWaters": "113",                            # Gewässer → Umwelt
    "environment": "113",                             # Umwelt-, Naturschutz → Umwelt
    "climatologyMeteorologyAtmosphere": "113",        # Atmosphäre, Luft, Klima → Umwelt
    "geoscientificInformation": "113",                # Geologie, Boden, naturbedingte Risiken → Umwelt
    "farming": "109",                                 # Landwirtschaft → Landwirtschaft
    "biota": "121",                                   # Wald, Flora, Fauna → Tiere
    "intelligenceMilitary": "111",                    # Militär, Sicherheit → Sicherheit
    "utilitiesCommunication": "110",                  # Ver-, Entsorgung, Kommunikation → Infrastruktur
    "health": "114",                                  # Gesundheit → Gesundheit
    "economy": "115",                                 # Wirtschaftliche Aktivitäten → Wirtschaft
    "structure": "120",                               # Gebäude, Anlagen → Gebäude und Grundstücke
    "planningCadastre": "102",                        # Raumplanung, Grundstückskataster → Bauen
    "transportation": "116",                          # Verkehr → Mobilität
}


def themes(
    topics: list[str] | None,
    geo_category: str | None,
) -> list[CodeInputModel] | None:
    """Map Dataspot topics (BFS) and geoCategory (eCH-166) to I14Y theme codes.

    Unknown values are dropped; duplicates are collapsed; insertion order
    is preserved. Returns ``None`` when nothing maps.
    """
    codes: list[str] = []
    seen: set[str] = set()
    for topic in topics or []:
        code = _TOPIC_TO_THEME.get(topic)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    if geo_category:
        code = _GEOCATEGORY_TO_THEME.get(geo_category)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    if not codes:
        return None
    return [CodeInputModel(code=c) for c in codes]


# Dataspot personal_data value → I14Y confidentialityPerson code.
# Ordered most-protective first; when a dataset carries multiple values
# the strongest classification wins.
_PERSONAL_DATA_PRIORITY = [
    ("particularly_protected", "protect_person"),
    ("natural_person", "person"),
    ("legal_person", "person"),
    ("none", "no_person"),
]


def confidentiality_person(values: list[str] | None) -> CodeInputModel | None:
    """Map Dataspot ``personal_data`` values to an I14Y confidentialityPerson code."""
    if not values:
        return None
    present = set(values)
    for value, code in _PERSONAL_DATA_PRIORITY:
        if value in present:
            return CodeInputModel(code=code)
    return None


def retention_period_complement(
    period_years: int | str | None,
    justification: str | None,
    lang: str = "de",
) -> MultiLanguageModel | None:
    """Combine Dataspot's retention period + justification into a plain-text block.

    I14Y has no structured field for retention metadata, so the two
    Dataspot custom properties are concatenated into a single description.
    """
    parts = []
    if period_years not in (None, ""):
        parts.append(f"Aufbewahrungsfrist (Jahre): {period_years}")
    if justification:
        parts.append(f"Begründung: {justification}")
    if not parts:
        return None
    return MultiLanguageModel(**{lang: "\n".join(parts)})
