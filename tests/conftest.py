"""Gemeinsame Test-Fixtures.

``load_fixture`` lädt aufgezeichnete Dataspot-Beispiel-Records aus
``tests/fixtures/*.json``. Diese Dateien sind die im Repository
abgelegten Testfälle — bewusst versioniert, damit sich nachvollziehen
lässt, gegen welche Eingaben die Transformation getestet wird.
"""

import json
from pathlib import Path

import pytest

from metadataswiss_connector.dcat.lookups import Lookups

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture():
    def _load(name: str) -> dict:
        with open(FIXTURE_DIR / name) as f:
            fx = json.load(f)
        # Transforms receive lookups as the per-run Lookups view, not as
        # the raw dict the fixture stores.
        if "lookups" in fx:
            fx["lookups"] = Lookups(fx["lookups"])
        return fx

    return _load
