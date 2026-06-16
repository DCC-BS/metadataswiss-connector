"""Shared test fixtures.

``load_fixture`` loads recorded Dataspot example records from
``tests/fixtures/*.json``. These files are the test cases stored in the
repository — deliberately versioned so that it stays traceable which
inputs the transformation is tested against.
"""

import json
from pathlib import Path

import pytest

from metadataswiss_connector.dcat.lookups import Lookups

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture():
    def _load(name: str) -> dict:
        with open(FIXTURE_DIR / name, encoding="utf-8") as f:
            fx = json.load(f)
        # Transforms receive lookups as the per-run Lookups view, not as
        # the raw dict the fixture stores.
        if "lookups" in fx:
            fx["lookups"] = Lookups(fx["lookups"])
        return fx

    return _load
