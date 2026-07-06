"""Guard that the generated mapping documentation stays in sync.

Runs the generator in ``--check`` mode (writes nothing, exits non-zero when
``docs/mappings.md`` differs from what the current source would produce), so a
mapping change without a regenerated page fails the test suite as well as CI.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "gen_mapping_docs.py"


def test_mapping_docs_up_to_date():
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "docs/mappings.md is out of date - regenerate it with "
        "`uv run python scripts/gen_mapping_docs.py`.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
