"""Repo-level contracts checked as files.

No Kafka, no Qdrant, no Docker, no network -- everything here reads the
Makefile and the CI workflow as text and asserts they agree with each other.
The repo already has this shape elsewhere (tests/test_cdc.py reads
metadata/sql/*.sql; tests/test_dashboards.py reads dashboards/json/*.json);
this file covers the pair that governs whether "CI matches local reality" is
true or merely intended.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MAKEFILE = REPO_ROOT / "Makefile"
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"


def _makefile_lint_paths() -> list[str]:
    match = re.search(r"^LINT_PATHS\s*=\s*(.+)$", MAKEFILE.read_text(encoding="utf-8"), re.M)
    assert match, "Makefile no longer defines LINT_PATHS"
    return match.group(1).split()


def _ci_ruff_paths() -> list[str]:
    for line in CI_WORKFLOW.read_text(encoding="utf-8").splitlines():
        if "ruff check" in line:
            return line.split("ruff check", 1)[1].split()
    raise AssertionError("no `ruff check` step found in ci.yml")


# ---------------------------------------------------------------------------
# 1. Lint scope cannot drift between the Makefile and CI
# ---------------------------------------------------------------------------


def test_makefile_and_ci_lint_the_same_paths() -> None:
    """If these drift, a directory is linted in one place and not the other --
    so `make lint` passes locally and CI fails, or worse, the reverse and an
    unlinted directory ships. Adding a new top-level Python package means
    editing both lines, and this test is what says so."""
    assert _makefile_lint_paths() == _ci_ruff_paths()


def test_lint_covers_every_top_level_python_package() -> None:
    """A new package that nothing lints is the failure this catches: ruff would
    keep passing while the code in it was never checked at all."""
    linted = {p.rstrip("/") for p in _makefile_lint_paths()}
    packages = {
        path.name
        for path in REPO_ROOT.iterdir()
        if path.is_dir()
        and not path.name.startswith((".", "_"))
        and path.name not in {"docs", "dashboards", "contracts", "dbt", "data"}
        and any(path.glob("**/*.py"))
    }

    assert packages <= linted, f"not linted: {sorted(packages - linted)}"
