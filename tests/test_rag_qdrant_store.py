import subprocess
import sys
from pathlib import Path

from services.rag.qdrant_store import _point_id

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_point_id_is_deterministic() -> None:
    assert _point_id("deadbeefdeadbeef") == _point_id("deadbeefdeadbeef")


def test_point_id_differs_across_hex_ids() -> None:
    assert _point_id("deadbeefdeadbeef") != _point_id("0000000000000001")


def test_point_id_matches_hex_conversion() -> None:
    assert _point_id("0000000000000001") == 1
    assert _point_id("ffffffffffffffff") == (2**64 - 1)


def test_import_does_not_pull_in_qdrant_client() -> None:
    """Run in a subprocess: an in-process check would depend on test ordering."""
    code = (
        "import sys, services.rag\n"
        "leaked = sorted(m for m in sys.modules if 'qdrant_client' in m)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
