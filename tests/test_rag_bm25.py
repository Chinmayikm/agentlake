import subprocess
import sys
from pathlib import Path

from services.rag.bm25 import BM25Index
from services.rag.chunk import Chunk

REPO_ROOT = Path(__file__).resolve().parent.parent


def _chunk(chunk_id: str, project: str, text: str) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        doc_id="doc1",
        project=project,
        version="1.0",
        section="Intro",
        source_path="a.md",
        chunk_index=0,
        text=text,
    )


def test_search_finds_exact_term_match() -> None:
    index = BM25Index()
    index.replace_chunks(
        "doc1",
        [
            _chunk("a", "kafka", "log compaction retains the last value per key"),
            _chunk("b", "kafka", "broker configs control retention"),
        ],
    )

    hits = index.search("log compaction", k=2)
    assert hits[0][0] == "a"


def test_search_respects_project_filter() -> None:
    index = BM25Index()
    index.replace_chunks("doc1", [_chunk("a", "kafka", "retention policy")])
    index.replace_chunks("doc2", [_chunk("b", "flink", "retention policy")])

    hits = index.search("retention policy", k=5, project="flink")
    assert [chunk_id for chunk_id, _ in hits] == ["b"]


def test_search_on_empty_index_returns_empty_list() -> None:
    assert BM25Index().search("anything", k=5) == []


def test_replace_chunks_removes_stale_entries_for_same_doc() -> None:
    index = BM25Index()
    index.replace_chunks("doc1", [_chunk("a", "kafka", "old content about brokers")])
    index.replace_chunks("doc1", [_chunk("b", "kafka", "new content about topics")])

    # "a" is gone from the index entirely -- not just outscored. BM25Okapi
    # still returns a (zero) score for every remaining doc even on a query
    # with no matching terms, so the assertion is on membership, not emptiness.
    hits = index.search("brokers", k=5)
    assert "a" not in [chunk_id for chunk_id, _ in hits]

    hits = index.search("topics", k=5)
    assert [chunk_id for chunk_id, _ in hits] == ["b"]


def test_save_and_load_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "bm25.pkl"
    index = BM25Index()
    index.replace_chunks("doc1", [_chunk("a", "kafka", "log compaction retains values")])
    index.save(path)

    loaded = BM25Index.load(path)
    hits = loaded.search("log compaction", k=1)
    assert hits[0][0] == "a"


def test_load_missing_path_returns_empty_index(tmp_path: Path) -> None:
    index = BM25Index.load(tmp_path / "does_not_exist.pkl")
    assert index.search("anything", k=5) == []


def test_import_does_not_pull_in_rank_bm25() -> None:
    """Run in a subprocess: an in-process check would depend on test ordering."""
    code = (
        "import sys, services.rag\n"
        "leaked = sorted(m for m in sys.modules if 'rank_bm25' in m)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
