import shutil
from pathlib import Path

import pytest

from services.rag.bm25 import BM25Index
from services.rag.chunk import doc_id_for
from services.rag.cli import main
from services.rag.fetch import FetchedFile, ProjectSpec
from services.rag.store import CorpusStore

FIXTURES = Path(__file__).parent / "fixtures" / "rag"


class FakeFetchStrategy:
    """Copies a fixture file into dest -- no network/git."""

    def __init__(self, fixture_name: str, dest_name: str) -> None:
        self.fixture_name = fixture_name
        self.dest_name = dest_name

    def __call__(self, spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
        dest.mkdir(parents=True, exist_ok=True)
        local_path = dest / self.dest_name
        shutil.copyfile(FIXTURES / self.fixture_name, local_path)
        return [
            FetchedFile(
                source_path=self.dest_name,
                local_path=local_path,
                content_hash=f"hash-{self.fixture_name}",
            )
        ]


@pytest.fixture
def fake_strategies() -> dict[str, object]:
    return {
        "rendered_html": FakeFetchStrategy("sample_kafka.html", "doc.html"),
        "git_sparse_checkout": FakeFetchStrategy("sample_flink.md", "doc.md"),
    }


@pytest.fixture
def bm25_index() -> BM25Index:
    return BM25Index()


def _ingest(
    store: CorpusStore,
    embedder: object,
    strategies: dict,
    bm25_index: BM25Index,
    raw_dir: Path,
    bm25_path: Path,
    project: str = "all",
) -> int:
    return main(
        ["ingest", "--project", project],
        store=store,
        embedder=embedder,
        bm25_index=bm25_index,
        strategies=strategies,
        raw_dir=raw_dir,
        bm25_path=bm25_path,
    )


def test_ingest_then_retrieve_end_to_end(
    tmp_path: Path, fake_embedder, fake_strategies, bm25_index
) -> None:
    store = CorpusStore(tmp_path / "corpus.db")
    raw_dir, bm25_path = tmp_path / "raw", tmp_path / "bm25.pkl"

    assert _ingest(store, fake_embedder, fake_strategies, bm25_index, raw_dir, bm25_path) == 0

    exit_code = main(
        ["retrieve", "log compaction retains the last value per key", "--k", "2"],
        store=store,
        embedder=fake_embedder,
        bm25_index=bm25_index,
    )
    assert exit_code == 0
    store.close()


def test_ingest_is_idempotent_for_unchanged_docs(
    tmp_path: Path, fake_embedder, fake_strategies, bm25_index
) -> None:
    store = CorpusStore(tmp_path / "corpus.db")
    raw_dir = tmp_path / "raw"
    bm25_path = tmp_path / "bm25.pkl"
    doc_id = doc_id_for("kafka", "3.8", "doc.html")

    _ingest(store, fake_embedder, fake_strategies, bm25_index, raw_dir, bm25_path)
    first_hash = store.content_hash_for(doc_id)

    _ingest(store, fake_embedder, fake_strategies, bm25_index, raw_dir, bm25_path)
    second_hash = store.content_hash_for(doc_id)

    assert first_hash == second_hash == "hash-sample_kafka.html"
    store.close()


def test_ingest_single_project_filter(
    tmp_path: Path, fake_embedder, fake_strategies, bm25_index, capsys
) -> None:
    store = CorpusStore(tmp_path / "corpus.db")

    _ingest(
        store,
        fake_embedder,
        fake_strategies,
        bm25_index,
        tmp_path / "raw",
        tmp_path / "bm25.pkl",
        project="kafka",
    )

    exit_code = main(
        ["retrieve", "log compaction", "--k", "5", "--project", "flink"],
        store=store,
        embedder=fake_embedder,
        bm25_index=bm25_index,
    )
    assert exit_code == 0
    assert capsys.readouterr().out == ""  # no flink docs were ingested -> zero results printed
    store.close()


def test_ingest_saves_bm25_index_to_disk(
    tmp_path: Path, fake_embedder, fake_strategies, bm25_index
) -> None:
    bm25_path = tmp_path / "bm25.pkl"
    store = CorpusStore(tmp_path / "corpus.db")

    _ingest(store, fake_embedder, fake_strategies, bm25_index, tmp_path / "raw", bm25_path)

    assert bm25_path.exists()
    loaded = BM25Index.load(bm25_path)
    hits = loaded.search("log compaction", k=1)
    assert hits
    store.close()
