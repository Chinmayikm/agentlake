import shutil
from pathlib import Path
from typing import ClassVar

import pytest

from services.rag.bm25 import BM25Index
from services.rag.chunk import doc_id_for
from services.rag.cli import main
from services.rag.fetch import FetchedFile, ProjectSpec
from services.rag.store import CorpusStore

FIXTURES = Path(__file__).parent / "fixtures" / "rag"


class FakeFetchStrategy:
    """Copies a per-project fixture into dest -- no network/git.

    Dispatches on spec.name, not on which strategy key it's registered
    under -- real sources.yaml may map several projects to the same
    `strategy` value (e.g. all three currently use git_sparse_checkout), and
    a fake keyed only by strategy name would then hand every project the
    same fixture, defeating per-project assertions in these tests.
    """

    _FIXTURES: ClassVar[dict[str, tuple[str, str]]] = {
        "kafka": ("sample_kafka.html", "doc.html"),
        "flink": ("sample_flink.md", "doc.md"),
        "iceberg": ("sample_iceberg.md", "doc.md"),
    }

    def __call__(self, spec: ProjectSpec, dest: Path) -> list[FetchedFile]:
        fixture_name, dest_name = self._FIXTURES[spec.name]
        dest.mkdir(parents=True, exist_ok=True)
        local_path = dest / dest_name
        shutil.copyfile(FIXTURES / fixture_name, local_path)
        return [
            FetchedFile(
                source_path=dest_name, local_path=local_path, content_hash=f"hash-{fixture_name}"
            )
        ]


@pytest.fixture
def fake_strategies() -> dict[str, object]:
    strategy = FakeFetchStrategy()
    return {"rendered_html": strategy, "git_sparse_checkout": strategy}


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
    capsys.readouterr()  # discard ingest's own progress output

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
