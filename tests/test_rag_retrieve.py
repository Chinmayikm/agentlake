from collections.abc import Iterator
from pathlib import Path

import pytest

from services.rag.bm25 import BM25Index
from services.rag.chunk import Chunk
from services.rag.retrieve import retrieve
from services.rag.store import CorpusStore

_CHUNKS = [
    Chunk(
        chunk_id="c1",
        doc_id="doc1",
        project="kafka",
        version="3.8",
        section="Log Compaction",
        source_path="a.md",
        chunk_index=0,
        text="log compaction retains the last value per key",
    ),
    Chunk(
        chunk_id="c2",
        doc_id="doc1",
        project="kafka",
        version="3.8",
        section="Broker Configs",
        source_path="a.md",
        chunk_index=1,
        text="broker config log.retention.hours controls retention",
    ),
    # Distractor in a different project: BM25's IDF math is unstable over a
    # 2-document corpus (a term in every document can get a near-zero or
    # negative weight), so a 3rd, unrelated document is what makes "the
    # chunk that actually shares query terms wins" a meaningful assertion.
    Chunk(
        chunk_id="c3",
        doc_id="doc2",
        project="iceberg",
        version="1.7",
        section="Schema Evolution",
        source_path="b.md",
        chunk_index=0,
        text="schema evolution allows adding dropping renaming columns without rewriting files",
    ),
]


_KAFKA_CHUNKS = [c for c in _CHUNKS if c.doc_id == "doc1"]
_ICEBERG_CHUNKS = [c for c in _CHUNKS if c.doc_id == "doc2"]


@pytest.fixture
def seeded_store(tmp_path: Path, fake_embedder) -> Iterator[CorpusStore]:
    store = CorpusStore(tmp_path / "corpus.db")
    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    store.replace_chunks(
        "doc1", _KAFKA_CHUNKS, fake_embedder.embed([c.text for c in _KAFKA_CHUNKS]), "fake-model"
    )
    store.upsert_document("doc2", "iceberg", "1.7", "b.md", "2026-08-26T00:00:00", "hash2")
    iceberg_embeddings = fake_embedder.embed([c.text for c in _ICEBERG_CHUNKS])
    store.replace_chunks("doc2", _ICEBERG_CHUNKS, iceberg_embeddings, "fake-model")
    yield store
    store.close()


@pytest.fixture
def seeded_bm25() -> BM25Index:
    index = BM25Index()
    index.replace_chunks("doc1", _KAFKA_CHUNKS)
    index.replace_chunks("doc2", _ICEBERG_CHUNKS)
    return index


@pytest.fixture
def empty_bm25() -> BM25Index:
    return BM25Index()


def test_retrieve_dense_returns_exact_text_match_first(
    seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    hits = retrieve(
        "log compaction retains the last value per key",
        k=2,
        mode="dense",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )
    assert hits[0].chunk_id == "c1"
    assert hits[0].score >= hits[1].score


def test_retrieve_bm25_finds_exact_term_match(
    seeded_store: CorpusStore, fake_embedder, seeded_bm25
) -> None:
    hits = retrieve(
        "log.retention.hours",
        k=1,
        mode="bm25",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=seeded_bm25,
    )
    assert hits[0].chunk_id == "c2"


def test_retrieve_hybrid_fuses_dense_and_bm25(
    seeded_store: CorpusStore, fake_embedder, seeded_bm25
) -> None:
    hits = retrieve(
        "broker config log.retention.hours controls retention",
        k=2,
        mode="hybrid",
        project="kafka",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=seeded_bm25,
    )
    assert {h.chunk_id for h in hits} == {"c1", "c2"}
    assert hits[0].chunk_id == "c2"  # exact text match on both dense and bm25


def test_retrieve_respects_k(seeded_store: CorpusStore, fake_embedder, empty_bm25) -> None:
    hits = retrieve(
        "anything",
        k=1,
        mode="dense",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )
    assert len(hits) == 1


def test_retrieve_result_carries_metadata(
    seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    hits = retrieve(
        "broker config log.retention.hours controls retention",
        k=1,
        mode="dense",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )
    assert hits[0].chunk_id == "c2"
    assert hits[0].project == "kafka"
    assert hits[0].version == "3.8"
    assert hits[0].section == "Broker Configs"


def test_retrieve_rejects_unknown_mode(
    seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    with pytest.raises(ValueError, match="unknown mode"):
        retrieve(
            "q", mode="nope", store=seeded_store, embedder=fake_embedder, bm25_index=empty_bm25
        )


def test_retrieve_emits_one_retrieval_span(
    events, seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    retrieve(
        "log compaction retains the last value per key",
        k=2,
        mode="dense",
        project="kafka",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )

    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "RETRIEVAL"
    assert event["attributes"]["name"] == "vector_search"
    assert event["attributes"]["mode"] == "dense"
    assert event["attributes"]["top_k"] == "2"
    assert event["attributes"]["project"] == "kafka"
    assert event["attributes"]["hits"] == "2"
    assert event["attributes"]["top_chunk_ids"] == "c1,c2"
    assert "log compaction" in event["attributes"]["query"]
    assert event["status"] == "ok"


def test_retrieve_span_hits_matches_result_count(
    events, seeded_store: CorpusStore, fake_embedder, seeded_bm25
) -> None:
    hits = retrieve(
        "broker config log.retention.hours controls retention",
        k=2,
        mode="hybrid",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=seeded_bm25,
    )

    assert len(events) == 1
    assert events[0]["attributes"]["hits"] == str(len(hits))
    assert events[0]["attributes"]["mode"] == "hybrid"


# ---------------------------------------------------------------------------
# The corpus_version regression (ADR-008 #1)
# ---------------------------------------------------------------------------
#
# QdrantStore.search() filters on corpus_version, so a store built without one
# matches nothing ingest wrote and returns zero hits -- indistinguishable, from
# the outside, from a query with no good answer. retrieve.py built
# QdrantStore() while cli.py built QdrantStore(corpus_version=...), so every
# library-path dense search returned nothing and hybrid silently degraded to
# BM25-only. These tests exist so that cannot come back.


def test_default_store_is_configured_with_the_ingested_corpus_version() -> None:
    """If this fails, dense retrieval returns zero rows in production and
    nothing raises -- hybrid just quietly becomes BM25-only."""
    from services.rag.fetch import load_corpus_version
    from services.rag.qdrant_store import default_store

    store = default_store()

    assert store.corpus_version == load_corpus_version()
    assert store.corpus_version != "unknown"


@pytest.mark.parametrize("module", ["retrieve", "cli"])
def test_no_module_builds_an_unconfigured_qdrant_store(module: str) -> None:
    """A source-text contract, because the bug was two call sites that had to
    agree and did not. Constructing QdrantStore(...) anywhere outside
    qdrant_store.py reintroduces exactly that -- go through default_store()."""
    source = (Path("services/rag") / f"{module}.py").read_text(encoding="utf-8")

    assert "QdrantStore(" not in source, (
        f"services/rag/{module}.py constructs a QdrantStore directly; "
        f"use services.rag.qdrant_store.default_store() instead"
    )
    assert "default_store" in source


def test_retrieval_span_records_the_corpus_version_it_searched(
    events, seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    """The attribute that would have made the bug visible: without it, a trace
    of a zero-hit retrieval cannot distinguish a filter mismatch from a query
    the corpus genuinely does not answer."""
    seeded_store.corpus_version = "2026-08-27-pinned"

    retrieve(
        "log compaction",
        k=2,
        mode="dense",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )

    assert events[0]["attributes"]["corpus_version"] == "2026-08-27-pinned"


def test_retrieval_span_omits_corpus_version_for_a_store_that_has_none(
    events, seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    """CorpusStore has no such concept, and _coerce_attrs drops a None -- so
    the sqlite fake emits a span of exactly the shape it emitted before this
    attribute existed. A null-valued entry is a shape the Avro contract's
    value-required map says cannot exist."""
    retrieve(
        "log compaction",
        k=2,
        mode="dense",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )

    assert "corpus_version" not in events[0]["attributes"]


def test_retrieval_span_records_which_documents_came_back(
    events, seeded_store: CorpusStore, fake_embedder, empty_bm25
) -> None:
    """eval/'s trace-mode hit@k reads this. Without it, reading the trace means
    resolving every chunk_id against the store -- asking the store a question
    the trace was supposed to answer. Deduplicated: two chunks of one document
    is the normal case and repeating its path says nothing."""
    retrieve(
        "log compaction retains the last value per key",
        k=2,
        mode="dense",
        project="kafka",
        store=seeded_store,
        embedder=fake_embedder,
        bm25_index=empty_bm25,
    )

    attrs = events[0]["attributes"]
    assert attrs["top_chunk_ids"] == "c1,c2"
    assert attrs["top_source_paths"] == "a.md"
    assert "top_source_paths_truncated" not in attrs


def test_source_paths_attribute_says_so_when_it_truncates() -> None:
    """A shortened list that did not say it was shortened would make hit@k
    quietly wrong rather than visibly incomplete."""
    from services.rag.retrieve import _MAX_SOURCE_PATHS, RetrievedChunk, _source_paths_attr

    many = [
        RetrievedChunk(
            chunk_id=f"c{i}",
            project="kafka",
            version="3.8",
            section="S",
            source_path=f"doc{i}.md",
            text="",
            score=1.0,
        )
        for i in range(_MAX_SOURCE_PATHS + 1)
    ]

    joined, truncated = _source_paths_attr(many)

    assert truncated is True
    assert joined.count(",") == _MAX_SOURCE_PATHS - 1
    assert "doc10.md" not in joined
