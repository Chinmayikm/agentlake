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
