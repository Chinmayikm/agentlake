from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pytest

from services.rag.chunk import Chunk
from services.rag.store import CorpusStore


@pytest.fixture
def store(tmp_path: Path) -> Iterator[CorpusStore]:
    corpus_store = CorpusStore(tmp_path / "corpus.db")
    yield corpus_store
    corpus_store.close()


def _chunk(chunk_id: str, doc_id: str, text: str) -> Chunk:
    return Chunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        project="kafka",
        version="3.8",
        section="Intro",
        source_path="a.md",
        chunk_index=0,
        text=text,
    )


def test_content_hash_for_unknown_doc_is_none(store: CorpusStore) -> None:
    assert store.content_hash_for("nope") is None


def test_upsert_document_roundtrips_content_hash(store: CorpusStore) -> None:
    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    assert store.content_hash_for("doc1") == "hash1"

    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T01:00:00", "hash2")
    assert store.content_hash_for("doc1") == "hash2"


def test_replace_chunks_then_get_chunk(store: CorpusStore) -> None:
    chunk = _chunk("c1", "doc1", "hello")
    embeddings = np.array([[1.0, 0.0, 0.0]], dtype=np.float32)

    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    store.replace_chunks("doc1", [chunk], embeddings, "fake-model")

    fetched = store.get_chunk("c1")
    assert fetched.text == "hello"
    assert fetched.project == "kafka"


def test_get_chunk_missing_raises_keyerror(store: CorpusStore) -> None:
    with pytest.raises(KeyError):
        store.get_chunk("missing")


def test_search_returns_nearest_neighbor_first(store: CorpusStore) -> None:
    chunks = [
        _chunk("a", "doc1", "a text"),
        _chunk("b", "doc1", "b text"),
        _chunk("c", "doc1", "c text"),
    ]
    embeddings = np.array(
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.9, 0.1, 0.0]],
        dtype=np.float32,
    )
    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    store.replace_chunks("doc1", chunks, embeddings, "fake-model")

    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    hits = store.search(query, k=2)

    assert [chunk_id for chunk_id, _ in hits] == ["a", "c"]


def test_search_respects_project_filter(store: CorpusStore) -> None:
    kafka_chunk = _chunk("k1", "doc-kafka", "kafka text")
    flink_chunk = Chunk(
        chunk_id="f1",
        doc_id="doc-flink",
        project="flink",
        version="1.20",
        section="Intro",
        source_path="b.md",
        chunk_index=0,
        text="flink text",
    )
    embeddings = np.array([[1.0, 0.0], [1.0, 0.0]], dtype=np.float32)

    store.upsert_document("doc-kafka", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    store.replace_chunks("doc-kafka", [kafka_chunk], embeddings[:1], "fake-model")
    store.upsert_document("doc-flink", "flink", "1.20", "b.md", "2026-08-26T00:00:00", "hash2")
    store.replace_chunks("doc-flink", [flink_chunk], embeddings[1:], "fake-model")

    hits = store.search(np.array([1.0, 0.0], dtype=np.float32), k=5, project="flink")
    assert [chunk_id for chunk_id, _ in hits] == ["f1"]


def test_search_on_empty_store_returns_empty_list(store: CorpusStore) -> None:
    assert store.search(np.array([1.0, 0.0], dtype=np.float32), k=5) == []
