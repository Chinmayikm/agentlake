"""Tests for services/mcp_server -- against dispatch_tool() directly, the same
seam tests/test_rag_retrieve.py uses for retrieve() itself, not the MCP
protocol machinery. See docs/adr/ADR-003.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from services.mcp_server.server import TOOLS, dispatch_tool, warmup
from services.rag.bm25 import BM25Index
from services.rag.chunk import Chunk
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
    # Distractor in a different doc/project: BM25's IDF math is unstable over
    # a 2-document corpus (see ADR-002 #3 / test_rag_retrieve.py), so a 3rd
    # unrelated document is what makes "the chunk sharing query terms wins" a
    # meaningful assertion instead of an artifact of a too-small corpus.
    Chunk(
        chunk_id="c3",
        doc_id="doc2",
        project="iceberg",
        version="1.7",
        section="Schema Evolution",
        source_path="b.md",
        chunk_index=0,
        text="schema evolution allows adding dropping renaming columns",
    ),
]

_DOC1_CHUNKS = [c for c in _CHUNKS if c.doc_id == "doc1"]
_DOC2_CHUNKS = [c for c in _CHUNKS if c.doc_id == "doc2"]


@pytest.fixture
def seeded_store(tmp_path: Path, fake_embedder) -> Iterator[CorpusStore]:
    store = CorpusStore(tmp_path / "corpus.db")
    store.upsert_document("doc1", "kafka", "3.8", "a.md", "2026-08-26T00:00:00", "hash1")
    store.replace_chunks(
        "doc1", _DOC1_CHUNKS, fake_embedder.embed([c.text for c in _DOC1_CHUNKS]), "fake-model"
    )
    store.upsert_document("doc2", "iceberg", "1.7", "b.md", "2026-08-26T00:00:00", "hash2")
    store.replace_chunks(
        "doc2", _DOC2_CHUNKS, fake_embedder.embed([c.text for c in _DOC2_CHUNKS]), "fake-model"
    )
    yield store
    store.close()


@pytest.fixture
def seeded_bm25() -> BM25Index:
    index = BM25Index()
    index.replace_chunks("doc1", _DOC1_CHUNKS)
    index.replace_chunks("doc2", _DOC2_CHUNKS)
    return index


def _deps(seeded_store, fake_embedder, seeded_bm25) -> dict:
    return {"store": seeded_store, "embedder": fake_embedder, "bm25_index": seeded_bm25}


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def test_search_docs_missing_required_query_is_reported_not_raised(events) -> None:
    result = dispatch_tool("search_docs", {})
    assert result == {"error": "invalid arguments for search_docs: 'query' is a required property"}


def test_search_docs_invalid_mode_is_reported_not_raised(events) -> None:
    result = dispatch_tool("search_docs", {"query": "x", "mode": "bogus"})
    assert "invalid arguments" in result["error"]


def test_get_trace_missing_trace_id_is_reported_not_raised(events) -> None:
    result = dispatch_tool("get_trace", {})
    assert "invalid arguments" in result["error"]


def test_query_metrics_missing_window_is_reported_not_raised(events) -> None:
    result = dispatch_tool("query_metrics", {"metric": "latency_p50"})
    assert "invalid arguments" in result["error"]


# ---------------------------------------------------------------------------
# search_docs against an injected fake store
# ---------------------------------------------------------------------------


def test_search_docs_returns_ranked_chunks(
    seeded_store, fake_embedder, seeded_bm25, events
) -> None:
    result = dispatch_tool(
        "search_docs",
        {"query": "log.retention.hours", "k": 1, "mode": "bm25"},
        deps=_deps(seeded_store, fake_embedder, seeded_bm25),
    )
    assert result["results"]
    hit = result["results"][0]
    assert hit["chunk_id"] == "c2"
    assert hit["project"] == "kafka"
    assert hit["source_path"] == "a.md"
    assert hit["section_path"] == "Broker Configs"
    assert "log.retention.hours" in hit["text"]
    assert isinstance(hit["score"], float)


def test_search_docs_default_mode_and_k(
    seeded_store, fake_embedder, seeded_bm25, events
) -> None:
    result = dispatch_tool(
        "search_docs",
        {"query": "log compaction"},
        deps=_deps(seeded_store, fake_embedder, seeded_bm25),
    )
    assert len(result["results"]) <= 5  # default k


# ---------------------------------------------------------------------------
# Honest stubs
# ---------------------------------------------------------------------------


def test_get_trace_is_an_honest_stub(events) -> None:
    result = dispatch_tool("get_trace", {"trace_id": "t1"})
    assert result == {"error": "trace store not yet available (ClickHouse lands Day 3)"}


def test_query_metrics_is_an_honest_stub(events) -> None:
    result = dispatch_tool("query_metrics", {"metric": "latency_p50", "window": "1h"})
    assert result == {"error": "metrics store not yet available (ClickHouse lands Day 3)"}


# ---------------------------------------------------------------------------
# TOOL_CALL span emission
# ---------------------------------------------------------------------------


def test_dispatch_tool_emits_one_tool_call_span(events) -> None:
    dispatch_tool("get_trace", {"trace_id": "t1"})
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "TOOL_CALL"
    assert event["attributes"]["tool"] == "get_trace"
    assert event["attributes"]["args_preview"] == '{"trace_id": "t1"}'
    assert int(event["attributes"]["result_size"]) > 0
    assert event["status"] == "ok"


def test_dispatch_tool_span_emitted_even_on_validation_failure(events) -> None:
    dispatch_tool("search_docs", {})
    assert len(events) == 1
    assert events[0]["event_type"] == "TOOL_CALL"
    assert events[0]["status"] == "ok"  # a bad-args result is a handled error, not a span failure


def test_all_registered_tools_have_a_schema_and_description() -> None:
    assert set(TOOLS) == {"search_docs", "get_trace", "query_metrics"}
    for spec in TOOLS.values():
        assert spec.schema["type"] == "object"
        assert spec.description


# ---------------------------------------------------------------------------
# Cross-process trace propagation (ADR-003 #4) -- an optional _trace_context
# sidecar in arguments, stripped before validation/dispatch
# ---------------------------------------------------------------------------


def test_dispatch_tool_joins_caller_trace_via_trace_context(events) -> None:
    dispatch_tool(
        "get_trace",
        {
            "trace_id": "t1",
            "_trace_context": {"trace_id": "caller-trace", "parent_span_id": "caller-span"},
        },
    )
    (event,) = events
    assert event["trace_id"] == "caller-trace"
    assert event["parent_span_id"] == "caller-span"


def test_dispatch_tool_strips_trace_context_before_schema_validation(events) -> None:
    # Every tool schema sets additionalProperties: False -- an unstripped
    # "_trace_context" key would be reported as an invalid argument.
    result = dispatch_tool(
        "get_trace",
        {"trace_id": "t1", "_trace_context": {"trace_id": "x", "parent_span_id": "y"}},
    )
    assert result == {"error": "trace store not yet available (ClickHouse lands Day 3)"}


def test_dispatch_tool_args_preview_excludes_trace_context(events) -> None:
    dispatch_tool(
        "get_trace",
        {"trace_id": "t1", "_trace_context": {"trace_id": "x", "parent_span_id": "y"}},
    )
    assert "_trace_context" not in events[0]["attributes"]["args_preview"]


def test_dispatch_tool_without_trace_context_roots_its_own_trace(events) -> None:
    dispatch_tool("get_trace", {"trace_id": "t1"})
    assert events[0]["parent_span_id"] is None


# ---------------------------------------------------------------------------
# warmup() -- observer-effect fix (ADR-003, recurrence of ADR-000 #3)
# ---------------------------------------------------------------------------


def test_warmup_loads_embedder_and_touches_qdrant(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "services.rag.embed.FastEmbedEmbedder.embed",
        lambda self, texts: calls.append("embed"),
    )
    monkeypatch.setattr(
        "services.rag.qdrant_store.QdrantStore.count_chunks",
        lambda self: calls.append("count_chunks") or 0,
    )
    assert warmup() is True
    assert calls == ["embed", "count_chunks"]


def test_warmup_never_raises_on_failure(monkeypatch) -> None:
    def boom(self, texts):
        raise RuntimeError("model not cached and no network")

    monkeypatch.setattr("services.rag.embed.FastEmbedEmbedder.embed", boom)
    assert warmup() is False
