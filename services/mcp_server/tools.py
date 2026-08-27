"""The three tool implementations. Plain Python in, plain dict out -- no MCP
types here, so these are testable as ordinary functions (see
tests/test_mcp_server.py) and framework-agnostic if the transport ever changes.
"""

from __future__ import annotations

from services.rag.bm25 import BM25Index
from services.rag.embed import Embedder
from services.rag.retrieve import RetrievalMode, retrieve
from services.rag.store import Store

_TRACE_STORE_UNAVAILABLE = "trace store not yet available (ClickHouse lands Day 3)"
_METRICS_STORE_UNAVAILABLE = "metrics store not yet available (ClickHouse lands Day 3)"


def search_docs(
    query: str,
    k: int = 5,
    mode: RetrievalMode = "hybrid",
    *,
    store: Store | None = None,
    embedder: Embedder | None = None,
    bm25_index: BM25Index | None = None,
) -> dict:
    """Wrap services.rag.retrieve() as a tool result.

    store/embedder/bm25_index are injectable purely for tests -- production
    calls (via server.dispatch_tool with no `deps`) always pass None here, so
    retrieve() falls back to its own real defaults (QdrantStore,
    FastEmbedEmbedder, BM25Index.load()), same as the CLI in services/rag/cli.py.
    """
    chunks = retrieve(query, k, mode=mode, store=store, embedder=embedder, bm25_index=bm25_index)
    return {
        "results": [
            {
                "chunk_id": chunk.chunk_id,
                "project": chunk.project,
                "source_path": chunk.source_path,
                "section_path": chunk.section,
                "text": chunk.text,
                "score": chunk.score,
            }
            for chunk in chunks
        ]
    }


def get_trace(trace_id: str) -> dict:
    """Look up one agent turn's span tree by trace_id.

    Honest stub: the trace store (ClickHouse) does not exist yet. This always
    returns a structured error, never fabricated spans -- an observability
    platform that invents data about its own traces when the real store is
    down would be worse than useless. Replace the body once ClickHouse lands
    (Day 3); the {"error": ...} shape stays the correct response for whenever
    the trace store is unreachable, even after that.
    """
    return {"error": _TRACE_STORE_UNAVAILABLE}


def query_metrics(metric: str, window: str) -> dict:
    """Query an aggregate metric (e.g. latency_p50) over a time window.

    Honest stub, same rationale as get_trace: the metrics store (ClickHouse)
    does not exist yet, so this always returns a structured error rather than
    a plausible-looking number. Replace the body once ClickHouse lands (Day 3).
    """
    return {"error": _METRICS_STORE_UNAVAILABLE}
