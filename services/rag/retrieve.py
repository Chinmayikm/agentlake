"""The public query API: retrieve(query, k) -> ranked RetrievedChunk list.

Every call emits a RETRIEVAL span via services.sdk -- an observability
platform's own retrieval path cannot be the one unobservable thing in it.
Shape matches the span already stubbed in services/demo_sdk.py
(`span("RETRIEVAL", "vector_search", index="docs-v1", top_k=4)`); k maps to
top_k, mode is recorded as an attribute.

mode="hybrid" (the default) fuses dense (embedding cosine similarity, via
Store.search()) and sparse (BM25Index) rankings with reciprocal rank fusion
-- see ADR-002 #3 and fusion.py. mode="dense" or mode="bm25" run either
ranking alone, useful for an A/B eval comparing them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from services.rag.bm25 import BM25Index
from services.rag.embed import Embedder, FastEmbedEmbedder
from services.rag.fusion import DEFAULT_RRF_K, reciprocal_rank_fusion
from services.rag.store import Store
from services.sdk import span

RetrievalMode = Literal["dense", "bm25", "hybrid"]

_VALID_MODES = ("dense", "bm25", "hybrid")

# A span attribute is a string in a map<string,string> Avro field -- truncate
# so a pathological query can't balloon the emitted event. Mirrors
# services.sdk.telemetry._MAX_ERROR_MESSAGE's reasoning.
_MAX_QUERY_ATTR = 200

# How many candidates to pull from each ranking before fusing. Wider than the
# final k so RRF has enough of each list to actually blend -- fusing two
# k=4 lists barely fuses anything.
_MIN_CANDIDATE_POOL = 20
_CANDIDATE_MULTIPLIER = 5


@dataclass(frozen=True, slots=True)
class RetrievedChunk:
    chunk_id: str
    project: str
    version: str
    section: str
    source_path: str
    text: str
    score: float


def _default_store() -> Store:
    from services.rag.qdrant_store import QdrantStore

    return QdrantStore()


def _candidate_pool(k: int) -> int:
    return max(_MIN_CANDIDATE_POOL, k * _CANDIDATE_MULTIPLIER)


def _dense_search(
    query: str, pool: int, project: str | None, store: Store, embedder: Embedder
) -> list[tuple[str, float]]:
    query_vec = embedder.embed([query])[0]
    return store.search(query_vec, pool, project=project)


def _bm25_search(
    query: str, pool: int, project: str | None, bm25_index: BM25Index
) -> list[tuple[str, float]]:
    return bm25_index.search(query, pool, project=project)


def retrieve(
    query: str,
    k: int = 4,
    *,
    project: str | None = None,
    mode: RetrievalMode = "hybrid",
    store: Store | None = None,
    embedder: Embedder | None = None,
    bm25_index: BM25Index | None = None,
    rrf_k: int = DEFAULT_RRF_K,
) -> list[RetrievedChunk]:
    if mode not in _VALID_MODES:
        raise ValueError(f"unknown mode {mode!r}; expected one of {_VALID_MODES}")

    store = store or _default_store()
    embedder = embedder or FastEmbedEmbedder()
    bm25_index = bm25_index if bm25_index is not None else BM25Index.load()

    with span("RETRIEVAL", "vector_search", index="docs-v1", top_k=k, mode=mode) as rspan:
        rspan.set(query=query[:_MAX_QUERY_ATTR], project=project)

        pool = _candidate_pool(k)
        if mode == "dense":
            hits = _dense_search(query, pool, project, store, embedder)[:k]
        elif mode == "bm25":
            hits = _bm25_search(query, pool, project, bm25_index)[:k]
        else:
            dense_hits = _dense_search(query, pool, project, store, embedder)
            bm25_hits = _bm25_search(query, pool, project, bm25_index)
            hits = reciprocal_rank_fusion([dense_hits, bm25_hits], k=rrf_k)[:k]

        results = [
            _to_retrieved_chunk(store, chunk_id, score) for chunk_id, score in hits
        ]
        rspan.set(
            hits=len(results),
            top_chunk_ids=",".join(r.chunk_id for r in results),
            top_scores=",".join(f"{r.score:.4f}" for r in results),
        )

    return results


def _to_retrieved_chunk(store: Store, chunk_id: str, score: float) -> RetrievedChunk:
    chunk = store.get_chunk(chunk_id)
    return RetrievedChunk(
        chunk_id=chunk.chunk_id,
        project=chunk.project,
        version=chunk.version,
        section=chunk.section,
        source_path=chunk.source_path,
        text=chunk.text,
        score=score,
    )
