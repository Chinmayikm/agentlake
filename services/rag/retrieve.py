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

# Same reasoning, wider budget: source paths run ~50 chars each and the point
# of recording them is that five of them fit. Beyond this the value is cut and
# top_source_paths_truncated="true" is set alongside -- a shortened list that
# didn't say it was shortened would make hit@k quietly wrong rather than
# visibly incomplete.
_MAX_SOURCE_PATHS = 10
_MAX_SOURCE_PATHS_ATTR = 500

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
    from services.rag.qdrant_store import default_store

    return default_store()


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
        # corpus_version is a SEARCH FILTER on the production store, so a store
        # configured with the wrong one returns zero hits and looks exactly like
        # a bad query. Recording it is what makes "retrieval returned nothing"
        # answerable from the trace alone -- the question this attribute exists
        # because nobody could answer. None is dropped by _coerce_attrs, so the
        # sqlite fake (which has no such concept) emits an identical span.
        rspan.set(
            query=query[:_MAX_QUERY_ATTR],
            project=project,
            corpus_version=getattr(store, "corpus_version", None),
        )

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
        source_paths, paths_truncated = _source_paths_attr(results)
        rspan.set(
            hits=len(results),
            top_chunk_ids=",".join(r.chunk_id for r in results),
            top_scores=",".join(f"{r.score:.4f}" for r in results),
            # WHICH docs came back, not just their opaque ids. Without this a
            # consumer reading the trace has to resolve every chunk_id against
            # the store to learn anything -- i.e. ask the store a question the
            # trace was supposed to answer. eval/'s trace-mode hit@k reads this.
            top_source_paths=source_paths,
            top_source_paths_truncated=paths_truncated or None,
        )

    return results


def _source_paths_attr(results: list[RetrievedChunk]) -> tuple[str, bool]:
    """Comma-joined source paths, bounded, plus whether bounding kicked in.

    Deduplicated preserving rank order: several chunks of one document is the
    normal case, and repeating its path says nothing a consumer wants.
    """
    seen: list[str] = []
    for r in results[:_MAX_SOURCE_PATHS]:
        if r.source_path not in seen:
            seen.append(r.source_path)
    joined = ",".join(seen)
    if len(results) > _MAX_SOURCE_PATHS or len(joined) > _MAX_SOURCE_PATHS_ATTR:
        return joined[:_MAX_SOURCE_PATHS_ATTR], True
    return joined, False


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
