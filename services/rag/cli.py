"""`python -m services.rag ingest|retrieve` -- see __main__.py."""

from __future__ import annotations

import argparse
import datetime as dt
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from services.rag.bm25 import DEFAULT_BM25_PATH, BM25Index
from services.rag.chunk import Chunk, chunk_html, chunk_markdown, doc_id_for
from services.rag.embed import DEFAULT_BATCH_SIZE, Embedder, FastEmbedEmbedder, embed_chunks
from services.rag.fetch import (
    DEFAULT_RAW_DIR,
    DEFAULT_STRATEGIES,
    FetchStrategy,
    load_corpus_version,
    load_sources,
)
from services.rag.retrieve import RetrievalMode
from services.rag.retrieve import retrieve as retrieve_fn
from services.rag.store import Store

_HTML_SUFFIXES = (".html", ".htm")

# How often _ingest prints a progress line, in chunks embedded (within the
# current document -- see _ingest's docstring on why "current doc" and not a
# grand total across all docs).
_PROGRESS_EVERY = 25


def _default_store() -> Store:
    from services.rag.qdrant_store import QdrantStore

    return QdrantStore(corpus_version=load_corpus_version())


def _batched(items: list[Chunk], size: int) -> Iterator[list[Chunk]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _ingest(
    project_filter: str | None,
    *,
    store: Store,
    embedder: Embedder,
    bm25_index: BM25Index,
    strategies: dict[str, FetchStrategy] | None = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
    bm25_path: Path = DEFAULT_BM25_PATH,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, Any]:
    """Resume-aware: for each doc, only chunk_ids missing from the store are
    (re-)embedded, in batches of `batch_size`. store.upsert_document() (which
    marks the doc as fully ingested via content_hash) only runs AFTER every
    chunk is confirmed present -- so a run killed mid-document leaves that
    doc's content_hash unset, and the next run re-detects it as needing work,
    picks up existing_chunk_ids(), and only embeds what's still missing.
    """
    specs = load_sources()
    if project_filter and project_filter != "all":
        specs = [s for s in specs if s.name == project_filter]

    strategies = strategies or DEFAULT_STRATEGIES
    docs_fetched = 0
    chunks_produced = 0
    dedup_drops = 0

    for spec in specs:
        strategy = strategies[spec.strategy]
        dest = raw_dir / spec.name / spec.version
        fetched_files = strategy(spec, dest)
        docs_fetched += len(fetched_files)

        for fetched in fetched_files:
            doc_id = doc_id_for(spec.name, spec.version, fetched.source_path)
            if store.content_hash_for(doc_id) == fetched.content_hash:
                dedup_drops += 1
                continue  # unchanged: skip the expensive chunk+embed+store step

            text = fetched.local_path.read_text(encoding="utf-8", errors="replace")
            chunk_fn = chunk_html if fetched.local_path.suffix in _HTML_SUFFIXES else chunk_markdown
            chunks = chunk_fn(
                text, project=spec.name, version=spec.version, source_path=fetched.source_path
            )
            if not chunks:
                continue
            chunks_produced += len(chunks)

            existing_ids = store.existing_chunk_ids(doc_id)
            missing = [c for c in chunks if c.chunk_id not in existing_ids]
            done = len(chunks) - len(missing)
            print(
                f"{fetched.source_path}: {done}/{len(chunks)} chunks already present, "
                f"embedding {len(missing)} more",
                flush=True,
            )

            for batch in _batched(missing, batch_size):
                embeddings = embed_chunks(batch, embedder)
                store.upsert_chunks(doc_id, batch, embeddings, embedder.__class__.__name__)
                done += len(batch)
                if done % _PROGRESS_EVERY < batch_size or done == len(chunks):
                    print(
                        f"  {fetched.source_path}: {done}/{len(chunks)} chunks "
                        f"(points_count={store.count_chunks()})",
                        flush=True,
                    )

            stale = existing_ids - {c.chunk_id for c in chunks}
            store.delete_chunks(stale)
            bm25_index.replace_chunks(doc_id, chunks)

            # Only now is doc_id fully present -- see docstring.
            store.upsert_document(
                doc_id=doc_id,
                project=spec.name,
                version=spec.version,
                source_path=fetched.source_path,
                fetched_at=dt.datetime.now(dt.UTC).isoformat(),
                content_hash=fetched.content_hash,
            )

    bm25_index.save(bm25_path)
    return {
        "docs_fetched": docs_fetched,
        "chunks_produced": chunks_produced,
        "dedup_drops": dedup_drops,
        "bm25_entries": len(bm25_index),
        "points_count": store.count_chunks(),
    }


def _retrieve(
    query: str,
    k: int,
    project: str | None,
    mode: RetrievalMode,
    *,
    store: Store,
    embedder: Embedder,
    bm25_index: BM25Index,
) -> None:
    hits = retrieve_fn(
        query, k, project=project, mode=mode, store=store, embedder=embedder, bm25_index=bm25_index
    )
    for hit in hits:
        print(f"[{hit.score:.3f}] {hit.project} {hit.version} :: {hit.section}")
        print(f"  {hit.source_path}")
        print(f"  {hit.text[:200]}")
        print()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m services.rag")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="fetch, chunk, embed, and store docs")
    ingest_parser.add_argument("--project", default="all")

    retrieve_parser = subparsers.add_parser("retrieve", help="query the corpus")
    retrieve_parser.add_argument("query")
    retrieve_parser.add_argument("--k", type=int, default=4)
    retrieve_parser.add_argument("--project", default=None)
    retrieve_parser.add_argument(
        "--mode", choices=("dense", "bm25", "hybrid"), default="hybrid"
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    store: Store | None = None,
    embedder: Embedder | None = None,
    bm25_index: BM25Index | None = None,
    strategies: dict[str, FetchStrategy] | None = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
    bm25_path: Path = DEFAULT_BM25_PATH,
) -> int:
    args = build_parser().parse_args(argv)

    store = store or _default_store()
    embedder = embedder or FastEmbedEmbedder()
    bm25_index = bm25_index if bm25_index is not None else BM25Index.load(bm25_path)

    if args.command == "ingest":
        import time

        start = time.perf_counter()
        stats = _ingest(
            args.project,
            store=store,
            embedder=embedder,
            bm25_index=bm25_index,
            strategies=strategies,
            raw_dir=raw_dir,
            bm25_path=bm25_path,
        )
        wall_s = time.perf_counter() - start
        print(
            f"done: docs_fetched={stats['docs_fetched']} "
            f"chunks_produced={stats['chunks_produced']} dedup_drops={stats['dedup_drops']} "
            f"bm25_entries={stats['bm25_entries']} points_count={stats['points_count']} "
            f"wall_time={wall_s:.1f}s"
        )
    elif args.command == "retrieve":
        _retrieve(
            args.query,
            args.k,
            args.project,
            args.mode,
            store=store,
            embedder=embedder,
            bm25_index=bm25_index,
        )
    return 0
