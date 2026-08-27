"""`python -m services.rag ingest|retrieve` -- see __main__.py."""

from __future__ import annotations

import argparse
import datetime as dt
from collections.abc import Sequence
from pathlib import Path

from services.rag.bm25 import DEFAULT_BM25_PATH, BM25Index
from services.rag.chunk import chunk_html, chunk_markdown, doc_id_for
from services.rag.embed import Embedder, FastEmbedEmbedder, embed_chunks
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


def _default_store() -> Store:
    from services.rag.qdrant_store import QdrantStore

    return QdrantStore(corpus_version=load_corpus_version())


def _ingest(
    project_filter: str | None,
    *,
    store: Store,
    embedder: Embedder,
    bm25_index: BM25Index,
    strategies: dict[str, FetchStrategy] | None = None,
    raw_dir: Path = DEFAULT_RAW_DIR,
    bm25_path: Path = DEFAULT_BM25_PATH,
) -> None:
    specs = load_sources()
    if project_filter and project_filter != "all":
        specs = [s for s in specs if s.name == project_filter]

    strategies = strategies or DEFAULT_STRATEGIES
    for spec in specs:
        strategy = strategies[spec.strategy]
        dest = raw_dir / spec.name / spec.version
        fetched_files = strategy(spec, dest)

        for fetched in fetched_files:
            doc_id = doc_id_for(spec.name, spec.version, fetched.source_path)
            if store.content_hash_for(doc_id) == fetched.content_hash:
                continue  # unchanged: skip the expensive chunk+embed+store step

            text = fetched.local_path.read_text(encoding="utf-8", errors="replace")
            chunk_fn = chunk_html if fetched.local_path.suffix in _HTML_SUFFIXES else chunk_markdown
            chunks = chunk_fn(
                text, project=spec.name, version=spec.version, source_path=fetched.source_path
            )
            if not chunks:
                continue

            embeddings = embed_chunks(chunks, embedder)
            store.upsert_document(
                doc_id=doc_id,
                project=spec.name,
                version=spec.version,
                source_path=fetched.source_path,
                fetched_at=dt.datetime.now(dt.UTC).isoformat(),
                content_hash=fetched.content_hash,
            )
            store.replace_chunks(doc_id, chunks, embeddings, embedder.__class__.__name__)
            bm25_index.replace_chunks(doc_id, chunks)

    bm25_index.save(bm25_path)


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
        _ingest(
            args.project,
            store=store,
            embedder=embedder,
            bm25_index=bm25_index,
            strategies=strategies,
            raw_dir=raw_dir,
            bm25_path=bm25_path,
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
