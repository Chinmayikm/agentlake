"""Qdrant-backed store -- the production backend for real ingest/retrieve.

Kept behind the same Store protocol as CorpusStore (services/rag/store.py),
which remains the sqlite+numpy test fake. See ADR-002 #1 for why Qdrant is
the production choice: payload filtering (project, corpus_version) at the
database layer instead of loading everything into a Python process, and its
own service boundary -- one more docker-compose service, mem_limit-budgeted,
matching how ADR-001 treats the gateway as its own service rather than a
library everything links.

qdrant-client is imported lazily inside _get_client(), not at module import
time -- same pattern as services.sdk._get_kafka() and FastEmbedEmbedder.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from services.rag.chunk import Chunk

if TYPE_CHECKING:
    from qdrant_client import QdrantClient

DEFAULT_URL = "http://localhost:6333"
DEFAULT_COLLECTION = "docs_v1"
DEFAULT_DIM = 384

# "kind" distinguishes the two point types sharing one collection: a document
# marker (zero vector, never returned by search()) and a real chunk (a real
# embedding). One collection, not two, keeps corpus_version filtering and
# collection lifecycle in one place instead of two.
_KIND_DOCUMENT = "document"
_KIND_CHUNK = "chunk"


def _point_id(hex_id: str) -> int:
    """chunk_id/doc_id are 16-hex-char sha1 prefixes (64 bits) -- Qdrant point
    ids accept an unsigned int directly, so no UUID conversion is needed.
    """
    return int(hex_id, 16)


def default_url() -> str:
    return os.environ.get("AGENTLAKE_QDRANT", DEFAULT_URL)


@dataclass
class QdrantStore:
    url: str = field(default_factory=default_url)
    collection: str = DEFAULT_COLLECTION
    dim: int = DEFAULT_DIM
    corpus_version: str = "unknown"
    _client: object | None = field(default=None, init=False, repr=False)

    def _get_client(self) -> QdrantClient:
        if self._client is None:
            from qdrant_client import QdrantClient
            from qdrant_client.http import models as qm

            client = QdrantClient(url=self.url)
            if not client.collection_exists(self.collection):
                client.create_collection(
                    collection_name=self.collection,
                    vectors_config=qm.VectorParams(size=self.dim, distance=qm.Distance.COSINE),
                )
            self._client = client
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> QdrantStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def content_hash_for(self, doc_id: str) -> str | None:
        client = self._get_client()
        points = client.retrieve(self.collection, ids=[_point_id(doc_id)], with_payload=True)
        if not points:
            return None
        return points[0].payload.get("content_hash")

    def upsert_document(
        self,
        doc_id: str,
        project: str,
        version: str,
        source_path: str,
        fetched_at: str,
        content_hash: str,
    ) -> None:
        from qdrant_client.http import models as qm

        client = self._get_client()
        client.upsert(
            collection_name=self.collection,
            points=[
                qm.PointStruct(
                    id=_point_id(doc_id),
                    vector=[0.0] * self.dim,
                    payload={
                        "kind": _KIND_DOCUMENT,
                        "doc_id": doc_id,
                        "project": project,
                        "version": version,
                        "source_path": source_path,
                        "fetched_at": fetched_at,
                        "content_hash": content_hash,
                        "corpus_version": self.corpus_version,
                    },
                )
            ],
        )

    def replace_chunks(
        self, doc_id: str, chunks: list[Chunk], embeddings: np.ndarray, embedding_model: str
    ) -> None:
        from qdrant_client.http import models as qm

        client = self._get_client()
        client.delete(
            collection_name=self.collection,
            points_selector=qm.FilterSelector(
                filter=qm.Filter(
                    must=[
                        qm.FieldCondition(key="doc_id", match=qm.MatchValue(value=doc_id)),
                        qm.FieldCondition(key="kind", match=qm.MatchValue(value=_KIND_CHUNK)),
                    ]
                )
            ),
        )
        points = [
            qm.PointStruct(
                id=_point_id(chunk.chunk_id),
                vector=vector.astype(np.float32).tolist(),
                payload={
                    "kind": _KIND_CHUNK,
                    "chunk_id": chunk.chunk_id,
                    "doc_id": chunk.doc_id,
                    "project": chunk.project,
                    "version": chunk.version,
                    "section": chunk.section,
                    "source_path": chunk.source_path,
                    "chunk_index": chunk.chunk_index,
                    "text": chunk.text,
                    "embedding_model": embedding_model,
                    "corpus_version": self.corpus_version,
                },
            )
            for chunk, vector in zip(chunks, embeddings, strict=True)
        ]
        if points:
            client.upsert(collection_name=self.collection, points=points)

    def get_chunk(self, chunk_id: str) -> Chunk:
        client = self._get_client()
        points = client.retrieve(self.collection, ids=[_point_id(chunk_id)], with_payload=True)
        if not points:
            raise KeyError(chunk_id)
        payload = points[0].payload
        return Chunk(
            chunk_id=payload["chunk_id"],
            doc_id=payload["doc_id"],
            project=payload["project"],
            version=payload["version"],
            section=payload["section"],
            source_path=payload["source_path"],
            chunk_index=payload["chunk_index"],
            text=payload["text"],
        )

    def search(
        self, query_vec: np.ndarray, k: int, project: str | None = None
    ) -> list[tuple[str, float]]:
        """Filters on corpus_version -- the stale-index guard.

        A point written under a different corpus_version (embedding model
        changed, sources.yaml version bumped, re-ingest not yet run) never
        matches this filter, so a version mismatch surfaces as fewer/zero
        results rather than silently serving stale or dimension-mismatched
        vectors alongside current ones.
        """
        from qdrant_client.http import models as qm

        client = self._get_client()
        must = [
            qm.FieldCondition(key="kind", match=qm.MatchValue(value=_KIND_CHUNK)),
            qm.FieldCondition(key="corpus_version", match=qm.MatchValue(value=self.corpus_version)),
        ]
        if project is not None:
            must.append(qm.FieldCondition(key="project", match=qm.MatchValue(value=project)))

        result = client.query_points(
            collection_name=self.collection,
            query=query_vec.astype(np.float32).tolist(),
            query_filter=qm.Filter(must=must),
            limit=k,
            with_payload=True,
        )
        return [(point.payload["chunk_id"], float(point.score)) for point in result.points]
