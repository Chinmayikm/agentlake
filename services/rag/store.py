"""sqlite-backed corpus store: metadata + vectors in one file, one table each.

This is the Store protocol's TEST FAKE and reference implementation, not the
production backend -- QdrantStore (services/rag/qdrant_store.py) is. See
ADR-002 #1: brute-force cosine over a numpy matrix is exactly right for a
hermetic unit test (real stdlib sqlite3, no server, sub-millisecond), but
production retrieval needs payload filtering and a real service boundary,
which is what Qdrant provides.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Protocol

import numpy as np

from services.rag.chunk import Chunk

DEFAULT_DB_PATH = Path(__file__).parent / "data" / "corpus.db"


class Store(Protocol):
    """Structural interface shared by CorpusStore (test fake) and QdrantStore
    (production). Callers -- retrieve.py, cli.py -- depend on this, never on
    a concrete implementation.
    """

    def content_hash_for(self, doc_id: str) -> str | None: ...

    def upsert_document(
        self,
        doc_id: str,
        project: str,
        version: str,
        source_path: str,
        fetched_at: str,
        content_hash: str,
    ) -> None: ...

    def replace_chunks(
        self, doc_id: str, chunks: list[Chunk], embeddings: np.ndarray, embedding_model: str
    ) -> None: ...

    def get_chunk(self, chunk_id: str) -> Chunk: ...

    def search(
        self, query_vec: np.ndarray, k: int, project: str | None = None
    ) -> list[tuple[str, float]]: ...

    def close(self) -> None: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id       TEXT PRIMARY KEY,
    project      TEXT NOT NULL,
    version      TEXT NOT NULL,
    source_path  TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    content_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id        TEXT PRIMARY KEY,
    doc_id          TEXT NOT NULL REFERENCES documents(doc_id),
    project         TEXT NOT NULL,
    version         TEXT NOT NULL,
    section         TEXT NOT NULL,
    source_path     TEXT NOT NULL,
    chunk_index     INTEGER NOT NULL,
    text            TEXT NOT NULL,
    embedding       BLOB NOT NULL,
    embedding_dim   INTEGER NOT NULL,
    embedding_model TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chunks_project_version ON chunks(project, version);
"""


class CorpusStore:
    def __init__(self, path: str | Path = DEFAULT_DB_PATH) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> CorpusStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def content_hash_for(self, doc_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT content_hash FROM documents WHERE doc_id = ?", (doc_id,)
        ).fetchone()
        return row[0] if row else None

    def upsert_document(
        self,
        doc_id: str,
        project: str,
        version: str,
        source_path: str,
        fetched_at: str,
        content_hash: str,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO documents (doc_id, project, version, source_path, fetched_at, content_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(doc_id) DO UPDATE SET
                fetched_at = excluded.fetched_at, content_hash = excluded.content_hash
            """,
            (doc_id, project, version, source_path, fetched_at, content_hash),
        )
        self._conn.commit()

    def replace_chunks(
        self,
        doc_id: str,
        chunks: list[Chunk],
        embeddings: np.ndarray,
        embedding_model: str,
    ) -> None:
        self._conn.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))
        self._conn.executemany(
            """
            INSERT INTO chunks (
                chunk_id, doc_id, project, version, section, source_path,
                chunk_index, text, embedding, embedding_dim, embedding_model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    chunk.chunk_id,
                    chunk.doc_id,
                    chunk.project,
                    chunk.version,
                    chunk.section,
                    chunk.source_path,
                    chunk.chunk_index,
                    chunk.text,
                    vector.astype(np.float32).tobytes(),
                    vector.shape[0],
                    embedding_model,
                )
                for chunk, vector in zip(chunks, embeddings, strict=True)
            ],
        )
        self._conn.commit()

    def get_chunk(self, chunk_id: str) -> Chunk:
        row = self._conn.execute(
            """
            SELECT chunk_id, doc_id, project, version, section, source_path, chunk_index, text
            FROM chunks WHERE chunk_id = ?
            """,
            (chunk_id,),
        ).fetchone()
        if row is None:
            raise KeyError(chunk_id)
        return Chunk(
            chunk_id=row[0],
            doc_id=row[1],
            project=row[2],
            version=row[3],
            section=row[4],
            source_path=row[5],
            chunk_index=row[6],
            text=row[7],
        )

    def search(
        self, query_vec: np.ndarray, k: int, project: str | None = None
    ) -> list[tuple[str, float]]:
        if project is None:
            rows = self._conn.execute("SELECT chunk_id, embedding FROM chunks").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT chunk_id, embedding FROM chunks WHERE project = ?", (project,)
            ).fetchall()
        if not rows:
            return []

        chunk_ids = [row[0] for row in rows]
        matrix = np.stack(
            [np.frombuffer(row[1], dtype=np.float32) for row in rows]
        )  # (n, dim)
        query = query_vec.astype(np.float32)
        query_norm = np.linalg.norm(query) or 1.0
        matrix_norms = np.linalg.norm(matrix, axis=1)
        matrix_norms[matrix_norms == 0] = 1.0
        scores = (matrix @ query) / (matrix_norms * query_norm)

        top_indices = np.argsort(-scores)[:k]
        return [(chunk_ids[i], float(scores[i])) for i in top_indices]
