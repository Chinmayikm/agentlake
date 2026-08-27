"""BM25 sparse index -- the other half of hybrid retrieval, alongside dense.

rank_bm25 has no incremental-update API: BM25Okapi is built once from the
full tokenized corpus. Rebuilding from a few thousand chunks is
sub-millisecond, so BM25Index just invalidates its cached BM25Okapi on any
mutation and rebuilds lazily on the next search() -- boring, and fast enough
at this corpus size that a smarter incremental scheme isn't worth it.

Persisted as a single pickle file alongside the vector store's data
(services/rag/data/bm25_index.pkl, gitignored) -- ingest_state that belongs
to the local pipeline, not to whichever vector store backend (sqlite fake or
Qdrant) is in play.
"""

from __future__ import annotations

import pickle
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from services.rag.chunk import Chunk

if TYPE_CHECKING:
    from rank_bm25 import BM25Okapi

DEFAULT_BM25_PATH = Path(__file__).parent / "data" / "bm25_index.pkl"

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


@dataclass(frozen=True, slots=True)
class _Entry:
    project: str
    text: str


@dataclass
class BM25Index:
    """Lazy: rank_bm25 is imported inside _ensure_built(), not at module
    import time, same pattern as FastEmbedEmbedder.embed().
    """

    _entries: dict[str, _Entry] = field(default_factory=dict)  # chunk_id -> entry
    _doc_to_chunk_ids: dict[str, set[str]] = field(default_factory=dict)
    _bm25: BM25Okapi | None = field(default=None, init=False, repr=False)
    _chunk_ids: list[str] = field(default_factory=list, init=False, repr=False)

    def replace_chunks(self, doc_id: str, chunks: list[Chunk]) -> None:
        for old_chunk_id in self._doc_to_chunk_ids.pop(doc_id, ()):
            self._entries.pop(old_chunk_id, None)
        new_ids = set()
        for chunk in chunks:
            self._entries[chunk.chunk_id] = _Entry(project=chunk.project, text=chunk.text)
            new_ids.add(chunk.chunk_id)
        self._doc_to_chunk_ids[doc_id] = new_ids
        self._bm25 = None  # invalidate: rebuilt lazily on next search()

    def _ensure_built(self) -> None:
        if self._bm25 is not None or not self._entries:
            return
        from rank_bm25 import BM25Okapi

        self._chunk_ids = list(self._entries.keys())
        tokenized = [_tokenize(self._entries[cid].text) for cid in self._chunk_ids]
        self._bm25 = BM25Okapi(tokenized)

    def search(self, query: str, k: int, project: str | None = None) -> list[tuple[str, float]]:
        self._ensure_built()
        if self._bm25 is None:
            return []

        scores = self._bm25.get_scores(_tokenize(query))
        pairs = [
            (chunk_id, float(scores[i]))
            for i, chunk_id in enumerate(self._chunk_ids)
            if project is None or self._entries[chunk_id].project == project
        ]
        pairs.sort(key=lambda pair: -pair[1])
        return pairs[:k]

    def save(self, path: str | Path = DEFAULT_BM25_PATH) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as fh:
            pickle.dump({"entries": self._entries, "doc_to_chunk_ids": self._doc_to_chunk_ids}, fh)

    @classmethod
    def load(cls, path: str | Path = DEFAULT_BM25_PATH) -> BM25Index:
        path = Path(path)
        index = cls()
        if path.exists():
            with open(path, "rb") as fh:
                data = pickle.load(fh)  # our own pipeline's output, not untrusted input
            index._entries = data["entries"]
            index._doc_to_chunk_ids = data["doc_to_chunk_ids"]
        return index
