"""agentlake RAG corpus -- fetch -> chunk -> embed -> store -> retrieve.

    from services.rag import retrieve

Local pipeline for pinned Kafka/Flink/Iceberg docs. QdrantStore is the
production Store backend; CorpusStore (sqlite+numpy) is the Store protocol's
test fake. retrieve() runs hybrid (dense + BM25, fused via reciprocal rank
fusion) by default and emits a RETRIEVAL span per call. See ADR-002.
"""

from services.rag.retrieve import RetrievedChunk, retrieve
from services.rag.store import CorpusStore, Store

__all__ = ["CorpusStore", "RetrievedChunk", "Store", "retrieve"]
