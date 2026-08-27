"""Turns chunk text into vectors.

Local ONNX model via fastembed -- no API key, no network at inference time
(only on first model download). This sits outside ADR-001's "gateway is the
only door to a provider" rule: Anthropic has no embeddings endpoint, so
embeddings were never a gateway question. See ADR-002 #2.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import numpy as np

if TYPE_CHECKING:
    from services.rag.chunk import Chunk

DEFAULT_MODEL_NAME = "BAAI/bge-small-en-v1.5"
DEFAULT_MODEL_DIM = 384
DEFAULT_MODEL_CACHE = Path(__file__).parent / "data" / "models"
# Caps peak RAM during one embed() call rather than materializing the whole
# input list's activations at once -- the WSL 4GB cap is the binding
# constraint here, not throughput.
DEFAULT_BATCH_SIZE = 8


class Embedder(Protocol):
    dim: int

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        """Return an (n, dim) float32 array, L2-normalized rows."""
        ...


@dataclass
class FastEmbedEmbedder:
    """Lazy: fastembed/onnxruntime are imported inside embed(), not at module
    import time, so `import services.rag` stays cheap and network-free --
    same pattern as services.sdk._get_kafka().
    """

    model_name: str = DEFAULT_MODEL_NAME
    cache_dir: Path = DEFAULT_MODEL_CACHE
    dim: int = DEFAULT_MODEL_DIM
    batch_size: int = DEFAULT_BATCH_SIZE
    _model: object | None = field(default=None, init=False, repr=False)

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        if self._model is None:
            from fastembed import TextEmbedding

            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self._model = TextEmbedding(model_name=self.model_name, cache_dir=str(self.cache_dir))
        vectors = self._model.embed(list(texts), batch_size=self.batch_size)
        return np.array(list(vectors), dtype=np.float32)


def embed_chunks(chunks: Sequence[Chunk], embedder: Embedder) -> np.ndarray:
    return embedder.embed([c.text for c in chunks])
