import subprocess
import sys
from pathlib import Path

import numpy as np

from services.rag.chunk import chunk_markdown
from services.rag.embed import embed_chunks

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_embed_chunks_returns_normalized_float32_matrix(fake_embedder) -> None:
    chunks = chunk_markdown(
        "# Heading\n\nsome body text", project="kafka", version="3.8", source_path="a.md"
    )
    matrix = embed_chunks(chunks, fake_embedder)

    assert matrix.shape == (len(chunks), fake_embedder.dim)
    assert matrix.dtype == np.float32
    norms = np.linalg.norm(matrix, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-5)


def test_embed_is_deterministic_per_text(fake_embedder) -> None:
    first = fake_embedder.embed(["hello world"])
    second = fake_embedder.embed(["hello world"])
    assert np.array_equal(first, second)


def test_import_does_not_pull_in_fastembed() -> None:
    """Run in a subprocess: an in-process check would depend on test ordering."""
    code = (
        "import sys, services.rag\n"
        "leaked = sorted(m for m in sys.modules if 'fastembed' in m or 'onnxruntime' in m)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
