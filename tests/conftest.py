"""Shared fixtures for the telemetry SDK suite.

Every test runs with an injected list-collector emitter and no Kafka, per
CLAUDE.md. The _no_kafka fixture makes that structural rather than aspirational.
"""

import hashlib
import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import fastavro
import numpy as np
import pytest

from services.mcp_server import clickhouse
from services.sdk import TraceEvent, telemetry

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"


class FakeEmbedder:
    """Deterministic hash-of-text projection -- no fastembed/onnxruntime, no
    network, no model download. Same vector every time for the same text, so
    tests can assert on nearest-neighbor ordering.
    """

    def __init__(self, dim: int = 8) -> None:
        self.dim = dim

    def embed(self, texts: Sequence[str]) -> np.ndarray:
        rows = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            row = np.frombuffer(digest[: self.dim], dtype=np.uint8).astype(np.float32)
            norm = np.linalg.norm(row) or 1.0
            rows.append(row / norm)
        return np.stack(rows)


@pytest.fixture
def fake_embedder() -> FakeEmbedder:
    return FakeEmbedder()


@pytest.fixture
def events() -> Iterator[list[TraceEvent]]:
    """A fresh collector per test; restores the previously installed emitter."""
    collected: list[TraceEvent] = []
    previous = telemetry._EMITTER  # module-private by design: snapshot and restore
    telemetry.configure(collected.append)
    try:
        yield collected
    finally:
        telemetry._EMITTER = previous


@pytest.fixture(autouse=True)
def _no_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loudly if a test ever reaches the Kafka path.

    The SDK's default emitter auto-configures Kafka on first emit (ADR-000 #3),
    so a test that forgot the `events` fixture would otherwise quietly dial
    localhost:9092 and merely look slow. This turns that into a test failure.
    """

    def boom(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("test attempted to build a Kafka producer")

    monkeypatch.setattr(telemetry, "_get_kafka", boom)


@pytest.fixture(autouse=True)
def _no_clickhouse(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point every un-injected ClickHouse client at a closed port.

    Same job as _no_kafka, one layer over. services/mcp_server's get_trace and
    query_metrics build a real ClickHouseClient when no `ch` is injected, so a
    test that forgot to inject one would otherwise query whatever ClickHouse the
    developer happens to have running -- and pass or fail on their local data.
    That is not hypothetical: it is how the ADR-005 work first noticed this
    fixture was missing.

    Redirecting rather than raising, unlike _no_kafka, because "the store is
    unreachable" is itself a contract these tools are required to honour
    (ADR-003 #3) rather than an error: a forgotten injection produces the real
    ClickHouseUnavailable path via an instant loopback refusal, and the tests
    that assert that behaviour get to exercise the production code rather than
    a mock of it. Nothing leaves the machine; port 1 is privileged and closed.

    Set through the environment rather than by patching
    clickhouse.default_url, and that distinction is load-bearing:
    ClickHouseClient resolves its url with
    ``field(default_factory=default_url)``, which captures the function object
    when the class is defined, so rebinding the module attribute afterwards has
    no effect. default_url() reads os.environ on every call, so setenv does.
    (services/rag/qdrant_store.py has the same shape and the same caveat.)
    """
    monkeypatch.setenv("AGENTLAKE_CLICKHOUSE", "http://127.0.0.1:1")
    assert clickhouse.default_url() == "http://127.0.0.1:1"


@pytest.fixture(scope="session")
def contract() -> dict[str, Any]:
    """The raw parsed .avsc, so tests assert against the contract, not the code."""
    with open(CONTRACT_PATH, encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="session")
def schema_field_names(contract: dict[str, Any]) -> set[str]:
    return {field["name"] for field in contract["fields"]}


@pytest.fixture(scope="session")
def avro_schema(contract: dict[str, Any]) -> Any:
    return fastavro.parse_schema(contract)
