# agentlake
Streaming lakehouse & evaluation platform for LLM agents. Solo portfolio project.

## Architecture (current + planned)
- Avro TraceEvents (contracts/trace_event_v1.avsc) -> Kafka topic traces.events.v1, keyed by session_id
- Schema Registry at localhost:8081 enforces BACKWARD compatibility; Kafka at localhost:9092
- Qdrant (vector store) at localhost:6333, backs services/rag's dense retrieval index
- Planned: Flink -> Iceberg (cold), ClickHouse (hot), Trino+dbt, Debezium CDC, eval harness

## Services
- Telemetry SDK: services/sdk (`from services.sdk import session, span`). contextvars
  propagation, one trace per turn, emit failures swallowed+logged, Kafka built lazily
  (`warmup()` pre-builds it at startup). Details: docs/adr/ADR-000.
- Inference gateway: services/gateway, port 8100, `make gateway`. The only thing allowed
  to call an LLM provider or hold ANTHROPIC_API_KEY. POST /v1/chat (streams as SSE when
  stream=true), GET /v1/health, GET /v1/stats. Model aliases + prices in
  services/gateway/models.yaml, never hardcoded. Details: docs/adr/ADR-001.
- RAG corpus: services/rag, `python -m services.rag ingest|retrieve`. Hybrid (dense+BM25)
  retrieval over pinned Kafka/Flink/Iceberg docs; QdrantStore in production, sqlite+numpy
  as the test fake; emits RETRIEVAL spans. ingest: run with kafka+sr stopped (memory), it
  is resume-aware. Details: docs/adr/ADR-002.
- services/agent: not yet built.

## Environment
- WSL2 Ubuntu, 8 GB laptop, WSL capped at 4 GB -> every compose service needs mem_limit
- Python 3.14 venv at .venv; run modules from repo root: python -m services.xyz
- Secrets only in .env (gitignored, see .env.example) -- ANTHROPIC_API_KEY required to run
  the gateway; never log it, never commit .env

## Tests & CI
- `python -m pytest -q` -- no Kafka needed (injectable emitters block the Kafka path in
  tests, see tests/conftest.py); `ruff check services/ tests/` for lint (rule set pinned
  in pyproject.toml)
- CI (.github/workflows/ci.yml) on every PR: lint, test, and a schema-compat gate that
  only runs when contracts/ changed

## Conventions
- Trunk-based: feat/* branches, squash-merge PRs to main, conventional commits (feat:/fix:/docs:/test:/chore:)
- Never break Avro BACKWARD compatibility
- Prefer boring, explainable code over clever code; the owner must be able to whiteboard every design decision
- Type hints everywhere; tests must not require running Kafka (use injectable emitters)
