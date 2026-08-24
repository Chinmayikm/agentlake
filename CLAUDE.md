# agentlake
Streaming lakehouse & evaluation platform for LLM agents. Solo portfolio project.

## Architecture (current + planned)
- Avro TraceEvents (contracts/trace_event_v1.avsc) -> Kafka topic traces.events.v1, keyed by session_id
- Schema Registry at localhost:8081 enforces BACKWARD compatibility; Kafka at localhost:9092
- Planned: Flink -> Iceberg (cold), ClickHouse (hot), Trino+dbt, Debezium CDC, eval harness

## Environment
- WSL2 Ubuntu, 8 GB laptop, WSL capped at 4 GB -> every compose service needs mem_limit
- Python 3.14 venv at .venv; run modules from repo root: python -m services.xyz

## Conventions
- Trunk-based: feat/* branches, squash-merge PRs to main, conventional commits (feat:/fix:/docs:/test:/chore:)
- Never break Avro BACKWARD compatibility; secrets only in .env (gitignored)
- Prefer boring, explainable code over clever code; the owner must be able to whiteboard every design decision
- Type hints everywhere; tests must not require running Kafka (use injectable emitters)
