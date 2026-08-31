# agentlake
Streaming lakehouse & evaluation platform for LLM agents. Solo portfolio project.

## Architecture (current + planned)
- Avro TraceEvents (contracts/trace_event_v1.avsc) -> Kafka topic traces.events.v1, keyed by session_id
- Schema Registry at localhost:8081 enforces BACKWARD compatibility; Kafka at localhost:9092
- Qdrant (vector store) at localhost:6333, backs services/rag's dense retrieval index
- Cold path: Flink SQL -> Iceberg on MinIO, via an Iceberg REST catalog. Tables
  lake.raw.trace_events and lake.curated.agg_model_5m. See stream/flink + ADR-004.
- Hot path: ClickHouse Kafka engine -> agentlake.trace_events_rt (7-day TTL), with
  Grafana provisioned on top. See stream/clickhouse + dashboards/ + ADR-005.
- Planned: Trino+dbt, Debezium CDC, eval harness

## Compose profiles
`docker-compose.yml` is sliced so only one heavy piece runs at a time (4 GB WSL cap):
- `docker compose up -d` -- kafka + schema-registry only (the spine; unprofiled)
- `docker compose --profile rag up -d` -- + qdrant
- `docker compose --profile streaming up -d` (`make stream-up`) -- + minio,
  iceberg-rest, flink jobmanager/taskmanager. Measured peak: ~1.8 GB across all six
  containers, host ~2.9 of 3.9 GB. Ports: MinIO 9000 (S3) / 9001 (console),
  Iceberg REST 8181, **Flink dashboard 8082** (8081 is Schema Registry).
- `docker compose --profile hotpath up -d` (`make hot-up`) -- + clickhouse, grafana.
  Measured: 953 MB across all four containers, host 2.3 of 3.9 GB. Ports:
  **ClickHouse 8123** (HTTP; native 9000 is container-internal only, so it cannot
  collide with MinIO), **Grafana 3000** (anonymous Viewer, no login; admin/admin at
  /login to edit).

Run `streaming` OR `hotpath`, not both -- each is sized to fit beside the spine, not
beside the other. They do not fight over host ports, so it is a memory decision.

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
- MCP server: services/mcp_server, stdio transport, `python -m services.mcp_server`.
  Exposes search_docs (wraps services.rag.retrieve), get_trace and query_metrics (both
  real as of ADR-005, backed by ClickHouse). query_metrics takes a WHITELISTED
  metric/window/group_by -- never free-form SQL -- and returns the SQL it ran so the
  agent's answer can be checked; get_trace returns a nested span tree. Both return a
  structured {"error": ...} rather than fabricate when the store is unreachable or the
  trace is gone. Every tool call emits a TOOL_CALL span. Details: docs/adr/ADR-003 and
  ADR-005.
- Agent: services/agent, `python -m services.agent "question" [--session ID] [--quality]`.
  Bounded tool-use loop (max 8 steps) over the gateway + services/mcp_server (spawned as
  a stdio subprocess -- a real MCP client, never a direct retrieve() import). Details:
  docs/adr/ADR-003.
- Cold path: stream/flink. Two Flink SQL jobs (no PyFlink, no DataStream API) from
  traces.events.v1 into Iceberg -- `01_raw_sink.sql` (append, all 13 contract fields,
  hidden-partitioned by day(ts_epoch_ms)) and `02_agg_model_5m.sql` (event-time 5-min
  tumbling aggregates by event_type + model). 30s watermark, 60s source idleness, 30s
  exactly-once checkpoints to MinIO. Details: docs/adr/ADR-004.

  Cold start, in order:

      make flink-jars     # once: ~172 MB of pinned connector jars -> stream/flink/lib
      make stream-up      # compose --profile streaming
      make flink-tables   # creates the Iceberg tables via the REST catalog
      make flink-jobs     # submits both SQL jobs; they run until cancelled
      make traffic        # 600 spans through the real SDK

  Tables are created by `python -m stream.flink.create_tables`, NOT by the SQL:
  Flink DDL cannot express Iceberg hidden partitioning (ADR-004 #2). Flink only
  ever INSERTs. `--recreate` drops and rebuilds them (destructive; dev reset).

  Verification queries live in stream/flink/verify/ and run with
  `make flink-verify`. They are batch jobs and need a task slot, and the two
  streaming jobs hold both -- run `make flink-stop` first or they queue silently.

  Stopping and resuming (ADR-004 #11). Jobs run with retained checkpoints, so:

      make flink-stop     # cancels, records each job's checkpoint under
                          # stream/flink/.resume/<pipeline.name> (gitignored)
      make flink-resume   # restarts both from exactly there

  `make flink-jobs` is REFUSED while a resume point exists -- a fresh submit
  reads from earliest-offset and would silently re-commit every row already in
  Iceberg. To reset deliberately:
  `python -m stream.flink.create_tables --recreate && ./stream/flink/stop.sh --forget`.
  Resume needs the job SQL unchanged (Flink derives operator IDs from the plan);
  editing a query means a reset.

- Hot path: stream/clickhouse. A ClickHouse Kafka engine table on traces.events.v1
  (AvroConfluent against the registry, group `clickhouse-hotpath`) feeding two
  materialized views: one into `agentlake.trace_events_rt` (ReplacingMergeTree, all 13
  contract fields, 7-day TTL) and one into `agentlake.trace_events_dlq` for anything
  that fails to parse. Grafana reads trace_events_rt; so do the MCP tools. Percentiles
  live here -- this is what ADR-004 #6 deferred downstream. Details: docs/adr/ADR-005.

  Cold start, in order:

      make hot-up         # compose --profile hotpath
      make ch-tables      # applies stream/clickhouse/sql/* in filename order
      make traffic        # 600 spans through the real SDK
      make ch-verify      # rows vs distinct span_ids vs topic offsets

  Then http://localhost:3000 -- both dashboards are already there, provisioned from
  dashboards/ (no clicking). `make ch-freshness` measures emit->queryable p95 (NFR-2,
  <=5s; measured 1569 ms) and `make ch-panels` times every dashboard query by reading
  it out of dashboards/json/ (NFR-5, <1s; measured worst 94 ms at 28K rows).

  Apply order in stream/clickhouse/sql/ is load-bearing, which is what the numeric
  prefixes encode: target tables, then the Kafka table, then the MVs -- creating an MV
  is what starts consumption, and `CREATE MATERIALIZED VIEW ... TO` does not create its
  target. Config for the container lives in stream/clickhouse/config.d and users.d;
  note that mounting those directories REPLACES the image's own config.d, which is why
  01-listen.xml has to exist (ADR-005 #9), and that XML comments cannot contain `--`.

  There is no resume guard here, unlike `make flink-jobs`: offsets live in the broker,
  so a restart resumes by itself and cannot replay into a populated table. A deliberate
  reset is `python -m stream.clickhouse.bootstrap --recreate` (destructive).

## Environment
- WSL2 Ubuntu, 8 GB laptop, WSL capped at 4 GB -> every compose service needs mem_limit
- Python 3.14 venv at .venv; run modules from repo root: python -m services.xyz
- Secrets only in .env (gitignored, see .env.example) -- ANTHROPIC_API_KEY required to run
  the gateway; never log it, never commit .env

## Tests & CI
- `python -m pytest -q` -- no Kafka, Flink or Docker needed (injectable emitters block the Kafka path in
  tests, see tests/conftest.py); `ruff check services/ tests/ stream/ scripts/` for lint (rule set pinned
  in pyproject.toml)
- CI (.github/workflows/ci.yml) on every PR: lint, test, and a schema-compat gate that
  only runs when contracts/ changed

## Conventions
- Trunk-based: feat/* branches, squash-merge PRs to main, conventional commits (feat:/fix:/docs:/test:/chore:)
- Never break Avro BACKWARD compatibility
- Prefer boring, explainable code over clever code; the owner must be able to whiteboard every design decision
- Type hints everywhere; tests must not require running Kafka (use injectable emitters)
