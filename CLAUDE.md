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
- Analytics: Trino over the SAME Iceberg REST catalog Flink writes to -> dbt marts in
  lake.analytics, a Great Expectations gate, OpenLineage lineage in Marquez.
  See analytics/ + dbt/ + quality/ + ADR-006.
- CDC: Postgres metadata DB -> Debezium (pgoutput) -> cdc.metadata.* topics ->
  lake.cdc.prompt_versions -> dbt dimension + the fct_cost_by_prompt join.
  See metadata/ + scripts/cdc_land.py + ADR-007.
- Planned: eval harness

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
- `docker compose --profile tools up -d kafka-ui` -- + kafka-ui (provectuslabs
  v0.7.2, Apache-2.0). On demand only, never part of a normal bring-up. Port
  **kafka-ui 8090** (container 8080 is remapped; 8080 stays free for anything else).
  Reads the topic through the registry, so TraceEvents render as decoded Avro fields
  rather than bytes. Read-only and no dynamic config -- it is a window onto the topic,
  not a way to produce to it. Measured 201 MB of a 320 MB limit, healthy ~30s after
  start; the JVM heap is pinned in JAVA_OPTS because a Spring Boot app left to size
  itself off a 320 MB cgroup has no usable heap (same argument as ADR-004 #8).

- `make analytics-up` -- + trino (and minio + iceberg-rest, which the `streaming`
  profile shares). Measured: 853 MB across all three containers, host 2.0 of 3.9 GB --
  the cheapest of the three slices. Port **Trino 8085** (container 8080 is remapped;
  8080 stays free, same decision as kafka-ui). See ADR-006.
- `make lineage-up` -- + marquez, marquez-web, marquez-db (the OpenLineage backend).
  A further 245 MB, and only `make lineage` needs it, which is why it is its own
  profile. Ports **Marquez API 5000**, **Marquez UI 3001** (grafana owns 3000).
- `make cdc-up` (`docker compose --profile cdc up -d`) -- + metadata-db,
  metadata-init (one-shot), connect. Measured: connect 478 MB of a 640 MB limit,
  metadata-db 57 MB of 256 MB; 1161 MB across all four containers with the spine,
  host 2.1 of 3.9 GB. Ports: **Postgres 5433** (5432 left free for a local one),
  **Kafka Connect 8083**. See ADR-007.

  Unlike `make analytics-up` this one does NOT name its services, and that is the
  right call rather than an oversight: the cdc slice genuinely needs kafka and
  schema-registry, so a bare profile bring-up starting them too is what you want.

  **Capture and landing are two phases, and the split is measured, not stylistic.**
  Capture (metadata-db + connect) is ~535 MB and landing needs the analytics
  slice's 853; both at once plus kafka leaves too little for dbt's 512 MB toolbox.
  Kafka keeps the topic in between, so stopping the capture side loses nothing.

Run `streaming` OR `hotpath` OR `analytics`, not two at once -- each is sized to fit
beside the spine, not beside another. They do not fight over host ports, so it is a
memory decision. `tools` and `lineage` are small enough to sit beside one, but both
are opt-in. `cdc` is a fourth slice: it fits beside any ONE of the three, and it
never needs `streaming` at all.

**The analytics slice does not need kafka or schema-registry at all** -- it reads
Iceberg through the REST catalog and never touches the topic. That is unique to it,
and it is worth 745 MB, so `make analytics-up` NAMES its service
(`docker compose --profile analytics up -d trino`) rather than bringing the profile up
bare: kafka and schema-registry carry no `profiles:` key, so a bare `up` starts them
under every profile. Stop them first:

    make flink-stop            # only if the Flink jobs are running
    docker compose stop flink-jobmanager flink-taskmanager kafka schema-registry

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

- Analytics: analytics/ + dbt/ + quality/. Trino 483 over the SAME Iceberg REST catalog
  the Flink jobs write through -- one catalog, two engines, so `lake.raw.trace_events`
  is the same fully-qualified name in both. dbt builds three staging models and four
  marts into `lake.analytics`; Great Expectations gates them; dbt-ol emits lineage.
  Details: docs/adr/ADR-006.

  **dbt and Great Expectations run in a container, and that is forced, not a style
  choice**: this repo is Python 3.14, dbt-core has no 3.14 support yet
  (dbt-labs/dbt-core#12098) and great-expectations 1.22.0 declares `<3.14`. So
  `analytics/Dockerfile` pins a python:3.12 toolbox and every invocation is a
  `docker compose run --rm dbt ...` -- the same one-shot pattern as flink-sql-client.
  Nothing in .venv changes.

  Cold start, in order (stop the cold path first -- Trino takes its place):

      make analytics-build   # once: builds the dbt/GE/dbt-ol image
      make analytics-up      # trino (+ minio, iceberg-rest), then warms the catalog
      make dbt-build         # dbt deps && dbt build -- 7 models, 71 tests
      make quality           # the GE gate; exits non-zero on a BLOCKING failure
      make analytics-verify  # marts vs staging vs raw, with the numbers

  Marts: `fct_sessions` (per session), `fct_model_costs` (model x day, with the EXACT
  p95 that ADR-004 #6 deferred downstream), `fct_tool_reliability` (tool x day), and
  `fct_cost_by_prompt` (prompt version x day -- the CDC join, ADR-007).
  `make analytics-session` hand-checks one row against raw, and
  `make analytics-crosscheck` compares Trino's exact p95 against ClickHouse's
  approximate `quantile(0.95)` (measured worst divergence 1.47%).

  Three things that will bite otherwise, all in ADR-006 #8:

  - `make analytics-up` ends with a warm-up query on purpose. Trino's first query
    against Iceberg costs ~25s (plugin + S3 client + catalog) against 0.4s warm, and
    dbt-trino exposes no request-timeout setting -- an unwarmed build intermittently
    fails its first model on the client's fixed 30s.
  - The catalog is SQLite and permits ONE writer. `CATALOG_CLIENTS: "1"` in
    docker-compose.yml is what stops dbt racing it into `SQLITE_BUSY`; do not remove it.
  - Staging models are tables, not views, because Trino cannot `CREATE VIEW` against an
    Iceberg REST catalog -- and `on_table_exists` must stay `drop`, because
    `CREATE OR REPLACE TABLE` fails deterministically on this catalog.

  Lineage is a separate excursion, because Marquez is 245 MB nothing else needs:

      make lineage-up && make lineage    # then http://localhost:3001
      make lineage-down

  `make lineage` does two things: `dbt-ol` captures the dbt run, and
  `scripts/emit_flink_lineage.py` DECLARES the Kafka -> Iceberg edge in front of it,
  parsed out of `stream/flink/jobs/*.sql`. dbt-ol is a post-processor over
  target/manifest.json, so it can only ever see dbt nodes -- that edge is declared, not
  captured, and ADR-006 #7 says so.

  `scripts/seed_iceberg.py` writes synthetic spans straight into Iceberg through Trino,
  for a warehouse with no Kafka or Flink in front of it (which is what CI has). It
  REFUSES a populated table without `--force`. `inject-bad-row` / `revert-bad-row` are
  the fault injector the quality gate is demonstrated against.

- CDC: metadata/ + scripts/cdc_land.py + scripts/register_connector.py. A Postgres 16
  metadata database (prompt_versions, golden_examples, eval_runs, eval_results) with
  `wal_level=logical`; Debezium 3.0.8.Final on Kafka Connect streams its WAL through
  the **pgoutput** plugin onto `cdc.metadata.<table>`. Day 5's eval harness writes
  those tables -- this slice builds the tables and the pipeline, not the harness.
  Details: docs/adr/ADR-007.

  **The landing path is a batch pull, not a Flink job, and ADR-007 #3 argues it.**
  A Debezium changelog is an *updating* stream and ADR-004's Iceberg sink is
  append-only, so Flink would mean upsert mode with v2 equality deletes -- a
  mechanism nothing else here uses -- to maintain a table of tens of rows. And
  `streaming` (1790 MB) + `cdc` (535) + the `analytics` slice dbt needs (853) does
  not fit in 3.9 GB. `scripts/cdc_land.py` is a confluent-kafka consumer feeding
  `INSERT`s through the existing TrinoClient; offsets live in the consumer group
  `agentlake-cdc-land`, so it resumes.

  Cold start, in order. PHASE 1 -- capture:

      make cdc-up          # metadata-db -> metadata-init (one-shot) -> connect
      make cdc-connector   # idempotent PUT; snapshot.mode=initial fires here
      make cdc-topic       # what Debezium actually emitted, record by record
      make cdc-psql        # ...make changes...
      docker compose stop connect metadata-db

  PHASE 2 -- land and model (stop the cold path first; Trino takes its place):

      make analytics-up
      make cdc-table       # once: creates lake.cdc.prompt_versions
      make cdc-land        # drain the topic into Iceberg
      make dbt-build && make quality && make analytics-verify

  Migrations are `metadata/sql/*.sql` applied by the `metadata-init` one-shot in
  **filename order**, which is load-bearing the same way `stream/clickhouse/sql/`'s
  prefixes are: the role before the grants, the tables before the publication that
  names them, the seed last so the initial snapshot has something to carry. Every
  file is safe to re-apply, because metadata-init runs on every bring-up.

  Four things that will bite otherwise, all in ADR-007:

  - **`lake.cdc`, not `lake.raw`.** ADR-006 #1's ownership table says lake.raw is
    written by Flink. A third namespace keeps that sentence true.
  - **`REPLICA IDENTITY FULL` on prompt_versions** (metadata/sql/02). Without it a
    DELETE's `before` image is the primary key and nothing else, so a deleted row
    lands with no `version` and stops joining -- indistinguishable from a lander
    that never ran.
  - **`restart_lsn`, not `confirmed_flush_lsn`, is what pins WAL.** `make cdc-slot`
    prints both. Measured across a 44s connector outage: retained WAL grew
    273 kB -> 1493 kB and did not fall when the connector reconnected;
    confirmed_flush_lsn caught up in seconds while restart_lsn took ~2m20s more.
    Monitoring the wrong one reports "caught up" with megabytes still retained.
  - **The CDC topics are NOT in the Schema Registry**, unlike traces.events.v1: the
    Debezium image does not ship Confluent's Avro converter (Confluent Community
    License), so they carry JSON with `schemas.enable=false`. ADR-007 #2.

  `scripts/cdc_land.py seed` writes synthetic changelog rows for a warehouse with
  no Kafka in front of it (which is what CI has), refusing a populated table
  without `--force` -- the same fence `scripts/seed_iceberg.py` has.

## Environment
- WSL2 Ubuntu, 8 GB laptop, WSL capped at 4 GB -> every compose service needs mem_limit
- Python 3.14 venv at .venv; run modules from repo root: python -m services.xyz
- Secrets only in .env (gitignored, see .env.example) -- ANTHROPIC_API_KEY required to run
  the gateway; never log it, never commit .env

## Tests & CI
- `python -m pytest -q` -- no Kafka, Flink or Docker needed (injectable emitters block the Kafka path in
  tests, see tests/conftest.py); `ruff check services/ tests/ stream/ scripts/ analytics/ quality/ metadata/` for
  lint (rule set pinned in pyproject.toml)
- CI (.github/workflows/ci.yml) on every PR: lint, test, a schema-compat gate that only
  runs when contracts/ changed, and a `quality` gate that only runs when the analytics
  layer's inputs changed. The quality job runs the analytics slice FOR REAL on the
  runner -- trino + iceberg-rest + minio, tables from `stream.flink.create_tables`,
  rows from `scripts/seed_iceberg.py` and `scripts/cdc_land.py seed`, then `dbt build`
  and the GE checkpoint. It fits because Kafka, Flink and Marquez are excluded;
  ADR-006 #10 records what that leaves uncovered, and ADR-007 #10 adds that CI runs
  neither Postgres nor Debezium -- capture is covered by ADR-007's verification log,
  not by CI.

## Conventions
- Trunk-based: feat/* branches, squash-merge PRs to main, conventional commits (feat:/fix:/docs:/test:/chore:)
- Never break Avro BACKWARD compatibility
- Prefer boring, explainable code over clever code; the owner must be able to whiteboard every design decision
- Type hints everywhere; tests must not require running Kafka (use injectable emitters)
