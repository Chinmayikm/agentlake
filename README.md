# agentlake

An open observability and evaluation platform for LLM agents, built on a streaming lakehouse.

## The problem

Agents fail quietly. A retrieval step returns the wrong chunk, a prompt change doubles
token spend, answer quality drifts over a week — and nothing crashes, no alert fires, no
error rate moves. By the time anyone notices, the traces that would explain it are gone.

Most observability treats telemetry as something an application collects and shows you
back through its own interface; the storage underneath is an implementation detail you
are not meant to query directly. agentlake takes a different position: the telemetry
*is* the product. One versioned Avro contract, enforced by a schema registry, on a Kafka
topic any consumer can subscribe to; landing in Iceberg tables any engine can query and
ClickHouse tables you own the DDL for. There is no application between you and your
data — dashboards, warehouse marts, evals, and the agent's own tools all read the same
contract. Two paths off it: a hot path that answers "what is happening right now" in
about a second, and a cold path that keeps every event exactly once, forever.

## Why agentlake

**Know what your agent actually did.** Every LLM call, tool invocation and retrieval
becomes a span in one distributed trace — across processes, with parent/child causality
intact. When someone asks "why did the agent say that?", `get_trace` returns the full
causal tree: what was retrieved, what the model was told, what it cost, where the time
went. Debugging stops being archaeology.

**Catch the failures that don't crash.** Agents degrade silently — answers get worse,
costs creep, a tool starts timing out — and nothing pages you. agentlake makes drift
visible: live dashboards for p95 latency, cost per session and tool error rates,
updating within about a second and a half of the event happening (measured p95 1.13s on
a fresh table and 1.57s at 27,867 rows, 50 probes each, none timed out).

**Own your telemetry.** Traces land in open formats on infrastructure you run — Avro
contracts in Kafka, Iceberg tables any Iceberg-capable engine can read, ClickHouse for
the hot path. No SaaS silo, no per-seat pricing, and your telemetry never leaves your
machines. The same events feed dashboards, warehouse marts and evals, because they are
just your data.

**Trust the pipeline itself.** The telemetry layer is built like the production system
it monitors: schema changes that would break consumers are rejected in CI against a real
Schema Registry; delivery to the archive is exactly-once, verified by killing the stream
processor mid-flight (1305 events, 1305 distinct span ids, 0 duplicates); and emit
failures inside the SDK are swallowed and logged rather than raised into the request
path of the app being observed.

**Ask your agent about itself.** The bundled MCP tools — `query_metrics`, `get_trace`
and `search_docs` — let the agent investigate its own telemetry. "What is my p95 LLM
latency this hour, and which model is most expensive?" is answered from live ClickHouse,
with the SQL it ran shown alongside the answer.

**Answer cost questions with numbers, not vibes.** Every call is metered against a
versioned price table — cost per turn, per session, per model. That table's version is
stamped into every LLM call's span, so a cost figure can always be traced back to the
prices that produced it. When finance asks what the agent costs, it is a dashboard panel
rather than an estimate. A traced agent turn here runs about $0.005.

### Where the money is

LLM spend is unusual among infrastructure costs: it is high-variance, per-request, and
driven by decisions nobody is watching — which model, how much context, how many
retries, how many steps. agentlake turns each of those into a measured, attributable
number.

- **Model routing is a 3x lever.** That is the actual spread between the two aliases in
  [models.yaml](services/gateway/models.yaml): 1.00/5.00 per MTok for
  `claude-haiku-4-5` against 3.00/15.00 for `claude-sonnet-5`, input and output alike.
  Widen the table with a frontier tier and the spread widens with it. Routing even a
  fraction of traffic down-tier is among the largest cost decisions an agent team makes
  — but only if quality and cost per model are visible side by side, which is what the
  `Cost by model` and `Tokens per minute by model` panels are for.
- **Token waste hides in the traces.** Oversized retrievals, prompts that grew after a
  "small" template change, an agent taking six steps where two suffice, retries
  multiplying spend on a flaky tool — none of these throw errors, and all of them show
  up in span data as tokens and steps per turn, attributable to a model and a tool. You
  cannot trim what you cannot see.
- **Regressions get caught before they compound.** A prompt change that doubles context
  length costs nothing in review and plenty at volume. `Cost per turn by prompt version`
  and `Tokens per turn by prompt version` are built and query
  `attributes['prompt_version']` — they return no rows today because nothing emits that
  attribute yet. Emit it from one `span.set()` and they populate with no dashboard
  change. The eval gate that would block quality regressions in CI is roadmap, not built.
- **The observability itself is the cheap part.** Self-hosted on open components, with
  no per-seat or per-trace pricing, and the marginal cost of a span is a local Kafka
  produce: measured at roughly 0.3 ms once the producer is warm, against a first emit of
  ~126 ms that `warmup()` exists to move out of the request path (ADR-000). A fully
  traced turn costs about $0.005 in model spend, and the tracing is a rounding error on
  the call it observes.

Instrumentation pays for itself the first time a dashboard shows you which model,
prompt, or tool your money is actually going to.

### Who this is for

- **Teams shipping agents to production** who need tracing, cost accounting and drift
  detection without adopting a SaaS — clone, `docker compose up`, instrument with the SDK.
- **Data and platform engineers** who want a reference implementation of a streaming
  lakehouse for AI telemetry — every design decision is an ADR, and every number links to
  the script that produced it.
- **Anyone evaluating the build-vs-buy line** for LLM observability: this is what owning
  the data layer actually involves, documented end to end.

## Components

| | |
|---|---|
| [contracts/trace_event_v1.avsc](contracts/trace_event_v1.avsc) | The Avro contract: 13 fields, keyed by session, BACKWARD compatibility gated in CI |
| [services/sdk](services/sdk) | `session()` and `span()`, `contextvars` propagation, lazy Kafka producer |
| [services/gateway](services/gateway) | The only door to a provider, and the only holder of the API key; versioned price table |
| [services/mcp_server](services/mcp_server) | `query_metrics`, `get_trace`, `search_docs` over stdio; whitelisted metrics, never free-form SQL |
| [services/agent](services/agent) | A bounded tool-use loop that is an ordinary MCP client, not a direct import |
| [services/rag](services/rag) | Hybrid retrieval — dense plus BM25 fused with RRF — over pinned Kafka, Flink and Iceberg docs |
| [stream/clickhouse](stream/clickhouse) | Hot path: Kafka engine into a 7-day table, plus a dead-letter table |
| [stream/flink](stream/flink) | Cold path: Flink SQL into Iceberg, hidden-partitioned by day |
| [dashboards/](dashboards) | Both Grafana dashboards and their datasource, provisioned from files |

## Architecture

```
  services/agent ──MCP (stdio)──> services/mcp_server ───> ClickHouse · Qdrant
        │                          search_docs · get_trace · query_metrics
        └──HTTP──> services/gateway ───> Anthropic API
                   the only holder of the API key; prices from models.yaml

  every process ── services/sdk ──Avro──> Kafka: traces.events.v1 (keyed by session_id)
                                              │   Schema Registry enforces BACKWARD
                    ┌─────────────────────────┴─────────────────────────┐
                 hot path                                            cold path
        ClickHouse Kafka engine                             Flink SQL, exactly-once
        agentlake.trace_events_rt (7-day TTL)          Iceberg on MinIO, REST catalog
        + trace_events_dlq                             lake.raw.trace_events
                    │                                  lake.curated.agg_model_5m
                 Grafana
```

Both paths consume the same topic independently. The hot path is for questions with an
answer in seconds and a seven-day memory; the cold path is the durable record. Run the
`streaming` profile or the `hotpath` profile, not both — they do not collide on ports,
but each is sized to fit beside Kafka on a small machine, not beside the other.

## Quickstart

Requires Python 3.14, Docker with Compose, and about 8 GB of free disk. An Anthropic API
key is needed only for the last step — everything through `make ch-verify` runs without
one.

```bash
git clone https://github.com/Chinmayikm/agentlake && cd agentlake

# first run: ~2.5 min, ~400 MB venv (onnxruntime and confluent-kafka are the bulk)
python3.14 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
cp .env.example .env          # add ANTHROPIC_API_KEY for the agent step below

make hot-up                   # kafka + schema-registry + clickhouse + grafana
                              # first run also pulls ~7 GB of images; on a cold volume
                              # Grafana installs its ClickHouse plugin before it reports
                              # healthy, so allow ~1 min after the command returns
make ch-tables                # applies stream/clickhouse/sql/* in filename order
make traffic                  # 600 spans through the real SDK
make ch-verify                # rows vs distinct span_ids vs Kafka offsets
```

`make ch-verify` should print a `MATCH` line and exit 0:

```
rows              601
distinct span_ids 601
duplicates        0
dead letters      0
topic offsets     601
MATCH             601 distinct spans == 601 messages on the topic
```

Then open <http://localhost:3000>. Both dashboards are already there — anonymous
read-only, `admin`/`admin` at `/login` to edit.

Two notes for a first run on an empty broker. The topic is created by the first
producer, which means one partition at the Kafka default; if you want the three
partitions [ADR-004](docs/adr/ADR-004-cold-path-flink-iceberg.md) reasons about, create
it explicitly first. And you may see one `Coordinator load in progress: retrying`
warning from the producer while a brand-new broker finishes electing itself — it
retries and succeeds.

### Ask the agent about its own p95

Start the gateway in a second shell, then ask it something only your telemetry can
answer:

```bash
make gateway

.venv/bin/python -m services.agent \
  "what is my p95 LLM latency by model in the last hour, and which model costs the most?"
```

It calls `query_metrics` twice, answers from ClickHouse, and prints what the turn cost:

```
[2 steps, tools: ['query_metrics', 'query_metrics'], 3743 tok, $0.0057,
 trace 5d57e887caa94320aeda1f823c25df30]
```

That trace id is real. Feed it to `get_trace` and the turn you just ran reads back as a
tree — seven spans, three levels, three processes, and a cost that agrees with the line
above:

```
AGENT_STEP  agent_turn         ok  8294.3ms
  GATEWAY     chat             ok  2917.2ms
    LLM_CALL  anthropic_messages ok 2088.4ms  claude-haiku-4-5  $0.002122
  TOOL_CALL   query_metrics    ok   835.2ms
  TOOL_CALL   query_metrics    ok   176.7ms
  GATEWAY     chat             ok  3466.7ms
    LLM_CALL  anthropic_messages ok 3466.4ms  claude-haiku-4-5  $0.003593

span_count 7, root_count 1, orphan_count 0, total_cost_usd 0.005715
```

The agent is an ordinary MCP client over stdio, so the same tools work from any MCP
host. Without the `rag` profile running, the tool server logs a Qdrant connection error
during warmup and `search_docs` is unavailable; the telemetry tools are unaffected.

### Optional: the cold path and the doc corpus

```bash
make flink-jars    # once, downloads 172 MB of pinned connector jars
make stream-up     # minio + iceberg-rest + flink (stop the hotpath profile first)
make flink-tables  # Iceberg tables, hidden-partitioned by day
make flink-jobs    # both SQL jobs; they run until cancelled

docker compose --profile rag up -d qdrant
.venv/bin/python -m services.rag ingest   # first run fetches ~470 MB of upstream docs
.venv/bin/python -m services.rag retrieve "how do watermarks handle idle partitions"
```

Ingest is resume-aware and downloads an embedding model on first use; on a small machine
run it with Kafka stopped.

## Measured, not claimed

| What | Measured | Reproduce |
|---|---|---|
| Emit to queryable, p95 | **1569 ms** at 27,867 rows; 50 probes, none timed out (target ≤ 5s) | `make ch-freshness` — [scripts/hot_path_verify.py](scripts/hot_path_verify.py) |
| Slowest dashboard panel | **94.2 ms** of 15 panels, 27,867 rows, 7-day window (target < 1s) | `python scripts/hot_path_verify.py panels --window-hours 168` |
| Exactly-once across a killed TaskManager | **1305 rows, 1305 distinct span ids, 0 duplicates** | `make flink-stop && make flink-verify` — [stream/flink/verify/01_raw_count.sql](stream/flink/verify/01_raw_count.sql) |
| Hot and cold path agree with the topic | **1908 events, 1908 rows on each path**, reconciled independently against Kafka offsets | `make ch-verify` and `make flink-verify` |
| Duplicates under five adversarial restarts, including a SIGKILL timed to a block flush | **0** | [ADR-005](docs/adr/ADR-005-hot-path-clickhouse-grafana.md) |
| Cost of one traced agent turn | **$0.0057**, 7 spans, 3743 tokens, on the cheap model alias | `get_trace` on the turn's own trace id |
| Whole hot path, resident | **953 MB** across four containers; host 2.3 of 3.9 GB | `docker stats` |
| Test suite | **284 tests**, no Kafka and no Docker required | `python -m pytest -q` |

These are hand-run local measurements on one 8 GB WSL2 machine, logged with their full
output in [ADR-004](docs/adr/ADR-004-cold-path-flink-iceberg.md) and
[ADR-005](docs/adr/ADR-005-hot-path-clickhouse-grafana.md). They are not CI-enforced
SLOs, and the freshness and panel figures are quoted at the largest table they were run
against — a fresh Quickstart run has fewer rows and comes out faster (1132 ms p95 on a
just-populated table).

## Design decisions

Every non-obvious choice is written down, with what it cost as well as what it bought.

| | |
|---|---|
| [ADR-000](docs/adr/ADR-000-telemetry-sdk-decisions.md) | Why Avro over JSON, why one trace per turn, and why the SDK never raises into your request path |
| [ADR-001](docs/adr/ADR-001-inference-gateway.md) | Why every LLM call goes through one door, and why the price table is a versioned file rather than constants |
| [ADR-002](docs/adr/ADR-002-rag-ingestion-pipeline.md) | Why hybrid retrieval beats dense-only here, with the queries where each one loses |
| [ADR-003](docs/adr/ADR-003-agent-mcp-design.md) | Why the agent speaks real MCP over stdio instead of importing `retrieve()`, and why the loop is bounded at 8 steps |
| [ADR-004](docs/adr/ADR-004-cold-path-flink-iceberg.md) | Why there is no p95 in Flink, why Iceberg tables are created outside the SQL, and how resume avoids re-committing |
| [ADR-005](docs/adr/ADR-005-hot-path-clickhouse-grafana.md) | Why the ClickHouse Kafka engine instead of Kafka Connect, and why every count uses `uniqExact(span_id)` |

## Status and roadmap

Built and verified: the telemetry SDK, the inference gateway, the RAG corpus and hybrid
retrieval, the MCP server and the bounded agent loop, the Flink to Iceberg cold path,
the ClickHouse and Grafana hot path, and CI (lint, tests, and a schema-compatibility
gate that runs when `contracts/` changes).

Next:

- **dbt marts on Trino** over the Iceberg tables — curated models with tests, replacing
  the single hand-written 5-minute aggregate.
- **Debezium CDC** so application state lands on the same topic contract as telemetry.
- **An eval harness with quality gates in CI** — the two panels in
  `dashboards/json/quality.json` are wired to the store and deliberately marked "not
  measured yet"; that is the gap this closes.
- **Terraform** for the deployed footprint.

The empty `dbt/`, `eval/` and `infra/` directories are placeholders for exactly these.

## Running it on a small machine

The whole thing is developed on an 8 GB laptop with WSL capped at 4 GB. Every service
carries an explicit `mem_limit`, and `docker-compose.yml` is sliced into profiles so
only one heavy piece runs at a time: the Kafka spine alone, plus `rag`, `streaming` or
`hotpath`. The measured footprints are in ADR-004 and ADR-005, alongside the JVM sizing
that a 4 GB ceiling forces on Flink.

## License

MIT — see [LICENSE](LICENSE).
