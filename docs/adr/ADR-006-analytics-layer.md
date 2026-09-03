# ADR-006: Analytics layer — Trino, dbt, Great Expectations, OpenLineage

- **Status:** Accepted
- **Date:** 2026-09-02
- **Context:** `analytics/`, `dbt/`, `quality/`, the `analytics` and `lineage` compose
  profiles, and the three marts in `lake.analytics`.

ADR-004 landed every TraceEvent in Iceberg and ADR-005 made "what is happening now"
answerable in ClickHouse. Neither gave the lake a **batch** engine, a modelled shape,
or any assertion that the numbers are right. This slice adds Trino over the *same*
Iceberg catalog Flink writes to, five dbt models, a Great Expectations gate, and
OpenLineage lineage into Marquez.

It also closes ADR-004 §6 for the second and last time. That gap — Flink 1.20 has no
percentile aggregate — was deferred "downstream from `lake.raw.trace_events`, one join
away". ADR-005 §4 discharged the *hot* half with ClickHouse's sampling `quantile()`.
This is the batch half, and it is **exact**.

---

## 1. One catalog, two engines — and per-table ownership

`analytics/trino/catalog/lake.properties` points at `http://iceberg-rest:8181` with
warehouse `s3://lake/` — byte for byte the `CREATE CATALOG` in
`stream/flink/jobs/*.sql`. The file is named `lake.properties` so Trino's catalog is
also called `lake`, which makes **`lake.raw.trace_events` the same fully-qualified name
in both engines**.

**Rejected: a second catalog over the same bucket.** It is the easy setup — point Trino
at MinIO with its own metastore — and it quietly forks the warehouse. Two catalogs
disagreeing about a table's current snapshot is not an error either of them can raise.
The whole argument for a REST catalog (ADR-004 §10) is that it is the shared
coordination point; running a second one throws that away and keeps the ceremony.

The claim is asserted rather than documented:
`tests/test_analytics_project.py::test_trino_catalog_points_at_the_same_rest_catalog_as_flink`
parses both files and compares the URI, the warehouse and the S3 endpoint. If they
drift, nothing breaks loudly — both engines keep working, against different data — so a
test is the only thing that would notice.

**"Only Flink writes to Iceberg" survives, as a per-table rule.** ADR-004 §2 established
that every data file in the warehouse is written by a Flink task. dbt now writes too —
but only into `lake.analytics`, a namespace Flink never touches, and no dbt model writes
to `lake.raw` or `lake.curated`. The invariant that matters is that a table has exactly
one writer, and it holds:

| namespace | written by | read by |
|---|---|---|
| `lake.raw` | Flink (`01_raw_sink.sql`) | Flink verify, Trino, dbt |
| `lake.curated` | Flink (`02_agg_model_5m.sql`) | Trino, dbt |
| `lake.analytics` | dbt (via Trino) | Trino, Great Expectations, Marquez |

The two deliberate exceptions are `scripts/seed_iceberg.py` (§9) and
`stream/flink/create_tables.py`, which writes table *definitions*, not data — ADR-004 §2
already drew that line.

**Port 8085, not 8080.** Trino's internal port is 8080 and every Trino document assumes
it, but `docker-compose.yml` records a deliberate decision to leave host 8080 free —
it is why `kafka-ui` is remapped to 8090. Publishing on 8085 keeps that decision intact
and costs one line of documentation.

---

## 2. dbt and Great Expectations run in a container, because they cannot run here

This is the constraint that shaped the slice, and it is a fact rather than a preference.

| | |
|---|---|
| agentlake | Python **3.14** (CLAUDE.md; the box has only `/usr/bin/python3.14`) |
| dbt-core | **no 3.14 support** — mashumaro pin; dbt-labs/dbt-core#12098 targets 1.12 |
| great-expectations 1.22.0 | `Requires-Python >=3.10,<3.14`, declared outright |

So `analytics/Dockerfile` builds a pinned `python:3.12-slim` toolbox and every dbt, GE
and dbt-ol invocation is a `docker compose run --rm dbt …`. This is exactly the
`flink-sql-client` pattern already in the repo: a one-shot client in its own profile,
never part of a bring-up.

Resolved versions, recorded because `analytics/requirements.txt` pins only the four
direct dependencies:

| | |
|---|---|
| dbt-core / dbt-adapters / dbt-trino | 1.12.3 / 1.24.5 / **1.10.2** |
| great-expectations | 1.22.0 |
| openlineage-dbt (+ python, sql, integration-common) | 1.53.0 |
| trino client / SQLAlchemy | 0.336.0 / 2.0.52 |
| dbt_utils | 1.3.0 |

**Rejected: a second venv on Python 3.12.** It is the faster inner loop and it needs
`python3.12` installed on the host, which is a machine mutation CI would then have to
reproduce a second way. The image is what CI runs, so local and CI cannot drift.

**Consequence, stated plainly:** the analytics toolchain is pinned to a Python this repo
does not otherwise use, and it will stay that way until dbt-core 1.12+ supports 3.14.
When it does, the container can collapse into `requirements-dev.txt` and nothing above
it changes.

---

## 3. Staging models are tables, because Trino cannot make them views

dbt convention is staging-as-view: cheap, always fresh, no storage. It is not available
here.

> The REST catalog does not support materialized view management. … JDBC catalog does
> not support views or materialized views.

Trino's Iceberg connector cannot `CREATE VIEW` against a REST catalog, and
`apache/iceberg-rest-fixture` *is* a JdbcCatalog (ADR-004 §10), so it fails on both
counts. `stg_trace_events` and `stg_agg_model_5m` are therefore real Iceberg tables.

**This is not purely a cost.** Staging-as-tables is what makes them datasets in the
OpenLineage graph rather than nodes that vanish into their consumers — the "staging"
layer of §7's four-layer picture exists because of this limitation. The storage is
2,829 rows.

**Rejected: `ephemeral`.** dbt's other answer, and it needs no view support: the model
is inlined as a CTE. It also disappears from the warehouse and, more importantly, from
lineage — which is half of what this slice is for.

---

## 4. The marts, and where the exact percentile lives

Three facts, at the grain a question would be asked at:

| mart | grain | notable columns |
|---|---|---|
| `fct_sessions` | one row per `session_id` | `turn_count`, `span_count`, `tool_call_count`, `llm_call_count`, `error_count`, tokens, `total_cost_usd`, `duration_ms` |
| `fct_model_costs` | (`event_day`, `model`) | `calls`, tokens in/out, `cost_usd`, latency avg/max, **exact `latency_p95_ms`** |
| `fct_tool_reliability` | (`event_day`, `tool_name`) | `calls`, `error_count`, `timeout_count`, `error_rate`, `timeout_rate`, latency avg/max/p95 |

**`turn_count` is a definition, not an estimate.** ADR-000 fixes the scope — a session is
one conversation, a trace is one turn's causal graph — so `count(distinct trace_id)`
within a session *is* the number of turns, and `span_count >= turn_count` is an
invariant (every turn has at least its root span) rather than a heuristic. It is a
blocking test in both gates.

**The percentile is exact, and that is the point.**
`dbt/macros/exact_percentile.sql` computes nearest rank over `array_sort(array_agg(…))`
— every recorded `latency_ms`, no sampling.

**Rejected: `approx_percentile`.** Trino has one, it is cheaper, and at scale it is the
right call. It is wrong *here* because exactness is the entire reason a mart reads from
`lake.raw.trace_events` instead of from the 5-minute aggregate. Using an approximation
would leave the repo with two approximate p95s and nothing to check either against —
and §11's cross-engine measurement would have had no reference side.

**Where that stops being true, stated rather than left to be discovered.** `array_agg`
materialises every value of a group in the coordinator's heap. At this grain — one model
per day, order 10K doubles, ~80 KB — it is nothing against a 614 MB heap. At a hundred
million values per group it is an OOM, and the answer there is `approx_percentile` with
the loss of exactness written down.

**Two smaller decisions worth recording.** `event_day`, not `day`: `DAY` is a keyword in
Trino's interval syntax and an unquoted column called `day` is a trap for the next
query. And `timeout` matches `status = 'timeout' OR error_class = 'TimeoutError'` —
matching only the first would report a timeout rate of zero against traffic that is
entirely timeouts, because the SDK's `except` block forces `status='error'` and records
the class separately.

**`stg_agg_model_5m` feeds no mart, deliberately.** Every mart reads raw, because
percentiles need individual values. The 5-minute table exists here to be *reconciled*:
`assert_streamed_agg_does_not_overcount.sql` recomputes each window from raw and asserts
the streamed count never exceeds it. One-directional on purpose — ADR-004 §3 (an idle
watermark leaves the last window open) and §5 (late events are dropped from the
aggregate but still reach raw) both make `streamed < recomputed` normal, while
`streamed > recomputed` is only reachable if the append-only guarantee broke. That is
`stream/flink/verify/04_agg_vs_raw.sql`'s check — a hand-run Flink batch job needing a
free task slot — turned into something that runs on every build with Flink stopped.

---

## 5. Percentiles: the gap is now closed on both sides

Worth stating in one place, because it took three ADRs.

| | percentile | why |
|---|---|---|
| ADR-004 `lake.curated.agg_model_5m` | **none** | Flink 1.20 SQL has no percentile aggregate; `PERCENTILE` arrived in Flink 2.0 |
| ADR-005 `agentlake.trace_events_rt` | `quantile(0.95)`, **approximate** | ClickHouse samples; 16.5 ms over the full table; answers "right now" |
| ADR-006 `lake.analytics.fct_model_costs` | nearest rank, **exact** | the lake keeps every `latency_ms`; this is the "one join away" ADR-004 promised |

The cold path never changed. ADR-004 §1 rejected Flink 2.1 because it would desync the
RAG corpus and reopen every compatibility question the version matrix had closed; the
answer both times was to put the question in a store that is better at it.

§11 measures the two live answers against each other.

---

## 6. The Great Expectations / dbt split, and blocking vs warn

**Two orthogonal splits, both stated as one-line rules.**

**Which tool.** GE owns single-table contracts on the finished marts — row counts, null
rates, value bounds, uniqueness, freshness, within-row column comparisons. dbt owns
everything relational — `relationships` between models, a mart reconciled against its
staging source, the streaming-vs-batch aggregate comparison.

That is forced as much as chosen, and the forcing is worth knowing: **GE's Trino backend
supports table assets only** — there is no query asset — so a GE expectation cannot
express a join, while a dbt test is an ordinary `SELECT` that can. Each tool got the
half it can actually do. `fct_sessions`' `span_count >= turn_count` is a GE
`ExpectColumnPairValuesAToBeGreaterThanB` precisely because it is within a row;
`fct_sessions.session_id -> stg_trace_events.session_id` is a dbt `relationships` test
precisely because it is not.

**Which severity.**

> **BLOCKING** = an invariant that can only be false if the pipeline is broken.
> **WARN** = a property that depends on when traffic last ran, or on data shape that is
> legitimately variable.

So row counts, null keys, value bounds, uniqueness and `span_count >= turn_count` all
block. Three things deliberately do not:

- **Freshness.** This is a laptop lakehouse whose source table only advances when
  someone runs `make traffic` with the Flink jobs up. A stale mart is the normal state
  on a Tuesday, and gating on it trains everyone to ignore the gate.
- **`accepted_values` on `status`.** `status` is a free-form contract string and the SDK
  promotes whatever `set(status=…)` is handed it — its own tests already cover
  `"degraded"` (ADR-005 §2). A new status value is news, not a build failure; blocking
  here would make adding one a schema migration.
- **`not_null` on `model`.** NULL for every non-LLM span by contract. `fct_model_costs`
  filters to `LLM_CALL` so it is populated in practice, but a `NOT NULL` here would be
  this layer asserting something the contract does not.

**Counted, not claimed:** 52 blocking dbt tests + 22 blocking GE expectations = **74
blocking checks**, against a target of 15.
`test_blocking_test_count_meets_the_bar` fails if that erodes, and
`test_checkpoint_marks_every_expectation_with_a_severity` fails if an expectation is
added with no severity — the default is blocking, which is safe, but an unmarked one is
more likely an oversight than a decision.

---

## 7. Lineage: what is captured, and what is declared

`make lineage` produces a four-layer graph. **The layers do not all come from the same
place, and the graph does not say so, so this does.**

**Captured.** `dbt-ol` runs the build, then reads `target/manifest.json` and
`target/run_results.json` and emits OpenLineage events describing a run that actually
happened — 30 of them. It is a *post-processor*, not a plugin, which is exactly why it
can only ever see dbt nodes. Its graph starts at `lake.raw.trace_events`.

**Declared.** `scripts/emit_flink_lineage.py` POSTs two events describing the Flink jobs
— `traces.events.v1 -> lake.raw.trace_events` and `-> lake.curated.agg_model_5m`. No
Flink job was watched. Two things keep that honest rather than decorative:

1. Every name is **parsed out of `stream/flink/jobs/*.sql`** — topic, brokers, target
   table and `pipeline.name` all come from the file Flink executes, so renaming the
   topic renames it here. `test_flink_lineage_is_parsed_from_the_job_sql` and
   `test_lineage_topic_matches_the_sdk` assert the parse still works and still agrees
   with `services.sdk.TOPIC`.
2. The events carry no run duration and a documentation facet that says they are
   declared, so nothing reads a wall-clock number the script did not measure.

**Rejected: instrumenting Flink for real** with the `openlineage-flink` jar. It is the
honest answer and it is expensive here: it means changing a job's configuration, and a
change to a job's SQL is a **state reset** — Flink derives operator IDs from the plan, so
an edited query cannot resume from its retained checkpoint (ADR-004 §11). Paying a full
warehouse rebuild for one graph edge is the wrong trade at this size. Recorded as the
gap.

**The namespace has to match, and this is the whole trick.** dbt-ol names Trino datasets
`trino://trino:8080` + `lake.raw.trace_events`, derived from the adapter's host and port
— the container-internal address, not the host-side 8085. The declared events use the
identical namespace and name, which is what makes the two halves join into one graph
instead of becoming two nodes with the same label. It is a constant in
`emit_flink_lineage.py` with that explanation attached.

---

## 8. Four things that had to be discovered by running it

Each cost real time and none is visible in any single doc page.

**`SQLITE_BUSY`, and the fix is `clients=1`.** The first full `dbt build` failed with
`ICEBERG_CATALOG_ERROR: Failed to drop table`, then `ICEBERG_COMMIT_ERROR: … 500:
Unknown failure`, intermittently, then permanently. The root cause is four layers down
in the REST fixture's log: `org.sqlite.SQLiteException: [SQLITE_BUSY] The database file
is locked`. The catalog is a JdbcCatalog on **SQLite**, which permits exactly one
writer, and Iceberg's `JdbcClientPool` defaults to **two** connections
(`CatalogProperties.CLIENT_POOL_SIZE`) — two writers racing one file lock. With Flink
alone this never appeared, because Flink writes table metadata about once per 30s
checkpoint; dbt creates and replaces five tables back to back.

`CATALOG_CLIENTS: "1"` serialises the pool and removes the race by construction. Three
consecutive drop+create rounds pass where every one had failed.
`?busy_timeout=30000` is on the JDBC URL too and is the belt to that braces — it makes a
contended lock wait rather than fail — but it was *not* what fixed this, and saying so
matters: the failures were fast, and a busy timeout that was working would have made
them slow. `test_iceberg_catalog_uses_a_single_jdbc_connection` pins it.

**`CREATE OR REPLACE TABLE` does not work on this catalog, deterministically.** It is
the tidiest materialization strategy — one atomic catalog write, no window where the
table is missing — and `on_table_exists: replace` appeared to work exactly once. That
run was against tables that did not yet exist, i.e. where REPLACE degenerates to CREATE.
Against existing tables it fails every time: a replace-table commit is an `UPDATE`
against the catalog row that the JdbcCatalog does not carry out. `drop` is what is left
(dbt-trino's default `rename` costs three catalog writes per model instead of two), and
it is sufficient once the catalog stops racing itself.

**Trino's first Iceberg query costs 25–28 s; the warm one costs 0.4 s.** Measured twice,
consistently: `SELECT count(*)` over 1,908 rows took 25.6 s cold and 0.41–0.61 s on
repeat, with idle CPU at 5% — so this is plugin initialisation, the S3 client build and
a catalog round trip, not thrash and not small files (the table is 3 data files, 3
snapshots, 103 KB). This would be a footnote except that **dbt-trino exposes no
request-timeout setting** — `trino.dbapi.connect` is called without one, so the client's
fixed 30 s applies — and an unwarmed `dbt build` intermittently fails its first model
with a read timeout that reads like a configuration error and hides the real one.
`make analytics-up` therefore ends with an explicit warm-up query. Same finding and same
fix as ADR-000 §3, ADR-003 §6 and ADR-005 §5, for the fourth time: pay initialisation
deliberately rather than charging it to the first real operation.

**The Trino image is cgroup-aware, and its default still overshoots.** Unlike Flink
(ADR-004 §8) and kafka-ui, `trinodb/trino:483` sizes its heap from the container limit
via `-XX:MaxRAMPercentage=80` — no `-Xmx` needed. The trap is the rest of the JVM: 80% of
a 1024m `mem_limit` is an 819m heap, and `ReservedCodeCacheSize` is 256m, which is
1075m of the fixed regions alone before metaspace or a thread stack. The code cache is
reserved virtual address space committed lazily, so this does not fail at startup — it
fails later, under load, as an OOM kill that looks like a query problem. 60% plus a 128m
code cache leaves ~614m of heap inside 1024m. `config.properties` must then fit inside
*that*: 256MB `query.max-memory-per-node` + 192MB `memory.heap-headroom-per-node` = 448
of 614. Trino validates this at startup and refuses to start, which is a kinder failure
than Flink's; `test_trino_query_memory_fits_inside_the_heap` recomputes the arithmetic
so editing one file and not the other fails in CI.

**Three smaller ones, recorded so nobody spends the afternoon again.**
`marquez-web` exits with `WEB_PORT environment variable is not defined` rather than
defaulting to the port it already listens on. dbt 1.12 requires generic-test arguments
under an `arguments:` key (the flat form still runs, emitting
`MissingArgumentsPropertyInGenericTestDeprecation` 24 times) — and `config:` must stay a
*sibling* of `arguments:`, because nesting `severity` under arguments makes it silently
inert. And GE's progress bars ignore `TQDM_DISABLE`; they are built from GE's own config
and only `ProgressBarsConfig(globally=False, metric_calculations=False)` turns them off,
which matters because redirected to a file they bury the result table in a few thousand
characters of carriage returns.

---

## 9. `scripts/seed_iceberg.py`, and the exception it is

CI has no Kafka and no Flink (§10), so it needs another way to get rows into
`lake.raw.trace_events`. The seeder writes the same 13-column shapes `gen_traffic.py`
emits — ok turns of AGENT_STEP + RETRIEVAL + LLM_CALL, error turns of AGENT_STEP +
TOOL_CALL carrying `error_class=TimeoutError` — straight through Trino.

It is a deliberate exception to §1's ownership rule, and it is fenced:

- **It refuses a table that already has rows** unless `--force`. Synthetic rows mixed
  into Flink-written rows cannot be told apart afterwards, and every later count becomes
  ambiguous. Same instinct as `submit.sh`'s resume guard (ADR-004 §11): make the safe
  path the default and the destructive one explicit.
- In CI the warehouse is created empty by `stream.flink.create_tables` and destroyed
  with the runner, so there is no writer to conflict with.
- `--table` exists so the seeder can be rehearsed against a scratch table without
  touching a real warehouse — which is how its SQL generation was verified (§11).

`inject-bad-row` / `revert-bad-row` are the fault injector: one span with
`cost_usd = -1` under a recognisable-on-sight `span_id` (`…0bad`, not a uuid4). **A gate
nobody has watched fail is a gate nobody should trust**, and §11 records both directions.

---

## 10. CI posture: the full slice, and the honest reason it fits

**Chosen: run the analytics layer for real on the runner.** The `quality` job starts
`trino` (plus `iceberg-rest` and `minio` by dependency), creates the tables with the
same `stream.flink.create_tables` the cold path uses, seeds 600 deterministic spans,
then runs `dbt deps && dbt build` and the Great Expectations checkpoint — 5 models, 54
dbt tests and 24 expectations, executed rather than compiled.

**It fits *because* Kafka, Flink and Marquez are excluded, and that exclusion is the
whole decision.** The alternative posture — `dbt parse` plus file-level tests in CI, with
the real gate as a pre-merge `make` target — was the fallback this slice was prepared to
take. It became unnecessary once `seed_iceberg.py` removed the need for a broker: the
expensive, flaky part of an end-to-end analytics job is standing up Kafka, fetching 172
MB of Flink jars and waiting on a 30 s checkpoint, and none of that tests the models,
the tests or the expectations. ADR-004 already verifies the cold path.

**Path-filtered** to `dbt/`, `quality/`, `analytics/`, `stream/`, `contracts/`,
`scripts/`, `docker-compose.yml` and the workflow itself, using the same base-ref logic
`contract-compat` already uses.

**What CI does not cover, stated rather than implied:**

- **Flink-written data.** The seeder is the writer in CI. The schema is identical
  (`create_tables.py` creates it) but the *producer* is not exercised — ADR-004's
  verification log is what covers that.
- **Lineage emission.** Marquez is another ~245 MiB for a graph no assertion reads.
  `make lineage` is a local step, and §11 records its output.
- **The cross-engine percentile check**, which needs ClickHouse as well.

The file-level tests that *do* run on every PR — `tests/test_analytics_project.py`, 19 of
them, no Docker — cover the things that fail silently: the contract reaching the source
declaration and the staging model, the shared-catalog claim, the Trino memory
arithmetic, the severity marking, the lineage parse, and the compose wiring.

---

## Verification log — 2026-09-02

Reproduce with `make analytics-build`, `make analytics-up`, `make dbt-build`,
`make quality`, `make analytics-verify`.

### 0. The engines agree on the input before anything is modelled

The first query Trino ever ran against the shared catalog returned the number ADR-004's
verification log ends with, and ADR-005 independently counted from the topic:

```
lake.raw.trace_events   1908 rows, 1908 distinct span_ids, 0 duplicates
```

Two engines, one catalog, one number. The rest of this log is against a warehouse
grown to **2,829 rows** by resuming the Flink jobs and running 901 more spans through
the real SDK with both the cold and hot paths consuming (which is what makes §4
possible).

### 1. dbt build

```
5 table models, 54 data tests
Done. PASS=59 WARN=0 ERROR=0 SKIP=0 TOTAL=59
```

41.8 s cold, **19.1 s** on an immediate re-run. Row counts:

| model | rows |
|---|---|
| `stg_trace_events` | 2,829 |
| `stg_agg_model_5m` | 42 |
| `fct_sessions` | 96 |
| `fct_model_costs` | 5 |
| `fct_tool_reliability` | 3 |

### 2. Reconciliation against raw

`make analytics-verify`, at the 1,908-row state:

```
staging by event_type          marts vs staging
AGENT_STEP  658                mart                  rows  spans_accounted  staging_spans
RETRIEVAL   592                fct_sessions            66             1908           1908
LLM_CALL    592                fct_model_costs          2              592            592
TOOL_CALL    66                fct_tool_reliability     1               66             66

MATCH  fct_sessions accounts for 1908 spans; lake.raw.trace_events holds 1908
```

658 + 592 + 592 + 66 = 1908, and it reads correctly: the generator emits one RETRIEVAL
and one LLM_CALL per successful turn (592 each), one TOOL_CALL per failed turn (66), and
one AGENT_STEP per turn either way (592 + 66 = 658).

### 3. One row, checked by hand

`make analytics-session` prints the mart row beside a recomputation straight from
`lake.raw.trace_events`. Session `24a49cfd6fd546b395cbb924ae449086`:

```
                turn  span  tool  llm  err  prompt  compl  total   cost_usd  duration_ms
fct_sessions      10    25     5    5   10    1810    522   2332   0.004420           76
recomputed        10    25     5    5   10    1810    522   2332   0.004420           76

event_type  spans  traces  errors  cost_usd
AGENT_STEP     10      10       5   0.000000
RETRIEVAL       5       5       0   0.000000
LLM_CALL        5       5       0   0.004420
TOOL_CALL       5       5       5   0.004420 (0)
```

Identical column for column, and internally consistent: 5 ok turns × 3 spans + 5 error
turns × 2 spans = 25; `error_count` 10 is the 5 AGENT_STEPs plus the 5 TOOL_CALLs their
raises propagated to.

### 4. Cross-engine: exact vs approximate p95

`make analytics-crosscheck`. Only groups **both stores hold identically** are compared —
Iceberg is the archive and ClickHouse expires at 7 days, so a partial day would measure
the retention difference and call it an accuracy result:

```
event_day   model             calls  trino_p95_exact  ch_p95_approx  delta_ms  delta_pct
2026-09-02  claude-haiku-4-5    141           12.026         12.026    +0.000     +0.00%
2026-09-02  claude-sonnet-5     134           12.708         12.521    -0.187     -1.47%

worst divergence 1.47% across 2 group(s) where both stores hold the identical span set.

not compared:
2026-08-31  claude-haiku-4-5  trino 2 / clickhouse 95   different ingest history
2026-08-28  claude-haiku-4-5  trino 314 / clickhouse 0  different ingest history
2026-08-28  claude-sonnet-5   trino 278 / clickhouse 0  different ingest history
```

**Identical call counts on both sides** — 141 and 134 — which is the load-bearing part:
it means the two engines consumed exactly the same spans from the same topic, so the
percentiles are comparable. On one group they agree exactly; on the other ClickHouse
reads 0.187 ms low, which is its sampling quantile against Trino's whole population.
1.47% is the price of the hot path's approximation, measured rather than assumed.

### 5. Breaking it on purpose, and putting it back

```
$ python scripts/seed_iceberg.py inject-bad-row
injected one span with cost_usd = -1.0 (span_id 00000000000000000000000000000bad)

$ make dbt-build
23 of 59 FAIL 1 dbt_utils_accepted_range_fct_model_costs_cost_usd__True__0
37 of 59 FAIL 1 dbt_utils_accepted_range_fct_sessions_total_cost_usd__True__0
Done. PASS=57 WARN=0 ERROR=2 SKIP=0 TOTAL=59

$ make quality  ->  exit 1
  FAIL  [blocking] expect_column_values_to_be_between(total_cost_usd)
  FAIL  [blocking] expect_column_values_to_be_between(cost_usd)
24 expectations: 22 ok, 2 blocking failures, 0 warnings
FAIL -- a blocking expectation did not hold.

$ python scripts/seed_iceberg.py revert-bad-row
removed 1 injected row(s); 0 remain

$ make dbt-build  ->  Done. PASS=59 WARN=0 ERROR=0
$ make quality    ->  exit 0
24 expectations: 24 ok, 0 blocking failures, 0 warnings
PASS -- every blocking expectation held.
```

One impossible row is caught independently by both gates, in both marts that carry a
cost, and the exit code moves 1 → 0 with the data. The guard was checked from the other
side too: `seed` against the populated warehouse exits 1 and refuses rather than mixing
synthetic rows into Flink-written ones.

**The seeder itself** was rehearsed against a scratch table (`--table lake.raw.seed_probe`)
so its SQL generation could be verified without touching real data: 600 spans, 600
distinct span_ids, 21 sessions, 206 traces, all four event types, `TimeoutError` on
every TOOL_CALL, timestamps and costs in range.

### 6. Lineage

```
$ make lineage
declared  agentlake-raw-sink       kafka://kafka:19092/traces.events.v1 -> lake.raw.trace_events
declared  agentlake-agg-model-5m   kafka://kafka:19092/traces.events.v1 -> lake.curated.agg_model_5m
[openlineage.dbt] Emitted 30 OpenLineage events
```

Queried back out of Marquez, 24 nodes reachable from `fct_model_costs`, all four layers
connected:

```
traces.events.v1                --[agentlake-raw-sink]-->      lake.raw.trace_events
traces.events.v1                --[agentlake-agg-model-5m]-->  lake.curated.agg_model_5m
lake.raw.trace_events           --[dbt]-->                     lake.analytics.stg_trace_events
lake.curated.agg_model_5m       --[dbt]-->                     lake.analytics.stg_agg_model_5m
lake.analytics.stg_trace_events --[dbt]-->                     lake.analytics.fct_model_costs
lake.analytics.stg_trace_events --[dbt]-->                     lake.analytics.fct_sessions
lake.analytics.stg_trace_events --[dbt]-->                     lake.analytics.fct_tool_reliability
```

Screenshot: `docs/img/lineage.png`.

### 7. Memory

Measured resident, `make analytics-up` only — note that this slice runs with **kafka and
schema-registry stopped**, which no previous profile could do: it reads Iceberg through
the REST catalog and never touches the topic.

| | resident | limit |
|---|---|---|
| trino | 652 MiB | 1024 MiB |
| iceberg-rest | 129 MiB | 384 MiB |
| minio | 72 MiB | 256 MiB |
| **total containers** | **853 MiB** | |
| host | 2088 MiB used of 3916, 1828 available | |

Adding the `lineage` profile costs a further **245 MiB** (marquez 179, marquez-db 38,
marquez-web 28) for 1,098 MiB total — which is why it is a separate profile and a
separate step rather than part of `make analytics-up`. Same reasoning that moved
`qdrant` into the `rag` profile.

Both are well inside the cap, and comfortably below the `streaming` profile's measured
1,790 MiB — the analytics slice is the cheapest of the three.

### 8. Tests and lint

```
$ python -m pytest -q
303 passed, 1 deselected in 8.85s        (19 of them new, in test_analytics_project.py)

$ ruff check services/ tests/ stream/ scripts/ analytics/ quality/
All checks passed!
```
