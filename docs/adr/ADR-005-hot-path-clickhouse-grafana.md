# ADR-005: Hot path — ClickHouse and Grafana

- **Status:** Accepted
- **Date:** 2026-08-31
- **Context:** `stream/clickhouse/`, `dashboards/`, the `hotpath` compose profile, and
  the two `services/mcp_server` tools that had been honest stubs since ADR-003 §3.

Before this, `traces.events.v1` had exactly one durable consumer: the Flink → Iceberg
cold path. That path is correct and slow on purpose — rows land once per 30s checkpoint
(ADR-004 §4), verification runs as batch jobs that need a free task slot (§7), and
Flink 1.20 has no percentile aggregate at all (§6). So "what is p95 right now" had no
answer, and neither did "show me the trace I just produced".

ClickHouse now consumes the same topic through its Kafka engine into
`agentlake.trace_events_rt`, Grafana is provisioned from files on top of it, and
`get_trace`/`query_metrics` are real. Decisions that are not obvious from reading the
SQL, plus the things that had to be discovered by running it.

---

## 1. Kafka engine, not Kafka Connect

**Decision.** ClickHouse consumes the topic itself: a `Kafka` engine table
(`stream/clickhouse/sql/04_trace_events_kafka.sql`) and two materialized views, one
into the hot table and one into a dead-letter table.

**Why.** Kafka Connect means a second JVM, a Connect worker, a converter configuration
and a REST API to drive it — on a box where ADR-004 §8 already had to shrink a
JobManager's metaspace to buy it a usable heap. The Kafka engine puts the consumer
inside the process that stores the data: no extra container, no extra memory budget,
and one fewer thing to be running for the pipeline to work.

**What it costs, stated plainly.**

- **No SMTs.** Any transformation has to be a materialized view, which is fine here
  (there is one, and it does a rename) but is genuinely less than Connect offers.
- **No dead-letter *topic*.** `kafka_handle_error_mode = 'stream'` surfaces failures
  in the `_error`/`_raw_message` virtual columns instead, and
  `06_trace_events_dlq_mv.sql` routes them into `agentlake.trace_events_dlq`. The two
  views' `WHERE` clauses are complements, so every message lands in exactly one table
  and nothing falls between them — but the dead letters live in ClickHouse, not back
  on Kafka where another consumer could pick them up.
- **The Schema Registry URL is baked into table DDL.** Moving the registry means
  `DROP` and recreate the Kafka table, not a config change.

**The dead-letter table exists because of ADR-004 §7.** On the Flink side, an Avro
enum that would not resolve to a SQL `STRING` failed *every* record on the topic, and
the only symptom was an empty sink — the pipeline looked like it was running. A hot
path that dropped unparseable messages silently would reproduce that failure mode
without the error message.

**No auth, deliberately.** The `default` user is open and the Grafana datasource uses
it. Kafka here is PLAINTEXT and Qdrant is unauthenticated, so a ClickHouse credential
would be the only one in an otherwise open dev stack — security theatre with a real
cost: `readonly = 1` additionally requires granting
`max_execution_time CHANGEABLE_IN_READONLY`, because the Grafana plugin's driver sets
that on every query and otherwise fails at query time *even though "Save & test"
passes*. A real deployment adds a read-only user and that grant together.

---

## 2. Dedup posture: aggregates tolerate, point lookups dedup

**The rule, in one line.** Counts are exact via `uniqExact(span_id)`; sums tolerate a
transient over-count bounded by the merge window; point lookups deduplicate their own
result set.

**Why there is anything to decide.** ClickHouse documents the Kafka engine as
**at-least-once**. A block is written to the MergeTree first and the Kafka offset is
committed second, so a process that dies between the two re-reads that batch on
restart. This is the exact opposite of the cold path's guarantee: ADR-004 §4 gets
end-to-end exactly-once because Flink's Iceberg sink commits data files and offsets as
one atomic fact in a two-phase commit. Trading that away is what buys ~1.5s freshness
instead of ~30s, and the honest thing is to say so rather than imply both paths have
the same guarantee.

**`span_id` is in the sorting key, and that is the whole mechanism.**
`ReplacingMergeTree` deduplicates on the `ORDER BY` and on nothing else.
`(event_type, model, ts)` is the query shape — what every panel and every whitelisted
metric filters and groups on. Appending `span_id`, a full uuid4 hex per ADR-000 §4,
makes the key unique per span, so a re-delivered row is byte-identical to the one
already stored and collapses on the next merge instead of accumulating. Byte-identical
includes `ts`, so every copy lands in the same `toDate(ts)` partition and
partition-local merging is sufficient.

**`allow_nullable_key = 1`** is required, because `model` is `Nullable(String)` and is
in that key. Keeping the contract's nullability rather than `ifNull(model, '')` is what
lets `tests/test_hot_path_contract.py` compare the DDL against the `.avsc` directly.
NULL sorts as its own value, so the NULL-model rows — `RETRIEVAL`, `TOOL_CALL`,
`AGENT_STEP`, real data per ADR-004's verification log — deduplicate correctly too.

**Counting rules, shared by the dashboards and the MCP tool** so both cannot disagree:

- `uniqExact(span_id)`, never `count()`. Exact whatever the merge state.
- Plain `sum()` for `cost_usd` and tokens. These are the values that transiently
  over-report, and the measurement below says by how much.
- `status != 'ok'`, never `status = 'error'`. `status` is a free-form contract string
  and `services/sdk/telemetry.py` promotes any `set(status=...)` value — the SDK's own
  tests already cover `"degraded"`.

**Rejected: `FINAL` everywhere — as a habit, not on cost.** At this volume `FINAL` is
free: `OPTIMIZE TABLE ... FINAL` over 28K rows took **0.881s**, and a per-query `FINAL`
is far cheaper than that. The objection is not the bill. It is that writing `FINAL` into
every panel and every metric teaches a pattern that stops being free exactly when the
table stops being small, and hides which queries actually needed exactness. Choosing an
aggregate that is exact by construction is strictly better than an expensive modifier
that makes an inexact one correct.

**Rejected: plain MergeTree plus a dedup view.** It keeps every duplicate forever and
moves the cost to read time on every query, to preserve a duplicate log nothing reads.

**Measured — and the honest result is that the window did not open.** Five attempts to
force duplicates against a live replay:

| # | Method | Duplicates |
|---|---|---|
| 1 | `docker restart` mid-replay (graceful, SIGTERM) | 0 |
| 2–4 | `docker kill` (SIGKILL) mid-replay, 3 times | 0 |
| 5 | `docker kill` timed to the instant a 1000-row block landed | 0 |

Attempt 1 is explainable and worth knowing: a graceful stop lets ClickHouse commit its
offsets, so the ordinary operational cases — `docker compose restart`, `down`/`up` —
do not duplicate at all. Attempts 2–5 are the real test, and the window still did not
open: the gap between "block written" and "offset committed" is in-process and
sub-millisecond, and an external signal is unlikely to land inside it. Final state
after all five: **27,867 rows, 27,867 distinct span_ids, 27,867 messages on the topic.**

**So the mechanism was verified directly instead.** Failing to win a race is not
evidence that the handling works, so 500 existing rows were re-inserted byte-identically
— exactly what a replayed Kafka batch produces — and every part of the posture was
measured against them:

| | before | with 500 duplicates | after merge |
|---|---|---|---|
| `uniqExact(span_id)` (LLM_CALL) | 8792 | **8792** | 8792 |
| `count()` (what we do *not* use) | 8792 | 9292 | 8792 |
| `sum(cost_usd)` | 7.021781 | 7.418556 | 7.021781 |

Counts were **exactly unaffected**. A raw row count would have over-reported by 5.7%,
and the sums did over-report by 5.6% until the merge — which is precisely the tolerance
this posture claims and bounds. `get_trace` on an affected trace returned **3 spans
where the raw table held 4 rows**: `LIMIT 1 BY span_id` is the point-lookup half, and a
trace is tens of rows, so deduplicating it outright costs nothing and the model is never
handed the same span twice.

---

## 3. TTL 7 days, and what the split is for

`TTL toDateTime(ts) + INTERVAL 7 DAY`, with `ttl_only_drop_parts = 1`.

Hot means recent. ClickHouse answers "what is happening now" over a window that fits in
memory on a laptop; Iceberg (`lake.raw.trace_events`) is the archive and has no TTL. The
division is not about cost here — 27,867 rows occupy **2.78 MiB** on disk — it is about
what each store is allowed to be asked. A question about last Tuesday belongs to the
lake, and `get_trace`'s not-found error says so by name rather than just failing.

`ttl_only_drop_parts = 1` drops a whole part once every row in it has expired, instead
of rewriting parts to delete rows individually. It pairs with the daily partitioning:
a day's part expires as a unit. The cost, stated rather than glossed: expiry lags by up
to `merge_with_ttl_timeout`, which defaults to **4 hours**. Rows older than 7 days can
therefore still be present, and a query that must not see them needs its own
`WHERE ts >= …` — every dashboard panel and every whitelisted metric has one anyway.

---

## 4. Percentiles land here, and that closes ADR-004 §6

ADR-004 §6 recorded a real gap: Flink 1.20's SQL dialect has no percentile aggregate
(`PERCENTILE` arrived in Flink 2.0; `APPROX_PERCENTILE` has never existed), so
`agg_model_5m` carries sums and a max, and percentiles were deferred "downstream from
`lake.raw.trace_events`, one join away".

This is downstream. `quantile(0.5|0.95|0.99)(latency_ms)` is a single column expression
here, it powers four dashboard panels and the `p95_latency` metric, and it runs in
**16.5 ms** over the full table. The gap is discharged, and it is worth noting *how*: not
by upgrading Flink — ADR-004 §1 explicitly rejected Flink 2.1 because it would desync
the RAG corpus and reopen every compatibility question the version matrix had just
closed — but by putting the question in the store that was always going to be better at
it. The cold path did not need to change at all.

---

## 5. Whitelisted queries for the agent's tool

**Decision.** `query_metrics(metric, window, group_by)` accepts three keys from three
module-level dicts in `services/mcp_server/tools.py` and nothing else. There is no
free-form SQL parameter, and no code path that concatenates caller text into a query.
`get_trace` takes a `trace_id` bound as a ClickHouse HTTP query parameter
(`{trace_id:String}`), never formatted into the string.

**Why, beyond injection.** Injection is the obvious reason and it is the least
interesting one. A closed set is a set whose cost and correctness can be *measured*:
every metric's SQL is timed by the same script that times the dashboard panels, and
`tests/test_hot_path_tools.py` can assert that every one of them counts distinct spans
and tests `status != 'ok'`. A free-form SQL tool has no such property — its worst case
is unbounded, its correctness is whatever the model wrote that time, and "the agent
wrote a slow query" becomes a production incident rather than a test failure.

The whitelist is enforced twice on purpose: as JSON Schema `enum`s, so
`dispatch_tool()`'s `jsonschema.validate()` rejects a bad argument before the tool runs;
and inside the tool, which is the seam the tests drive and the one a non-MCP caller
would reach. `tests/test_hot_path_tools.py` asserts the two agree, because a whitelist
that exists in two places is a whitelist that can disagree with itself.

**The result carries the SQL that produced it.** This is the point of the tool, not a
debugging aid. An agent quoting a p95 to a human is asking to be believed; shipping the
query alongside the number makes the claim checkable instead of trusted. It is the same
instinct as ADR-003 §3's honest stubs, applied to a tool that now has real data: the
failure being designed against is not "the tool is wrong", it is "nobody could tell".

**Honest failure survived the store arriving.** ADR-003 §3 wrote `{"error": ...}` as the
permanent contract for an unreachable store, not as a placeholder, and that turned out
to be right — the two tools kept the shape and only changed what they do when the store
*is* reachable. `get_trace` on a missing trace returns an error naming the 7-day TTL and
pointing at `lake.raw.trace_events`, never an empty tree that would read as "this turn
did nothing".

**Warmup, for the third time.** `server.warmup()` now pings ClickHouse alongside loading
the embedding model and touching Qdrant. Same finding as ADR-000 §3 and ADR-003 §6:
without it the first `query_metrics` span would include a TCP handshake, and a metric
describing latency would be the one thing in this repo lying about its own.

---

## 6. The contract is not duplicated on this path

ADR-004 §7 had to spell the entire `.avsc` into an `avro-confluent.schema` literal in
*both* Flink jobs, because Flink derives a reader schema from the table's column types,
which makes `event_type` a plain Avro `string`, and Avro's schema resolution does not
promote an enum to a string — every record failed with
`Found agentlake.v1.EventType, expecting string`.

ClickHouse's `AvroConfluent` reader resolves the enum into a `String` column on its own.
There is no reader schema to supply, so the contract lives in exactly one file on this
path. Verified by the dead-letter table staying empty across 27,867 messages.

`tests/test_hot_path_contract.py` asserts the *absence* of a contract copy in the
hot-path SQL — the inverse of what `tests/test_cold_path_contract.py` has to check — so
that nobody "fixes" a future problem by pasting one in and quietly reintroducing the
drift surface.

---

## 7. No resume guard, unlike the cold path

`stream/flink/submit.sh` refuses a plain submit while a resume point exists, because
Flink keeps its Kafka offsets in checkpointed state and a fresh start would replay from
earliest into a populated Iceberg table (ADR-004 §11). That guard exists because the
failure is silent: no error, no warning, no symptom until someone runs a `COUNT` days
later.

The hot path needs no equivalent. This consumer's offsets live in the broker under group
`clickhouse-hotpath`, so a restart resumes from the committed offset by itself — which
is what attempts 1–5 in §2 demonstrate from the other direction. `bootstrap.py
--recreate` is still destructive, but it destroys data rather than silently duplicating
it, and re-ingest picks up from the committed offset rather than replaying the topic.

The asymmetry is surprising enough to be worth stating: the path with the *weaker*
delivery guarantee is the one that needs no operational guard, because its offsets live
somewhere that outlives the process.

---

## 8. Rename at the transformation boundary, once

`trace_events_kafka` spells the column `ts_epoch_ms`; `trace_events_rt` calls it `ts`;
the materialized view does `ts_epoch_ms AS ts`.

The contract-shaped layer has no choice: ClickHouse binds Avro fields to columns by
name, so the Kafka table must use the contract's name or the field is silently skipped.
The MV is this path's one transformation step, so the rename rides along with the
projection that is happening anyway, and everything downstream — dashboards,
whitelisted metrics, `get_trace` — uses the short name.
`tests/test_hot_path_contract.py` asserts it is the only rename, in the only place.

**Why the cold path differs is not the obvious reason, and the obvious reason is
falsifiable with one `grep`.** It is *not* that Iceberg stores an unconverted long:
`stream/flink/create_tables.py` types `ts_epoch_ms` as Iceberg `timestamp`, and
`01_raw_sink.sql` declares it `TIMESTAMP(3)`. The cold path converts the type at exactly
the same point this one does. What it does not have is a *projection* step —
`01_raw_sink.sql` is a straight column-for-column `INSERT ... SELECT` — so there is no
boundary for a rename to ride along with, and ADR-004 §2's "the column IS the contract
field" stands unchanged.

The rule is **rename where you already transform**, not "long stays, timestamp renames".

---

## 9. Memory, and four things that had to be discovered by running it

**Measured, whole `hotpath` slice up, both dashboards loaded:**

| | resident | limit |
|---|---|---|
| kafka | 402 MiB | 1024 MiB |
| clickhouse | 263 MiB | 768 MiB |
| schema-registry | 148 MiB | 512 MiB |
| grafana | 140 MiB | 320 MiB |
| **total containers** | **953 MiB** | |
| host | 2321 MiB used of 3916, 1595 available | |

Comfortably inside the 4 GB cap, and well under the sum of the limits — the limits are
ceilings that stop a runaway process, not reservations. ClickHouse holds 27,867 rows in
2.78 MiB.

**Grafana's peak is at first start, not steady state.** It settles at 140 MiB but
measured **257 MiB** while unzipping the ClickHouse datasource plugin on a cold volume,
and **248 MiB** on a second cold install after `down -v`. A 256m limit — the obvious
round number, and the one this slice was first sized at — sits right on top of that
peak, so it would have been an OOM kill on the very first `make hot-up` and would have
looked like a Grafana bug rather than a sizing one. 320m, with `GOMEMLIMIT: 260MiB` so
the Go GC gets aggressive before the cgroup killer does.

Four things cost real time and none is visible in any single doc page.

**A directory mount over `config.d` replaces the image's own config.** ClickHouse ships
exactly one file there, `docker_related_config.xml`, and it contains the `listen_host`
settings. Bind-mounting a host directory at `/etc/clickhouse-server/config.d` hides it,
so the server binds `127.0.0.1` inside the container and nothing else. The symptom is
the worst kind: the container reports `Up`, the log shows a clean startup with every
config file merged, and every connection is refused — because from the server's point of
view nothing went wrong. `01-listen.xml` restores those three settings. This is the same
class of trap as the Flink jars in `docker-compose.yml`, which are bind-mounted one file
at a time precisely because a directory mount over `/opt/flink/lib` hides Flink's own
jars. A directory mount is a replacement, not a merge.

**`background_pool_size` does not shrink alone.** Cutting it to 4 — standard low-memory
advice — exits **36 (BAD_ARGUMENTS)** at boot, because ClickHouse sanity-checks that
`number_of_free_entries_in_pool_to_execute_mutation` (default 20) sits below
`background_pool_size * background_merges_mutations_concurrency_ratio` (4 × 2 = 8).
Raising it to 8 then tripped
`number_of_free_entries_in_pool_to_execute_optimize_entire_partition` (default 25, needs
< 16). Each fix revealed the next. Chasing it to the end would have meant three magic
numbers in `config.d/00-memory.xml` to buy a handful of lazily committed thread stacks,
on a server whose memory actually goes to caches and query working sets — both of which
*are* capped. The pool is back at its default and the attempt is recorded in the file so
the next person does not repeat it. Same shape as ADR-004 §8: these knobs are a system of
constraints, not independent dials.

**XML comments cannot contain a double hyphen.** This repo writes `--` as an em dash
everywhere, and the first `config.d` file did too. The result is not a warning: the
server refuses to start with
`Failed to merge config with ... SAXParseException: Invalid token`. Every XML file here
now says so at the bottom.

**Mounting `users.d` read-only breaks the entrypoint.** With neither `CLICKHOUSE_USER`
nor `CLICKHOUSE_PASSWORD` set, it tries to *write*
`users.d/default-user.xml` to disable network access for `default`, and exits 1 with
`Read-only file system`. `CLICKHOUSE_SKIP_USER_SETUP: "1"` is what skips that block and
keeps `default` reachable, which is what Grafana and every HTTP caller need.

**One more, in the Python.** `sum(prompt_tokens) AS prompt_tokens` puts the alias in
scope for the rest of the `SELECT`, so a later `sum(prompt_tokens)` resolves to the
aggregate rather than the column and ClickHouse rejects the query with code 184,
*"Aggregate function ... is found inside another aggregate function"*. Suffixing the
aliases is the whole fix; it is noted in `tools.py` beside the query.

**And one in the tests.** `tests/conftest.py` blocked the Kafka path but nothing blocked
ClickHouse, so a tool test that forgot to inject a fake client silently queried the
developer's running instance and asserted against local data. The new `_no_clickhouse`
autouse fixture points `AGENTLAKE_CLICKHOUSE` at a closed loopback port. It sets the
environment variable rather than patching `clickhouse.default_url`, and that distinction
is load-bearing: `ClickHouseClient` resolves its url with
`field(default_factory=default_url)`, which captures the function object at class
definition, so rebinding the module attribute afterwards does nothing.
`services/rag/qdrant_store.py` has the same shape and the same caveat.

---

## Verification log — 2026-08-31

Reproduce with `make hot-up`, `make ch-tables`, `make traffic`, then the
`scripts/hot_path_verify.py` subcommands.

**1. Ingest.** The consumer was pointed at a topic that already held the 1908 events
ADR-004's verification log ended with, and landed **exactly 1908 rows, 1908 distinct
span_ids, 0 duplicates, 0 dead letters** — the hot path and the cold path independently
agreeing on the same number from the same topic. After a further 2001-span run plus the
duplicate-window testing above:

```
rows              27867
distinct span_ids 27867
duplicates        0
dead letters      0
active parts      2 holding 27867 rows

topic offsets     27867
MATCH             27867 distinct spans == 27867 messages on the topic
```

The dead-letter count is the load-bearing zero: it is what proves ClickHouse resolved
the Avro enum without a reader schema (§6).

**2. Freshness (NFR-2).** 50 probes, each emitting a marked span through the real SDK —
`services.sdk`'s `session()`/`span()`, so the measured path is Avro → Kafka → Kafka
engine → materialized view → MergeTree with nothing stubbed — then polling ClickHouse
until it is visible:

```
probes    50 visible, 0 timed out
min       1474 ms
p50       1526 ms
p95       1569 ms
max       1590 ms

PASS      NFR-2 target is p95 <= 5s
```

The distribution is remarkably tight because `kafka_flush_interval_ms = 1000`
dominates it. That setting is the freshness knob: at the 7500 ms default the p95 target
would not be reachable.

**3. Panel timings (NFR-5).** All 15 SQL targets, read out of `dashboards/json/*.json`
so the timings cannot drift into measuring queries the dashboards do not run, with the
Grafana macros expanded and `elapsed` taken from ClickHouse's own `FORMAT JSON`
statistics. At 27,867 rows over a 7-day window:

```
  ok       60.8 ms   ops/Spans                    ok       16.6 ms   ops/Tokens per minute by model
  ok       43.6 ms   ops/Error rate               ok        6.3 ms   ops/Cost by model
  ok        7.7 ms   ops/Cost                     ok        8.6 ms   ops/Retrieval latency p50 / p95
  ok       16.5 ms   ops/p95 LLM latency          ok       10.7 ms   ops/Tool error rate
  ok       57.8 ms   ops/Request rate by type     ok       24.1 ms   ops/Cost per session
  ok       18.8 ms   ops/LLM latency p50/p95/p99  ok       94.2 ms   quality/Cost per turn by prompt version
  ok       13.6 ms   ops/Error rate               ok        4.9 ms   quality/Tokens per turn by prompt version
                                                  ok       21.6 ms   quality/Turn cost distribution

slowest   94.2 ms
PASS      NFR-5 target is every panel < 1000 ms
```

**No pre-aggregating materialized view was added**, and that is a measurement rather
than an assumption: the slowest panel is 10× inside the budget against the full table.
A rollup would have been complexity bought with no evidence. If a panel ever misses,
`scripts/hot_path_verify.py panels` is what says which one.

**The first query after a restart is the slow one**, and it is worth knowing which
number is which. Re-run against a freshly created container and a cold page cache, the
worst panel was **570.9 ms**; the immediately following run of the same 15 queries put
it at **103.6 ms**. Both pass, but a single cold measurement is roughly 5× a warm one,
so a panel timing taken right after `make hot-up` is not the steady-state number and
should not be quoted as one.

**4. Duplicate window.** Covered in §2 with the five attempts and the injected-duplicate
measurement. Short version: five adversarial restarts including one SIGKILL timed to a
block flush produced zero duplicates; injecting 500 byte-identical rows showed
`uniqExact(span_id)` exactly unaffected, `sum(cost_usd)` over-reporting by 5.6% until
the merge, `get_trace` returning 3 spans where the table held 4 rows, and
`OPTIMIZE ... FINAL` collapsing all 500 in 0.881s.

**5. Agent end to end.** With ClickHouse settled, then the gateway and Qdrant started
(Grafana stopped for the memory), `python -m services.agent` was asked the question the
tools were built for. It called `query_metrics` twice — `p95_latency` grouped by model,
then `cost_by_model` — and answered:

> claude-haiku-4-5: 11.81 ms · claude-sonnet-5: 11.793 ms · Unspecified model: 16.269 ms
> … claude-haiku-4-5 is the most expensive at $3.30, claude-sonnet-5 follows at $3.25 …
> likely due to higher call volume (4,138 spans vs 4,063)

Checked against the store directly, every figure holds: p95 11.819 / 11.793 / 16.271,
cost 3.304134 / 3.251370, spans 4139 / 4063. The small drift is seconds of new traffic
between the two queries. The agent also read the NULL-model group correctly as
"unspecified" rather than dropping it.

Then `get_trace` on that turn's **own** `trace_id`:

```
AGENT_STEP  agent_turn          ok    15721.49ms
  GATEWAY     chat              ok     5255.65ms
    LLM_CALL    anthropic_messages  ok  3173.50ms  claude-haiku-4-5  $0.002021
  TOOL_CALL   query_metrics     ok     1469.02ms
  TOOL_CALL   query_metrics     ok     1111.60ms
  GATEWAY     chat              ok     2406.89ms
    LLM_CALL    anthropic_messages  ok  2405.61ms  claude-haiku-4-5  $0.002964

span_count 7, root_count 1, orphan_count 0, total_cost_usd 0.004985, total_tokens 3585
```

Seven spans, one root, no orphans, three levels, across three processes — and
`total_cost_usd` 0.004985 matches the `$0.0050` the turn itself reported. This is the
loop ADR-003 §4 opened when it rebuilt trace propagation because a single turn was
showing up as six separate traces: one turn is one trace end to end, and the agent can
now read it back.
