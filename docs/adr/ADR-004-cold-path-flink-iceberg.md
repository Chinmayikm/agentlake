# ADR-004: Cold path — Flink SQL to Iceberg

- **Status:** Accepted
- **Date:** 2026-08-28
- **Context:** `stream/flink/` and the `streaming` compose profile — the path that
  turns `traces.events.v1` from a 7-day Kafka retention window into a queryable
  lakehouse table.

Before this, nothing durably landed a TraceEvent. `scripts/consume_tree.py` tails the
topic and `services/mcp_server`'s `get_trace`/`query_metrics` are honest stubs precisely
because there was no history to query. Two Flink SQL jobs now write Iceberg tables on
MinIO: `lake.raw.trace_events` (append, every contract field) and
`lake.curated.agg_model_5m` (5-minute tumbling aggregates).

Decisions that are not obvious from reading the SQL, plus two gaps recorded rather
than papered over.

---

## 1. The version matrix, and what actually pinned it

| Component | Pin |
|---|---|
| Flink | `flink:1.20.5-scala_2.12-java11` |
| Kafka SQL connector | `flink-sql-connector-kafka:3.4.0-1.20` |
| avro-confluent format | `flink-sql-avro-confluent-registry:1.20.5` |
| Iceberg Flink runtime | `iceberg-flink-runtime-1.20:1.10.1` |
| Iceberg AWS bundle | `iceberg-aws-bundle:1.10.1` |
| Hadoop | `hadoop-client-api` + `hadoop-client-runtime:3.4.2` |
| REST catalog | `apache/iceberg-rest-fixture:1.10.1` |
| MinIO | `RELEASE.2025-09-07T16-13-09Z`, `mc:RELEASE.2025-08-13T08-35-41Z` |

**Flink 1.20 was the input, not a conclusion** — it matches the corpus pin in
`services/rag/sources.yaml`, so a question answered out of the RAG index describes the
Flink we actually run. Everything else follows from it. `flink-sql-connector-kafka`
publishes `3.4.0-1.20` as its newest build for 1.20; the 4.x and 5.x lines are
Flink-2.x-only. The format jar is Flink-versioned and tracks the image patch.

**What pinned Iceberg to 1.10.1 was the catalog image, not the connector.** Iceberg
publishes `iceberg-flink-runtime-1.20` for 1.7.0 through 1.11.0, so the connector was
never the constraint. The constraint is that a REST catalog needs a server:
`tabulario/iceberg-rest`, the one most guides still name, is abandoned — its newest tag
is Iceberg **1.6.0**. The maintained replacement, `apache/iceberg-rest-fixture`,
publishes 1.8.1 through 1.10.1. 1.10.1 is the newest tag that exists, so that is the
version, and everything Iceberg-shaped matches it.

**Consequence — a drift we accept.** The corpus pins Iceberg **1.7** docs while the
runtime is 1.10.1. There is no way to close it from the runtime side: no REST-catalog
image exists below 1.8.1. Closing it from the corpus side is a one-line edit to
`services/rag/sources.yaml` plus a re-ingest; deferred, and written down here so it is
a known gap rather than a surprise.

**Rejected: Flink 2.1.** It would have supplied the percentile function §6 does without,
and Iceberg 1.11.0 does publish `iceberg-flink-runtime-2.1`. But it desyncs the corpus,
and it reopens every compatibility question the matrix above just closed, in exchange
for one aggregate we can compute downstream.

---

## 2. Iceberg DDL lives outside Flink, because Flink cannot express it

`lake.raw.trace_events` is partitioned by `day(ts_epoch_ms)` — Iceberg *hidden*
partitioning: derived from a column by a transform, not a column itself, pruned on
without ever being named in a query. Flink SQL cannot declare it. From the Iceberg 1.10
Flink DDL docs, verbatim:

> Iceberg supports hidden partitioning but Flink doesn't support partitioning by a
> function on columns. There is no way to support hidden partitions in the Flink DDL.

`PARTITIONED BY` in Flink takes identity columns only.

**Decision.** `stream/flink/create_tables.py` POSTs an Iceberg `CreateTableRequest`
straight at the REST catalog, partition spec included; the SQL jobs only ever
`INSERT INTO` and pick the spec up from the catalog. Verified live — the warehouse
lays data out as `data/ts_day=2026-08-28/…parquet`.

**This does not violate "only Flink writes to Iceberg."** That rule is about the data
plane, and it holds: every Parquet file and every snapshot in the warehouse is written
by a Flink task. What `create_tables.py` writes is a table definition.

**Rejected: an explicit `ts_day DATE` column** partitioned by identity. It is pure
Flink SQL, but it is a 14th column that is not in the contract, and it is not hidden
partitioning — every query would have to filter on `ts_day` by hand to prune.

**Rejected: pyiceberg.** `httpx` is already a dependency. pyiceberg would be a new one
that declares support only through Python 3.13 (this repo runs 3.14) and pulls in
pyarrow, to do what is one POST of a JSON document.

---

## 3. Watermarks: 30s out-of-orderness, 60s source idleness

`WATERMARK FOR ts_epoch_ms AS ts_epoch_ms - INTERVAL '30' SECOND`, with
`table.exec.source.idle-timeout = 60s`.

**30 seconds** is sized to the skew the SDK actually produces. `ts_epoch_ms` is stamped
in-process when a span *opens*, but the event is emitted from the span's `finally`, so
a record's timestamp is older than its produce time by the span's duration, plus
librdkafka batching, plus the ~800ms first-emit init ADR-000 §3 describes. Not clock
drift — a single process stamps every event here.

**60 seconds of idleness, deliberately longer than the 30s checkpoint interval.** The
topic has 3 partitions and less traffic than that; a partition that goes quiet holds
its watermark at its last event, and the operator watermark is the *minimum* across
partitions, so one quiet partition freezes every window indefinitely. Idleness drops a
silent partition out of that minimum.

**Verified, and worth being precise about what it proves.** Traffic was trickled into a
single session — therefore a single partition, since the SDK keys by `session_id` —
from 17:04:57 to 17:12:23, while the other two partitions had been silent since ~16:48.
The first aggregate rows committed at **17:05:22, 25 seconds after the trickle began**:
the two idle partitions did not hold the watermark back, and windows closed on the
strength of the one active partition.

**What idleness does not do, and this is the honest half.** It excludes idle partitions
from the minimum; it does not manufacture a watermark. When *every* partition is idle
the watermark stops dead, and the window holding the last events never closes. Measured
directly: after the first 600-event burst ended at ~16:47 the aggregate job's
`currentInputWatermark` froze and `lake.curated.agg_model_5m` stayed empty until new
traffic arrived. So the final open window of any quiet period is not "late" — it is
simply not yet closed, and it closes when traffic resumes. A test that stops all
traffic and waits for the last window to close waits forever. Closing that properly
needs an idleness-driven watermark advance the SQL API does not offer.

---

## 4. Exactly-once, and what it actually costs

`execution.checkpointing.interval = 30s`, `mode = EXACTLY_ONCE`, checkpoints on MinIO.

Kafka offsets live in checkpointed state — not in the broker's committed offsets, which
is why `scan.startup.mode = earliest-offset` only decides where a job with *no* state
begins. The Iceberg sink is a two-phase commit: writers produce data files continuously
and the `IcebergFilesCommitter` commits them as one Iceberg snapshot in the checkpoint's
completion phase, stamping the snapshot with `flink.job-id` and
`max-committed-checkpoint-id`. On recovery the committer reads those properties back
and skips any checkpoint already committed, so a replay from the restored offsets
cannot commit the same files twice. The committed snapshot and the offsets that
produced it are one atomic fact.

**Consequence: Iceberg lags by up to one checkpoint interval.** Rows are visible ~30s
after they are produced, not continuously. That is the price of the guarantee, and it
is why this is the *cold* path.

**Verified** by `docker kill agentlake-flink-tm` mid-stream — see the verification log
at the end.

**The restart budget has to outlast the thing it waits for.** The first configuration
was `fixed-delay, 10 attempts, 5s` — 50 seconds. Restarting the TaskManager container
on this box takes ~2.5 minutes (JVM start, plugin extraction, slot registration), so
both jobs exhausted their attempts and failed *permanently*. That is worse than it
sounds: a failed job loses its state, and resubmitting replays from earliest into a
table that already holds those rows — the exactly-once guarantee is intact within a
job's lifetime and says nothing across one. Now `60 attempts, 10s` — a 10-minute
budget.

---

## 5. Known gap: late events are dropped, silently

A row arriving after the watermark has passed its window end is **discarded** by
`agg_model_5m`. Not counted, not logged, not diverted.

Flink SQL's window TVF aggregation has no side output — that is a DataStream API
facility, and `OutputTag` has no SQL equivalent. `table.exec.emit.late-fire` exists but
re-fires the window as an *updating* result, which the Iceberg append sink rejects
outright, so it is not an option here either.

The 30s watermark makes this rare rather than absent. It is a real gap, deferred to
hardening. Closing it means a second job writing a `lake.raw.late_events` side table
(the raw sink already keeps every event regardless of lateness, so nothing is lost from
the lake — only from the aggregate), or moving this job to the DataStream API, which
this slice deliberately does not do.

**`lake.raw.trace_events` has no such gap.** It is an unwindowed append: lateness is
meaningless there, and every event reaches it.

---

## 6. Known gap: no `latency_p95` in the 5-minute table

**Flink 1.20 has no percentile aggregate.** The full list of built-in aggregates in the
1.20 SQL reference is COUNT, AVG, SUM, MAX, MIN, STDDEV_POP, STDDEV_SAMP, VAR_POP,
VAR_SAMP, VARIANCE, COLLECT. `PERCENTILE` arrived in Flink 2.0 (FLINK-36123);
`APPROX_PERCENTILE` has never existed in Flink at all.

So `agg_model_5m` carries `latency_sum_ms` and `latency_max_ms` (with `event_count`,
which gives the mean), and percentiles are computed downstream from
`lake.raw.trace_events`, which holds every individual `latency_ms`. This is a stated
limitation of the 5-minute table, not an omission: the data to compute an exact p95 is
in the lake, one join away, at whatever grain the question needs.

**Rejected: a Python UDAF** via the PyFlink Table API. It gives a true p95, but it
needs a custom Flink image carrying pyflink and pandas and a Python process per
TaskManager — on a box where the TaskManager has 217 MB of task heap.

**Rejected: a fixed-bucket latency histogram** in pure SQL
(`COUNT(*) FILTER (WHERE latency_ms < …)` per bucket, interpolated downstream). It
works and it is boring, but it adds seven columns and an approximation to a table that
sits next to the exact source data.

---

## 7. Four things that had to be discovered by running it

Recorded because each cost real time and none is visible in any single doc page.

**The Avro enum does not resolve to a SQL STRING.** Flink derives the avro-confluent
*reader* schema from the table's columns, which makes `event_type` a plain Avro
`string`; Avro's schema resolution does not promote an enum to a string, so every
record failed with `Found agentlake.v1.EventType, expecting string`. The fix is
Flink 1.20's `avro-confluent.schema` option, handing the format the real contract so
reader and writer schemas are identical — the enum symbol then reaches the STRING
column through Flink's own `toString()` conversion. Changing the contract instead was
never available: enum→string is exactly the resolution that fails, so it would break
BACKWARD compatibility. The cost is that the contract is now duplicated into two SQL
files, which is why `tests/test_cold_path_contract.py` exists.

**Iceberg's Flink catalog needs Hadoop on the classpath even with a REST catalog and S3
storage**, because `FlinkCatalogFactory` takes a `org.apache.hadoop.conf.Configuration`
regardless. `CREATE CATALOG` fails with `ClassNotFoundException` without it.
`hadoop-client-api` + `hadoop-client-runtime` 3.4.2 are the two shaded jars that
satisfy it; Iceberg's docs suggest `flink-shaded-hadoop-2-uber`, which is one jar but
pins Hadoop 2.8.3 and is deprecated Flink-side.

**`raw` and `model` are reserved words in Flink SQL** — the RAW type and the ML model
syntax. Both need backticks. The names are kept: `raw`/`curated` is the layer
vocabulary the rest of the lakehouse will use, and Trino and dbt have no such conflict.

**Verification queries need a task slot, and there are only two.** They are bounded
batch jobs; with both streaming jobs running they do not fail, they *queue* — one sat
pending for 25 minutes looking exactly like a hung terminal. `submit.sh --verify` now
checks `slots-available` first and says so. Cancel the streaming jobs, verify, resubmit.

---

## 8. Memory: the JobManager heap is what is left over, not a share

Every Flink process sets `*.memory.process.size` below its cgroup `mem_limit`. Without
it the JVM sizes itself from host RAM, overshoots the cgroup and is OOM-killed rather
than back-pressuring.

The trap is the arithmetic. Heap is the **remainder** after the fixed regions are
subtracted, so at stock defaults a 600 MB JobManager gets 256m metaspace + 192m
overhead floor + 128m off-heap = 576m of fixed regions and a **24 MB heap** — Flink
logs a warning that this is below its own 128 MB recommended minimum, then dies of
`OutOfMemoryError` during startup. Shrinking the three fixed regions is what buys a
usable heap.

Measured, as Flink reports them at startup:

| | JobManager | TaskManager |
|---|---|---|
| `mem_limit` (cgroup) | 768m | 1024m |
| Total Process Memory | 700m | 960m |
| Total Flink Memory | 380m | 512m |
| JVM Heap / Task Heap | **284m** | **179.2m** |
| Framework heap | — | 128m |
| Off-heap / Framework off-heap | 96m | 128m |
| Managed | — | 25.6m (`fraction: 0.05`) |
| Network | — | 51.2m |
| JVM Metaspace | 192m | 256m |
| JVM Overhead | 128m | 192m |

`managed.fraction` is cut to 0.05 because managed memory only matters to the RocksDB
state backend and this runs `hashmap`. That buys task heap, which is what the Iceberg
Parquet writers need.

**`framework.off-heap.size` was cut too, and that was a mistake worth recording.** At
64m the streaming jobs were fine and every *batch* query — i.e. every verification
query in `stream/flink/verify/` — died with `Can't allocate enough direct buffer for
batch shuffle read buffer pool`. Flink carves the batch shuffle pool out of framework
off-heap, and its own error message spells out the diagnosis: *"If you have ever
decreased taskmanager.memory.framework.off-heap.size, you need to undo the decrement."*
Streaming and batch do not have the same memory profile, and tuning against only one of
them tunes the other into a wall. It is back at its 128m default.

The Parquet writers are also configured down at the table level —
`write.parquet.row-group-size-bytes` 8 MB against a 128 MB default, and 32 MB target
files — because a row group is buffered in the writer's heap before it is flushed, and
one default row group is most of this TaskManager's task heap.

**Measured resident set with the whole streaming profile up and both jobs running:**

| | |
|---|---|
| kafka | 444 MiB |
| flink-taskmanager | 557 MiB |
| flink-jobmanager | 412 MiB |
| schema-registry | 157 MiB |
| iceberg-rest | 149 MiB |
| minio | 72 MiB |
| **total containers** | **1790 MiB** |
| host | 2958 MiB used of 3916 MiB, 958 MiB available |

Comfortably inside the 4 GB WSL cap, and well under the sum of the limits — the limits
are ceilings that stop a runaway JVM, not reservations.

**Two slots is the ceiling, and it binds.** Two streaming jobs at parallelism 1 fill
them, which is what makes §7's verification-queue trap possible. `qdrant` was moved to
its own `rag` profile so that `--profile streaming up -d` does not also start 512 MB of
vector store nobody asked for.

---

## 9. Checkpoints go to MinIO, not to a local volume

The first attempt was a named volume mounted into both the JobManager and the
TaskManager. It failed with `Failed to create directory for shared state`: a fresh
named volume is created root-owned, and the Flink image runs as uid 9999 `flink`. Same
class of trap as the `KAFKA_LOG_DIRS` comment in `docker-compose.yml`.

Rather than work around the permission, checkpoints moved to `s3://lake/checkpoints` via
Flink's bundled `flink-s3-fs-hadoop` plugin. Object storage is the better answer
anyway: it is reachable from every container, it outlives all of them, it removes the
"both processes must see the same disk" caveat entirely, and it is what a real
deployment checkpoints to. The plugin loads in an isolated classloader, so its shaded
Hadoop cannot collide with the `hadoop-client-*` jars in `lib/`.

Credentials reach Iceberg's S3FileIO as `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
environment variables on the Flink containers rather than as `CREATE CATALOG` options,
because catalog options live in `stream/flink/jobs/*.sql`, which is committed.
`AWS_REGION` is required even though MinIO ignores it — the SDK refuses to build a
client without one.

---

## 10. The Iceberg REST catalog is a test fixture

`apache/iceberg-rest-fixture` is exactly what its name says: upstream's test fixture, a
`JdbcCatalog` behind the REST spec, backed by SQLite. It is right for a laptop and it
is not a production catalog. Its own default puts the SQLite file at
`/tmp/iceberg_catalog.db` inside the container layer, so every `docker compose down`
would drop the catalog while the data files survived in MinIO — an orphaned warehouse.
`CATALOG_URI` points it at a named volume mounted on `/home/iceberg`, which is the one
path that pre-exists in that image owned by its uid-1000 user, so Docker seeds the
volume with workable ownership.

A real deployment swaps in Nessie, Polaris or a managed catalog. Nothing above the
catalog changes when it does — that is the point of talking REST to it.

---

## 11. Stopping and resuming: retained checkpoints, not savepoints

§4's exactly-once guarantee holds *within one job's lifetime*. Across a restart it held
nothing, because Flink deletes a job's checkpoints when the job is cancelled — so the
only way to restart was from `earliest-offset`, into an Iceberg table that already held
those rows. That is a duplicate-generating footgun sitting behind a routine operation,
and on a two-slot TaskManager stopping the jobs is not rare: it is what you do every
time you want to run a verification query (§7).

**Decision.** One option, set in each job's SQL beside the other
`execution.checkpointing.*` settings:

```sql
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
```

**It belongs in the SQL, not in `docker-compose.yml`, and that distinction cost a
verification run.** Put on the JobManager and TaskManager it loads cleanly — the
startup log even prints `Loading configuration property:
execution.checkpointing.externalized-checkpoint-retention, RETAIN_ON_CANCELLATION` —
and then retains nothing, because `execution.checkpointing.*` is a *job*-level option
baked into the JobGraph by whoever submits it. Here that is the SQL client container,
which has its own configuration and never sees the JobManager's. The symptom was a
resume that failed with `FileNotFoundException: Cannot find checkpoint or savepoint
file/directory 's3://lake/checkpoints/<job-id>/chk-6'` — the `chk-*` directories had
been deleted on cancel exactly as if nothing had been configured, leaving only the
empty `shared/` and `taskowned/` siblings. Cluster config that loads without complaint
is not the same as cluster config that applies; `execution.checkpointing.interval` and
`mode` were already in the SQL for this same reason, and this now sits with them.

The last completed checkpoint now outlives the job, and a job restarted with
`execution.savepoint.path` pointing at it resumes from the Kafka offsets in that
checkpoint. Iceberg's committer recognises the restored `max-committed-checkpoint-id`,
so nothing already committed is committed twice — the same two-phase-commit mechanism
§4 describes, now spanning a restart.

Two small scripts make it a single command each. `stop.sh` reads the job's latest
completed checkpoint path out of the REST API *before* cancelling (once the job is gone
the listing goes with it, though the S3 files remain) and files it under
`stream/flink/.resume/<pipeline.name>` — keyed on the name the SQL itself declares, so
the mapping cannot drift from the filename. `submit.sh --resume` passes it back as
`sql-client -D execution.savepoint.path=…`.

**A plain submit is refused while a resume point exists.** This is the part that
matters: replaying into a populated table produces no error, no warning and no symptom
until someone runs a `COUNT` days later. Making the safe path the default one and the
destructive path explicit (`stop.sh --forget`, or `create_tables.py --recreate`) is
worth more here than any amount of documentation.

**Rejected: stop-with-savepoint.** It is the more standard-looking answer and it is
strictly weaker here. A savepoint is produced by a *graceful* stop, so it covers exactly
the case that was already easy and does nothing for a crash, an OOM kill, or a
`docker kill` — the failure this pipeline was explicitly tested against in §4. Retained
checkpoints cover the graceful stop *and* the crash with one config line and no
savepoint-triggering, polling, or request-id plumbing. Savepoints earn their keep when
state has to survive a change of parallelism, state backend or Flink version; none of
which is what "resume the job I just stopped" needs.

**Consequences, stated plainly.**

- Retained checkpoints are **not garbage-collected**. `state.checkpoints.num-retained`
  bounds a *running* job's checkpoints, but the one left behind by a cancelled job is
  nobody's responsibility. They are small (~50 KB here), they accumulate one per stop,
  and clearing them is `mc rm --recursive --force lake/lake/checkpoints/<job-id>`.
- Resume requires the **SQL to be unchanged**. Flink SQL derives operator IDs from the
  plan, so editing a job's query and resuming it fails with a state-mapping error. That
  is the correct behaviour — the alternative silently restores state that no longer
  means what it did — but it means a query change is a reset, and a reset means
  `create_tables.py --recreate`.
- A job stopped inside its first checkpoint interval has no resume point, and needs
  none: it committed nothing to Iceberg, so starting it over is exactly right.
  `stop.sh` says so rather than writing an empty file.

---

## Verification log — 2026-08-28

Run against a reset warehouse (`create_tables.py --recreate`) and a freshly created
3-partition topic. Reproduce with `make flink-jobs`, `make traffic`, then
`make flink-stop && make flink-verify`.

**Traffic.** 1305 spans emitted through the real SDK across three runs (600 burst, 122
single-partition trickle, 283 during the failure test). Ground truth is the topic
itself:

```
$ kafka-get-offsets.sh --topic traces.events.v1
traces.events.v1:0:491
traces.events.v1:1:408
traces.events.v1:2:406      -> 1305
```

**Raw sink, and exactly-once.** `stream/flink/verify/01_raw_count.sql`:

```
| row_count | distinct_span_ids | duplicates |                   earliest |                     latest |
+-----------+-------------------+------------+----------------------------+----------------------------+
|      1305 |              1305 |          0 | 2026-08-28 16:47:03.880000 | 2026-08-28 17:28:03.164000 |
```

1305 rows against 1305 events on the topic, and zero duplicates — and that is *across a
TaskManager kill*. `docker kill agentlake-flink-tm` at 17:26:49 with a producer running;
the container was back at 17:20:43 on the earlier attempt and both jobs recovered on
this one, the raw sink reporting `restored from checkpoint id: 2`, 9 checkpoints
completed and 2 failed (the two that were in flight during the outage). No gap, no
duplicate. `span_id` is a full uuid4 hex per ADR-000 §4, which is what makes
`COUNT(*) - COUNT(DISTINCT span_id)` a valid duplicate detector.

**Hidden partitioning.** The warehouse lays the table out by the transform, not by a
column:

```
$ mc ls -r lake/lake/raw/trace_events/
32KiB  data/ts_day=2026-08-28/00000-0-c6dd6116-…-00001.parquet
```

**Three sample rows** (`02_raw_sample.sql`), all 13 contract fields, abridged:

```
| event_type | model  | latency_ms | status |                ts_epoch_ms | attributes                     |
|  TOOL_CALL | <NULL> |      1.659 |  error | 2026-08-28 17:26:05.502000 | {error_message=orders_api d... |
| AGENT_STEP | <NULL> |    137.785 |  error | 2026-08-28 17:26:05.502000 | {error_message=orders_api d... |
|  RETRIEVAL | <NULL> |      3.185 |     ok | 2026-08-28 17:26:06.841000 | {hits=3, name=vector_search... |
```

The first two share a trace and the TOOL_CALL carries the AGENT_STEP as its
`parent_span_id`: nesting, the error path and the attributes map all survive the trip.

**Aggregates** (`03_agg.sql`) — 22 rows across 5 closed windows. One window:

```
| window_start        | event_type | model            | event_count | error_count | prompt_tokens_sum | cost_usd_sum |
| 2026-08-28 16:45:00 |   LLM_CALL | claude-haiku-4-5 |         103 |           0 |             31246 |     0.080691 |
| 2026-08-28 16:45:00 |   LLM_CALL |  claude-sonnet-5 |          83 |           0 |             24719 |     0.064604 |
| 2026-08-28 16:45:00 |  RETRIEVAL |           <NULL> |         186 |           0 |            <NULL> |       <NULL> |
| 2026-08-28 16:45:00 |  TOOL_CALL |           <NULL> |          21 |          21 |            <NULL> |       <NULL> |
```

Reads correctly on inspection: 103 + 83 = 186 LLM_CALLs against 186 RETRIEVALs, which
is right because the generator emits exactly one of each per successful turn; token and
cost sums are NULL for every non-LLM event type; `error_count == event_count` for
TOOL_CALL, because every TOOL_CALL the generator emits raises. The NULL `model` group
is real, not a defect — it is how "what did tool use cost us" stays answerable.

**Aggregates against raw** (`04_agg_vs_raw.sql`) recomputes every window's
`event_count` straight from `lake.raw.trace_events`, joining on each window's own
`[window_start, window_end)` range rather than re-deriving TUMBLE's floor. **All 22
rows matched:**

```
|               window_start | event_type |            model | streamed_count | recomputed_count | match |
+----------------------------+------------+------------------+----------------+------------------+-------+
| 2026-08-28 16:45:00.000000 | AGENT_STEP |           <NULL> |            207 |              207 |  TRUE |
| 2026-08-28 16:45:00.000000 |   LLM_CALL | claude-haiku-4-5 |            103 |              103 |  TRUE |
| 2026-08-28 16:45:00.000000 |   LLM_CALL |  claude-sonnet-5 |             83 |               83 |  TRUE |
| 2026-08-28 16:45:00.000000 |  RETRIEVAL |           <NULL> |            186 |              186 |  TRUE |
| 2026-08-28 16:45:00.000000 |  TOOL_CALL |           <NULL> |             21 |               21 |  TRUE |
| …                          |            |                  |                |                  |       |
| 2026-08-28 17:15:00.000000 |  TOOL_CALL |           <NULL> |              7 |                7 |  TRUE |
22 rows in set (16.38 seconds)
```

The streaming aggregate and a batch recomputation over the raw table agree on every
window, every event type and every model — including the NULL-model groups. No late
event was dropped during this run.

**Stop and resume** (§11), run against a reset warehouse with 1606 events already on
the topic. Submit both jobs, let them drain, `stop.sh`, produce 302 more *while the
jobs are down*, `submit.sh --resume`. Iceberg's own snapshot history is the cleanest
possible record of what happened:

```
time (UTC)   added   total  operation
  21:49:54     982     982  append
  21:50:18     624    1606  append     <- backlog committed; jobs stopped at 21:51
  21:55:27     302    1908  append     <- after --resume
```

`stop.sh` recorded `s3://lake/checkpoints/75499a…/chk-4`, and the resumed job — a new
job id — reported `restored from: s3://lake/checkpoints/75499a…/chk-4`, i.e. from the
*previous* job's retained checkpoint. The resume added **exactly 302 rows**: not 1908
(a replay from earliest) and not 0 (a gap). Final state:

```
| row_count | distinct_span_ids | duplicates |
|      1908 |              1908 |          0 |
```

1908 rows against 1908 events on the topic, no duplicates, and the 302 events produced
while nothing was consuming were picked up on restart. The guard was checked too: a
plain `submit.sh 01_raw_sink.sql` with a resume point present exits 1 and prints the
recorded path rather than replaying.

**Watermarks and idleness.** Covered in §3 with timings. Short version: windows closed
25 seconds after traffic resumed on a single partition while the other two were idle;
and with all three idle the watermark froze and the last window stayed open, which is
the honest limit of what idleness does.

**Not covered.** `lake.curated.agg_model_5m` holds no row for the final window
(`[17:25, 17:30)`) even though raw has events at 17:28 — that window had not closed
when the jobs were cancelled, per §3. It is not lost data; it is an unclosed window.

