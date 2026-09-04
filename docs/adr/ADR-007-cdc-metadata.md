# ADR-007: CDC — a metadata database, Debezium, and the first join

- **Status:** Accepted
- **Date:** 2026-09-04
- **Context:** `metadata/`, `scripts/cdc_land.py`, `scripts/register_connector.py`, the
  `cdc` compose profile, `lake.cdc.prompt_versions`, and the two dbt models that turn
  it into a dimension and a join.

Every path into this lake so far starts in the same place: a TraceEvent emitted by
`services/sdk`. So everything in the warehouse is a **fact** — a span that happened.
There were no **dimensions**. Nothing said which prompt template produced a turn, which
golden example an eval scored, or what a retriever was configured with. The two Grafana
panels the README ships — `Cost per turn by prompt version`, `Tokens per turn by prompt
version` — returned no rows for exactly that reason, and the README said so.

This slice adds the other half. A Postgres metadata database holds the operational
tables an eval harness will write; Debezium streams every row change out of its WAL
onto Kafka; the changelog lands in Iceberg; dbt resolves it to current state and joins
it to the trace facts. `services/agent` starts stamping `prompt_version`, and
`lake.analytics.fct_cost_by_prompt` becomes the first mart in the repo that is a join
rather than a reshaping.

**The eval harness itself is not built here.** This slice builds the tables and the
pipeline that carries them.

---

## 1. Why CDC, and not dual-writes

The alternative is one line of application code: after `INSERT INTO prompt_versions`,
also produce a message to Kafka. It needs no Postgres configuration, no connector, no
replication slot and no new container. It is the wrong answer, for a reason that is
structural rather than aesthetic.

**A dual-write is a distributed transaction that nobody wrote a coordinator for.** The
database commit and the Kafka produce are two independent operations, and every
interleaving of their failures is reachable: the row exists and the event does not (the
warehouse silently disagrees with production, forever, with no error anywhere); the
event exists and the row does not (the warehouse holds a prompt version that was rolled
back); both succeed but in the other order (a consumer that reads its own writes sees
the future). None of these throw. They are discovered months later as a number that
does not add up.

CDC removes the second write. There is exactly one durable act — the Postgres commit —
and the WAL is a record of it that Postgres was already keeping for its own recovery.
Everything downstream is derived from that record rather than racing it. The log is the
source of truth, which is the same argument ADR-000 makes for putting spans on a topic
instead of writing them to a database from the request path.

**Two consequences worth stating, because they are the price.** A replication slot is
now a piece of production state that can fill a disk if nobody reads it (§5, measured),
and the pipeline has a component — Kafka Connect — that the application does not
control and that can be down while the application is fine. Both are real. Both are
visible and fixable; a dual-write's failure mode is neither.

**Rejected: outbox.** Write the event into an `outbox` table in the same transaction as
the row, and CDC *that*. It is the standard answer, it is strictly better for events
that carry domain meaning rather than row state, and it is what this would become if
the metadata tables ever needed to publish something other than "this row changed". It
is not needed here: `prompt_versions` **is** the state a consumer wants, so an outbox
would be a hand-maintained copy of a table Debezium can already read.

---

## 2. The version matrix, and the converter it forced

| Component | Pin |
|---|---|
| Postgres | `postgres:16.10-alpine` |
| Debezium / Kafka Connect | `quay.io/debezium/connect:3.0.8.Final` |
| Logical decoding plugin | `pgoutput` (in-tree since PG10) |
| Converter | `org.apache.kafka.connect.json.JsonConverter`, `schemas.enable=false` |

**Kafka was the input, not a conclusion**, exactly as Flink 1.20 was in ADR-004 §1.
`apache/kafka:3.8.0` is already the broker. Debezium 3.0 is built against Kafka Connect
**3.8.0** and tested against 3.8.0 brokers, and `3.0.8.Final` is the newest tag on that
line — so the connector matches the broker rather than the broker being dragged forward
to suit the connector.

**pgoutput, not decoderbufs or wal2json.** Those are extensions that must be compiled
into the Postgres image; pgoutput is Postgres' own output plugin and has been in-tree
since 10. That is why `postgres:16.10-alpine` works unmodified, and it is the same
instinct that keeps `httpx` doing the Iceberg REST calls rather than adding pyiceberg
(ADR-004 §2).

**JSON, and this is forced rather than preferred.** `traces.events.v1` is registry-
enforced Avro and the obvious thing is for the CDC topics to match. They cannot,
cheaply: **the Debezium connect image does not ship Confluent's Avro converter** — it
is under the Confluent Community License and is not Debezium's to redistribute — so
registry-backed Avro means building and pinning a second image, fetching licence-bound
jars in the cold start, and keeping that image current. For four topics that carry tens
of records describing a handful of prompt templates, that is a large amount of
machinery bought with a real maintenance cost.

**Consequence, stated plainly rather than left to be discovered:** unlike
`traces.events.v1`, the CDC topics are **not schema-checked by anything**. A column
added to `prompt_versions` changes the payload shape with no compatibility gate and no
CI failure. Three things blunt that and none of them closes it: the landing schema is
explicit in `scripts/cdc_land.py` (an unknown key is ignored, not crashed on),
`tests/test_cdc.py` asserts that schema against `dbt/models/sources.yml` so the two
cannot drift, and the whole envelope is landed as `envelope_json` so a column added
today can be back-filled tomorrow without a re-snapshot. The gap is real and it is the
first thing to close if these tables ever get more than one writer.

`schemas.enable=false` because the schema-carrying form wraps every field in a
`payload`/`schema` envelope that roughly triples the record and tells the lander
nothing it does not already know.

**Two more converter settings that are not defaults and had to be reasoned about.**
`decimal.handling.mode=string`, because `numeric(12,6)` otherwise arrives as a
base64-encoded unscaled value plus a scale — correct, lossless, and unreadable by any
consumer that has not been told. And `time.precision.mode=connect`, which makes every
non-zoned temporal type a millisecond integer instead of `adaptive`'s microseconds.
§7 records the part of that which is still a trap.

---

## 3. The landing path: a batch pull, not a third Flink job

**Decision.** `scripts/cdc_land.py` — a `confluent-kafka` consumer (the same library
`services/sdk` produces with) that batches records into `INSERT INTO … VALUES`
statements through `analytics/trino_client.py` (the same client
`scripts/seed_iceberg.py` writes through). Offsets live in the consumer group
`agentlake-cdc-land`, committed **after** the Trino insert returns.

**Rejected: a Flink SQL job on the existing cluster.** It is the reflex answer in a
repo that already runs two of them, and it is wrong here for two reasons that are
independent of taste.

The first is **mechanical**. A Debezium changelog is an *updating* stream. ADR-004 §2's
Iceberg sink is append-only — that is not a configuration, it is why the streamed
aggregate can be committed at all (ADR-006 §4). Landing a changelog through Flink
therefore means enabling Iceberg upsert mode with v2 equality deletes: a new file
format concern, a new compaction concern, and a mechanism nothing else in this
warehouse uses, introduced to maintain a table of tens of rows.

The second is **the box**, and it is measured rather than estimated. The `streaming`
profile is 1,790 MiB (ADR-004 §8) and `cdc` adds ~535. That is 2,325 MiB before Trino,
and dbt needs Trino, which §8 measures at up to 1,001 MiB on its own. On a 3.9 GB
ceiling that does not fit by any arrangement. The batch pull needs kafka + `cdc` +
`analytics` and never starts Flink at all — and even that runs in two phases (§8).

And it is **the honest shape for the data**. Prompt versions change on a human
timescale, not per span. A streaming job holding one of two task slots forever to move
a handful of rows a week is paying a streaming price for a batch question, and it would
have made the cold path's slot contention (ADR-004 §7) permanently worse.

**The trade, stated rather than hidden.** Freshness is "whenever you run
`make cdc-land`", not thirty seconds. And the pull is **at-least-once**, not
exactly-once: the offset commit follows the insert, so a crash between them replays the
batch. That order is deliberate — commit-then-insert would be at-most-once, which is
silent data loss, and no amount of downstream dedup repairs a record that was never
written. §4 is why the duplicates it can produce are harmless.

### `lake.cdc`, a third namespace

ADR-006 §1 established one writer per table and printed an ownership table whose first
row reads "`lake.raw` — written by Flink". Putting the changelog in `lake.raw` would
have made that sentence false and turned a clean rule into a rule with a footnote. A
third namespace keeps it true:

| namespace | written by | read by |
|---|---|---|
| `lake.raw` | Flink (`01_raw_sink.sql`) | Flink verify, Trino, dbt |
| `lake.curated` | Flink (`02_agg_model_5m.sql`) | Trino, dbt |
| `lake.cdc` | **`scripts/cdc_land.py`** | Trino, dbt |
| `lake.analytics` | dbt (via Trino) | Trino, Great Expectations, Marquez |

The table is created by `cdc_land.py create-table`, which imports
`create_table_request()` and `create_if_absent()` from `stream/flink/create_tables.py`
rather than copying them — but is **not** in that module's `TABLES` list, because that
module is documented as creating the tables the *Flink jobs* write into and this is not
one.

**No partition spec, and that is a decision.** The only consumer is a latest-record
resolution, which is a full scan by definition — `ROW_NUMBER() OVER (PARTITION BY id)`
cannot prune by day. Partitioning would buy zero pruning and cost one small file per
partition per commit against a 32 MB target file size, on a table whose entire lifetime
content is kilobytes. Iceberg supports partition evolution, so `day(ingest_ts)` can be
added later without rewriting a single data file — which is what makes "none" a
decision rather than a deferral.

---

## 4. Tombstones, and what "latest record" is ordered by

### The changelog is landed, not the state

One Iceberg row per Kafka record. That is what makes the resolution auditable (§9 shows
three records for one row and the single dimension row they produce), what makes an
at-least-once puller safe, and what lets an UPDATE and a DELETE be two more rows rather
than a mutation nobody can see afterwards.

### The order, and why each key is in it

```sql
row_number() over (
    partition by id
    order by source_lsn                                desc nulls last,
             case op when 'r' then 0 else 1 end        desc,
             kafka_offset                              desc,
             ingest_ts                                 desc
)
```

- **`source_lsn`** is Postgres' own total order over WAL records — the only key that
  orders a change the way the *database* committed it, so it survives a topic recreate
  or a consumer-group reset, both of which renumber `kafka_offset`. `NULLS LAST` is
  written out rather than left to a default: a record with no LSN must lose to one that
  has an LSN, never win by accident.
- **The `op='r'` rank** exists because Debezium stamps every snapshot row with the
  *same* LSN — the snapshot's consistent point. That is measured, not assumed: §9 shows
  all three seeded rows arriving at `lsn 26723880`. A streaming change at that same LSN
  is the later fact.
- **`kafka_offset`** is per-key commit order, because Debezium keys every record by the
  row's primary key, so every record for one `id` is in one partition.
- **`ingest_ts`** is the last resort, and the only key an at-least-once replay can
  differ on.

What survives all four is exactly one class: two rows from the same `(partition,
offset)`, landed twice by a replayed batch. Those are identical in every payload
column, so which one the window function picks is **not observable** — the model's
output is deterministic even though the ordering is not total. That is precisely what
makes §3's commit-after-insert safe.

### Tombstones are counted, and deliberately not landed as `op='t'`

`tombstones.on.delete` is `true` — it is also the default, and it is set explicitly
because it is a requirement of this design rather than an inherited default. A delete
therefore emits **two** records: the `op='d'` envelope carrying the full `before` image,
then a null-valued tombstone for log compaction.

The tombstone is **skipped and counted**, not landed. It carries nothing the `op='d'`
record does not. And the obvious encoding for it — a row with `op='t'` — is actively
wrong: **Debezium already uses `op='t'` for TRUNCATE**, so a landed tombstone would
collide with a real truncate in `stg_prompt_versions`' `accepted_values` test. `op='m'`
(a logical decoding message) is skipped for the same reason. All three categories are
printed by count, which is what makes "never silently dropped" checkable rather than
asserted.

### `REPLICA IDENTITY FULL`, and why the delete would otherwise be useless

Under Postgres' default replica identity, a DELETE's WAL record carries **only the
primary key**, so Debezium's `before` image arrives with every other column NULL. The
deleted row would land with no `version`, resolve to a dimension row that joins to
nothing, and appear in `fct_cost_by_prompt` as `prompt_attribution='unknown'` — the
state that is supposed to mean *the lander has not run*. A retired prompt would look
exactly like a broken pipeline.

`ALTER TABLE prompt_versions REPLICA IDENTITY FULL` fixes it, and the cost is real:
every UPDATE and DELETE now writes the full old row to the WAL, not just the key. On a
table of a few dozen narrow rows that is free. On a wide, hot table it is not, and §10
says what changes at that scale.

**Rejected: carrying the last non-null value forward** with `last_value(…) IGNORE
NULLS` in the staging model. More SQL, and it would also mask a column that is
*genuinely* null — a repair that cannot tell a missing value from an absent one.

### Deleted rows are kept, flagged, not filtered

`stg_prompt_versions` emits `is_deleted` and keeps the row. The facts the dimension
explains are historical: a prompt retired last Tuesday still has three weeks of cost
attributed to it. Filtering it out of the dimension would turn those spans into
`'unknown'` — again, a routine event made to look like a pipeline failure. Filtering
downstream is `where not is_deleted`; un-filtering is impossible. Same asymmetry
`stg_trace_events` invokes when it keeps `attributes` whole *and* unpacks it.

---

## 5. Replication slots: what a stopped connector actually costs

This is the classic production failure mode, and it was measured rather than cited.

**The setup.** `docker compose stop connect` at 16:13:10. Five rows inserted into
`prompt_versions` while it was down. Then three bursts of 2,000 wide rows into
`wal_churn` — a table that is **not in the publication** — each followed by an explicit
`CHECKPOINT`.

```
                       active  restart_lsn   retained WAL   wal segments
connector running        t     0/197C5F0        273 kB           1
connector stopped        f     0/197C5F0        273 kB           1
+ 5 rows inserted        f     0/197C5F0        275 kB           1
+ churn burst 1          f     0/197C5F0        794 kB           1
+ churn burst 2          f     0/197C5F0       1142 kB           1
+ churn burst 3          f     0/197C5F0       1493 kB           1
```

**`restart_lsn` does not move, across three bursts and three checkpoints.** WAL that
Postgres would otherwise have recycled is pinned, and the segment count stays at 1
because nothing could be freed. The load that grew it was writes to a table the
publication does not even contain — which is the whole point: **a replication slot
retains WAL globally, not per-table.** An idle captured table on a busy database is the
worst case, not the safe one, and it is why `heartbeat.interval.ms=10000` is set: it
gives Debezium something to acknowledge on a timer so the slot can advance even when
nothing it captures has changed.

**The restart, and the finding that was not expected.**

```
                                  restart_lsn   confirmed_flush_lsn   behind restart   behind confirmed
16:13:54  connector restarted     0/197C5F0     —                        1522 kB          —
16:15:34  sample 1                0/197C5F0     0/1B0C4C0                1600 kB          608 bytes
16:15:54  sample 2                0/197C5F0     0/1B0C4C0                1601 kB         1504 bytes
16:16:15  sample 3                0/1B0C6E8     0/1B0C6E8                6424 bytes      6424 bytes
```

**`confirmed_flush_lsn` and `restart_lsn` are not the same thing, and only one of them
frees disk.** `confirmed_flush_lsn` — how far the consumer has acknowledged — caught up
within seconds of the connector reconnecting: 608 bytes behind at the first sample.
`restart_lsn`, which is what actually pins WAL, stayed frozen for a further **~2m20s**
and only then jumped to meet it, at which point retained WAL fell from 1,601 kB to
6,424 bytes.

That gap is the trap. A monitor that watches `confirmed_flush_lsn` — the more obvious
metric, and the one most "replication lag" queries reach for — reports *caught up* while
1.6 MB is still retained. At laptop scale that is nothing; at production write rates it
is the difference between an alert that fires before the disk fills and one that fires
after. `make cdc-slot` prints **both**, deliberately.

**What was not tested, stated rather than implied.** The outage was 44 seconds and the
WAL was pushed with synthetic churn. Nothing here demonstrates behaviour at a scale
where `max_slot_wal_keep_size` (unset, i.e. unbounded, in this compose file) would
actually matter — and unbounded is the setting that turns this failure mode from an
alert into an outage. §10 says what to change.

---

## 6. `prompt_version`: one attribute, three files, and the span it had to land on

The README promised these panels would populate from "one `span.set()`". That was
optimistic by two files, and the reason is worth recording.

**The LLM_CALL span is not opened in `services/agent`.** It is opened in
`services/gateway/chat.py`, on both the streaming and non-streaming paths; the agent
opens only `AGENT_STEP`. And `cost_usd`, `prompt_tokens` and `completion_tokens` exist
**only** on LLM_CALL rows — by contract, since `AGENT_STEP` has no usage to report. So
an AGENT_STEP-only attribute would have made both dashboards render a series at zero:
present, grouped correctly, and worthless.

**Decision.** `DEFAULT_PROMPT_VERSION = "v3"` in `services/agent/loop.py`; the agent
sends `X-Prompt-Version` on every gateway call; the gateway stamps it on the LLM_CALL
span. It is set on `AGENT_STEP` too, so a turn is self-describing — and that costs
nothing in the panels, because both spans share a `trace_id` and the denominator is
`uniqExact(trace_id)`.

**A header, not a `ChatRequest` field.** This is telemetry metadata about the *caller*,
exactly like the `X-Session-Id` / `X-Trace-Id` / `X-Parent-Span-Id` it sits beside
(ADR-003 §4). Putting it in the body would change `/v1/chat`'s public contract and
imply the gateway does something with it, when all it does is stamp a span it already
opens. The precedent is `price_table_version`, set the same way in the same place
(ADR-001 §2).

**Stamped at span-open, not in `record_usage()`.** `record_usage` is never reached when
the provider call raises — so stamping there would attribute a prompt version's
successes and leave its failures anonymous, which is precisely backwards for the
question "did v4 make things worse". A test covers exactly this: a 429 still emits an
LLM_CALL span carrying `prompt_version` and `status='error'`.

`_coerce_attrs` drops a `None`, so a caller that sends no header emits a span
byte-identical to before this existed — asserted, because the Iceberg `attributes` map
is `value-required` and a null-valued entry is a shape the contract says cannot exist.

**No Avro contract change.** `attributes` is the extension point ADR-004 and ADR-006
designed for; `contracts/trace_event_v1.avsc` is untouched, so the BACKWARD-compatibility
gate is not involved.

### `fct_cost_by_prompt`, and the two ways a join lies

Grain: one row per `(event_day, prompt_version)`.

**Aggregate first, join second.** The facts are grouped in a CTE *before* the dimension
is touched, so a dimension that fanned out could add mart rows but could never inflate
`calls`, `cost_usd` or tokens. That is the property that makes
`assert_marts_reconcile_with_staging.sql` hold no matter what the dimension does — and
that test gained a fourth arm here precisely because this is the mart that most needs
it.

**LEFT JOIN, and it has to be.** An inner join would silently drop every LLM_CALL span
carrying no `prompt_version` — 869 rows of history at the time of writing. The mart
would stay internally consistent, pass every column test, and simply be about the wrong
rows. A right or full join is wrong in the other direction: it would invent a row for a
prompt version nobody ever called, and a dimension row is not a cost fact.

**NULL `prompt_version` is a real group, not a sentinel.** Trino's `GROUP BY` treats
NULL as a group, so those spans collapse to one row per day instead of disappearing —
the same decision `fct_model_costs` records for its NULL `model` group. A sentinel like
`'(unattributed)'` would be a value this layer invented that nothing upstream can
produce.

**`prompt_attribution` names the three states**, so they are never confused:

| value | meaning |
|---|---|
| `unversioned` | the span carries no `prompt_version`. Normal for everything older than this change. |
| `known` | the dimension holds it. `prompt_is_deleted` then says whether it has since been retired — the join still matches, because the cost was real. |
| `unknown` | the span named a version the dimension has never held. Reachable between a row being created in Postgres and `make cdc-land` next running — which is why it **warns** and does not gate. |

That last row is ADR-006 §6's rule applied unchanged: WARN is for "depends on when
something last ran". The share of rows that are `unversioned` is deliberately **not**
gated either — it only falls as new traffic arrives, so any threshold on it measures how
recently someone ran traffic.

---

## 7. Things that had to be discovered by running it

**`time.precision.mode=connect` does not cover `timestamptz`.** It makes every
*non*-zoned temporal type a millisecond integer, and that is what its documentation
says. Postgres' *zoned* type maps to Debezium's `ZonedTimestamp` semantic type, which
is an ISO-8601 string regardless of the precision mode. `created_at` is a `timestamptz`,
so it arrives as `"2026-09-04T16:07:56.402020Z"` while a plain `timestamp` column added
to the same table tomorrow would arrive as `1788538076402`. With
`schemas.enable=false` there is no type information in the payload to disambiguate
them. `parse_timestamp()` handles both and says why, because guessing wrong is not an
error — it is a column of nonsense timestamps that nothing raises about.

**A folded YAML scalar splits a shell command in half.** `metadata-init`'s entrypoint
is a `>` block, and `>` folds *equally indented* lines into spaces but preserves a
*more* indented line's newline literally. The naturally-formatted version put a newline
inside the `psql` invocation. Caught by rendering `docker compose config` and reading
the string back rather than by running it, which would have failed with a confusing
`psql: option requires an argument`.

**psql does not interpolate variables inside dollar-quoted strings.** The `debezium`
role's password is passed in as `-v dbz_password=…`, and the natural place for it — a
`DO $$ … EXECUTE format(…) $$` block, next to the `CREATE ROLE` guard that needs one —
would have sent the literal text `:'dbz_password'` as the password. `ALTER ROLE … WITH
PASSWORD :'dbz_password'` as a plain statement is what works.

**`CREATE PUBLICATION` has no `IF NOT EXISTS`** before Postgres 18, and `CREATE ROLE`
has none at all. Both need `DO` blocks with `pg_publication` / `pg_roles` guards,
because `metadata-init` runs on every bring-up.

**`ON CONFLICT DO NOTHING` with no unique constraint is a silent no-op.** The seed's
`golden_examples` insert had one and no constraint to conflict on, so re-applying would
have inserted the set again every bring-up. It needed a real unique index on
`(question, dataset_version)` — the clause reads like a guard and is not one until
something can actually conflict.

**A checkpoint-count monitor that greps the wrong JSON shape reports zero forever.**
Five minutes were spent believing the resumed Flink jobs were not checkpointing; the
REST payload nests them under `counts.completed`, not `completed.count`. The jobs had
completed 11 checkpoints throughout. Worth recording only because the failure looked
exactly like a broken pipeline and was a broken *observer* — which is the same class of
mistake as watching `confirmed_flush_lsn` in §5.

---

## 8. Memory: why capture and landing are two phases

Measured resident, three configurations.

**Phase 1 — capture, beside the spine:**

| | resident | limit |
|---|---|---|
| kafka | 542 MiB | 1024 MiB |
| connect | 478 MiB | 640 MiB |
| schema-registry | 222 MiB | 512 MiB |
| metadata-db | 57 MiB | 256 MiB |
| **total containers** | **1,298 MiB** | |
| host | 2,831 MiB of 3,916 | |

**Phase 2 — landing and modelling, capture stopped:**

| | resident | limit |
|---|---|---|
| trino | 1,001 MiB | 1024 MiB |
| kafka | 409 MiB | 1024 MiB |
| iceberg-rest | 189 MiB | 384 MiB |
| schema-registry | 167 MiB | 512 MiB |
| minio | 73 MiB | 256 MiB |
| **total containers** | **1,838 MiB** | |
| host | 3,082 MiB of 3,916, 834 available | |

**Everything at once, for the record:** 2,150 MiB across seven containers, host 3,279
of 3,916 with **636 MiB available** — before `docker compose run --rm dbt` adds its
toolbox. That is why the runbook is two phases and not a preference. **Kafka holds the
topic between them**, so stopping `connect` and `metadata-db` before
`make analytics-up` loses nothing at all; §9's verification did exactly that, twice.

Note trino at **1,001 MiB against a 1,024 MiB limit** after a session of heavy queries
— ADR-006 §8 sized its heap to fit and it is sitting right at the ceiling. Nothing
failed, but it is the container with no headroom left, and it is the reason `cdc` was
never going to fit beside `streaming` *and* `analytics`.

**Connect's heap is pinned** (`KAFKA_HEAP_OPTS=-Xms128m -Xmx384m`), the same argument
ADR-004 §8 makes for Flink and the compose file makes for kafka-ui: Connect's own
default is 1g, which under a 640m limit is an OOM kill during snapshot rather than
backpressure.

---

## 9. Verification log — 2026-09-04

Reproduce with `make cdc-up`, `make cdc-connector`, `make cdc-topic`, then
`make analytics-up`, `make cdc-table`, `make cdc-land`, `make dbt-build`, `make quality`,
`make analytics-verify`.

### 0. The schema, the role, and the publication

```
$ make cdc-up          # 7 migrations applied in filename order, then connect
applying 01_role_debezium.sql … applying 07_seed.sql
INSERT 0 3 / INSERT 0 3
metadata schema ready

 wal_level | rolname  | rolreplication | rolsuper |    pubname    | puballtables
-----------+----------+----------------+----------+---------------+--------------
 logical   | debezium | t              | f        | agentlake_cdc | f
```

`wal_level=logical` from first boot, a replication role that is **not** superuser, and
a publication that is **not** `puballtables` — the three things §2 argued for, asserted
rather than assumed.

```
$ make cdc-connector
created  agentlake-metadata
connector  agentlake-metadata  RUNNING
task 0     RUNNING
```

The topics that appear are `cdc.metadata.prompt_versions` and
`cdc.metadata.golden_examples` — **not** `cdc.metadata.public.*`, so the `RegexRouter`
did its job. The two empty tables produce no topic, because Debezium creates one on the
first record it captures.

### 1. Row journey — INSERT a v4 and follow it to the mart

`INSERT INTO prompt_versions … VALUES ('agent-system','v4', …)` through `make cdc-psql`
lands as `id = 12`, and reaches Kafka as offset 8:

```
$ make cdc-topic
  off part  op  snapshot                        lsn   id version    payload
    8    0  c   false                      28359328   12 v4         You are agentlake's documentation assistant.
```

```
$ make cdc-land
landed 10 row(s) into lake.cdc.prompt_versions
  op=c  6      op=r  3      op=u  1
skipped 0 records
committed through offset {0: 9}
```

and after `make dbt-build`, with three real agent turns behind it (§4 below), the mart
carries it as a **join**, not a string:

```
event_day    version  attribution  prompt_name    deleted  calls turns  cost_usd  per_turn   tokens
2026-08-28   NULL     unversioned  NULL           NULL       592   592  0.471262  0.000796   236234
2026-08-31   NULL     unversioned  NULL           NULL         2     1  0.005715  0.005715     3743
2026-09-02   NULL     unversioned  NULL           NULL       275   275  0.222431  0.000809   112215
2026-09-04   v3       known        agent-system   false       12     2  0.128048  0.064024   118584
2026-09-04   v4       known        agent-system   false        8     1  0.155380  0.155380   149476
```

Postgres → WAL → Kafka → Iceberg → dimension → join, end to end. And the 869
pre-existing LLM_CALL spans are still there as `unversioned` — **nothing was dropped to
make the new rows look tidy**, which is what the LEFT JOIN and the NULL group are for.

### 2. UPDATE, then DELETE — resolution correct both times

An `UPDATE … WHERE version='v2'`, landed and rebuilt; then a `DELETE`, landed and
rebuilt. The topic gained **exactly two** records for the delete — the `op='d'` and its
tombstone:

```
cdc.metadata.prompt_versions:0:10   ->   :0:12
```

```
$ make cdc-land
landed 1 row(s) into lake.cdc.prompt_versions
  op=d  1
skipped 1 record(s), by kind:
  tombstone  1
```

The changelog holds three records for `id=2`; the dimension holds one row:

```
lake.cdc.prompt_versions, id=2
  offset  1   op=r  lsn=26723880  You are a helpful assistant with access to tools. Use search_docs before …
  offset  9   op=u  lsn=28361920  You are a helpful assistant with access to tools. UPDATED: always cite sources.
  offset 10   op=d  lsn=28563144  You are a helpful assistant with access to tools. UPDATED: always cite sources.

lake.analytics.stg_prompt_versions, id=2
  version=v2  last_op=d  is_deleted=true  last_kafka_offset=10
  template_text = "…UPDATED: always cite sources."
```

Three things this shows at once. The **update** resolved (the row carries the new text,
not the snapshot's). The **delete** resolved (`is_deleted=true`). And the delete's
before-image carries the **post-update** text — which is `REPLICA IDENTITY FULL` doing
exactly what §4 bought it for; under the default identity `version` would have been
NULL here and the row would have stopped joining.

```
changelog_rows  dimension_rows  deleted_rows
            11               9             1
```

The log grows, the dimension does not. That is the whole point of landing the log.

### 3. Resilience — stop the connector, insert, restart

`docker compose stop connect` at 16:13:10; five rows inserted while it was down;
restarted at 16:13:54 (a **44-second** outage).

```
before:  cdc.metadata.prompt_versions:0:3
after:   cdc.metadata.prompt_versions:0:8
```

Exactly five records, and read back off the topic they are in insert order with
strictly increasing LSNs — nothing lost, nothing reordered, nothing duplicated:

```
  off  op  snapshot   lsn         id  version
    3  c   false      27003432     4  p1
    4  c   false      27004936     5  p2
    5  c   false      27005168     6  p3
    6  c   false      27005400     7  p4
    7  c   false      27005632     8  p5
```

The slot behaviour across that window — retained WAL climbing to 1,493 kB with
`restart_lsn` frozen through three `CHECKPOINT`s, and `confirmed_flush_lsn` catching up
~2m20s before `restart_lsn` did — is the substance of §5 and is not repeated here.

The **resume** was checked from the other side too. The second `make cdc-land` (§2)
read only offsets 10–11, not 0–11: the consumer group's committed offset is the resume
point, and re-running the lander is a no-op rather than a duplicate-generator.

### 4. End-to-end payoff — the panels' first real rows

`prompt_version` before this slice, on a hot path holding 1,560 rows:

```
SELECT count(*), countIf(attributes['prompt_version'] != '') FROM agentlake.trace_events_rt
1560    0
```

Three real agent turns through the gateway against the live provider — two at the
default `v3`, one at `--prompt-version v4`, the row CDC had just delivered:

```
[5 steps, tools: 4x search_docs,  51580 tok, $0.0558, prompt v3, trace cf0f8459…]
[7 steps, tools: 8x search_docs,  67004 tok, $0.0722, prompt v3, trace e9d15c6e…]
[8 steps, tools: 11x search_docs, 149476 tok, $0.1554, prompt v4, trace 215d1430…]
```

and after:

```
prompt_version  event_type   spans  turns   cost_usd   tokens
v3              AGENT_STEP       2      2       NULL     NULL
v3              LLM_CALL        12      2   0.128048   118584
v4              AGENT_STEP       1      1       NULL     NULL
v4              LLM_CALL         8      1   0.155380   149476
```

**The NULL rows are the argument of §6, visible.** AGENT_STEP carries the version and
no cost; if that had been the only span stamped, both panels would have plotted zero.

Timed through the panel SQL read out of `dashboards/json/` — so this is the dashboards'
own queries, not a hand-written approximation:

```
  ok      417.7 ms     4 rows out    quality/Cost per turn by prompt version
  ok       58.7 ms     4 rows out    quality/Tokens per turn by prompt version
slowest   417.7 ms
PASS      NFR-5 target is every panel < 1000 ms
```

**Four rows where there were zero**, with no dashboard change — which is the promise
the README made. The arithmetic checks by hand: v3 is ($0.0558 + $0.0722) / 2 turns =
**$0.064024/turn**; v4 is $0.1554 / 1 = **$0.155380/turn**.

**Screenshot: not captured.** This environment has no browser and Grafana's `/render`
endpoint returns 500 without the image-renderer plugin (~400 MiB nobody else needs).
The panels are populated and the numbers above are the numbers they plot;
`http://localhost:3000` renders them in one click. This is the one deliverable of this
slice that is stated rather than shown.

### 5. The lake agrees with the hot path

The three turns were then landed into Iceberg by resuming the Flink jobs
(`make flink-resume`, 11 checkpoints, `restored from checkpoint`):

```
lake.raw.trace_events   2829 rows  ->  2918 rows      (+89, the three turns' spans)
```

and `fct_cost_by_prompt` reports **$0.064024** and **$0.155380** per turn — *identical*
to the ClickHouse panel above, to the digit. Two engines, two entirely separate paths
(Kafka → Flink → Iceberg → Trino, and Kafka → ClickHouse), one set of numbers. That was
not engineered; it is what a shared contract buys.

### 6. The gates

```
$ make dbt-build
7 table models, 71 data tests
Done. PASS=78 WARN=0 ERROR=0 SKIP=0 TOTAL=78

$ make quality
33 expectations: 33 ok, 0 blocking failures, 0 warnings
PASS -- every blocking expectation held.

$ make analytics-verify
marts vs staging
mart                  rows_  spans_accounted  staging_spans
fct_sessions          99     2918             2918
fct_tool_reliability  4      129              129
fct_cost_by_prompt    5      889              889
fct_model_costs       6      889              889

fct_cost_by_prompt by attribution
prompt_attribution  mart_rows  llm_calls  turns  cost_usd
known               2          20         3      0.283428
unversioned         3          869        868    0.699408

cdc changelog vs resolved dimension
changelog_rows  dimension_rows  deleted_rows
11              9               1

MATCH  fct_sessions accounts for 2918 spans; lake.raw.trace_events holds 2918
```

`sum(calls) = 889` against a staging LLM_CALL count of 889 is the load-bearing line:
**the join added no rows and dropped none**, which is what §6's aggregate-first
structure exists to guarantee and what the fourth arm of
`assert_marts_reconcile_with_staging.sql` now checks on every build.

**Counted, not claimed:** 68 blocking dbt tests + 30 blocking GE expectations = **98
blocking checks**, up from ADR-006's 74. Three dbt tests and three expectations are
severity `warn`, each for a reason written next to it.

### 7. The CI fixture

`scripts/cdc_land.py seed` was rehearsed against a scratch table
(`--table lake.cdc.seed_probe`) so its SQL generation could be verified without
touching real data:

```
seeded 5 changelog record(s)
resolved:  id=1 v1 last_op=d is_deleted=True
           id=2 v2 last_op=u is_deleted=False
           id=3 v3 last_op=r is_deleted=False
```

Three snapshot reads sharing one LSN, one update and one delete — the shapes the real
connector produces, including the tie the `op='r'` rank exists to break. The guard was
checked from the other side too: a second `seed` against the populated table exits 1
and refuses.

### 8. Tests and lint

```
$ python -m pytest -q
335 passed, 1 deselected in 7.28s        (32 new: tests/test_cdc.py + prompt_version)

$ ruff check services/ tests/ stream/ scripts/ analytics/ quality/ metadata/
All checks passed!
```

---

## 10. CI posture, and what Part-2 scale changes

**CI runs neither Postgres nor Debezium.** The `quality` job fits precisely *because*
it excludes Kafka (ADR-006 §10), and Debezium needs a broker. What CI covers is the
modelling and the gates: `scripts/cdc_land.py create-table` and `… seed` put a
realistic changelog into Iceberg, `scripts/seed_iceberg.py` stamps matching
`prompt_version` attributes on its LLM_CALL spans, and the join is therefore actually
executed rather than merely compiled. Without that second seed the mart would be 100%
`unversioned`, every test would pass, and the join — the point of the mart — would never
run. **Capture is not covered**, and §9 is what covers it instead.

One state CI does not reach: `prompt_attribution='unknown'`, because the seeded versions
match on both sides by construction. It is a warn-severity condition, and exercising it
would mean seeding a deliberate mismatch that a future reader would reasonably try to
"fix".

**What Part-2 scale changes.** Five things, in the order they would bite. **The
replication slot becomes an SLO, not a footnote** — `max_slot_wal_keep_size` is unset
here, i.e. unbounded, which is exactly what turns §5's measured 1.5 MB into a full disk
at production write rates; bounding it (and alerting on `restart_lsn` lag, not
`confirmed_flush_lsn`) is the first change, and it trades an outage for a dropped slot
that must then be re-snapshotted. **Kafka Connect goes distributed** with more than one
worker, at which point the single-task Postgres connector stops being a single point of
failure for the *worker* even though it remains one for the *slot*. **The converter
becomes registry-backed Avro**, closing §2's stated gap, once a custom image is worth
maintaining. **`REPLICA IDENTITY FULL` stops being free** on any table that is wide or
hot, and the answer there is a `REPLICA IDENTITY USING INDEX` on the natural key plus a
staging model that no longer expects a full before-image. And **the batch pull becomes
a merge**: at a changelog of millions of records, `INSERT`-then-`ROW_NUMBER()` is a full
scan per build, and the answer is an incremental dbt model or an Iceberg `MERGE` keyed
on `(id, source_lsn)` — which is the point at which the Flink upsert path §3 rejected
starts being worth its complexity.

**Deliberately out of scope, recorded rather than implied.** The eval harness itself.
Landing `golden_examples`, `eval_runs` and `eval_results` — the connector captures all
four, so they are already on Kafka, and landing them is a copy of `cdc_land.py` rather
than a design. A mart over eval results. And CDC lineage into Marquez: the OpenLineage
graph still begins at `lake.raw.trace_events` and `lake.cdc.prompt_versions`, with the
Postgres → Kafka edge undeclared, for the same reason ADR-006 §7 left the Flink edge
declared rather than captured.
