-- The hot table. Everything reads from here: both Grafana dashboards, both
-- whitelisted MCP tools, and scripts/hot_path_verify.py. Nothing writes to it
-- except 05_trace_events_mv.sql.
--
-- All 13 fields of contracts/trace_event_v1.avsc, types mapped straight across:
--
--     string                 -> String
--     ["null", X]            -> Nullable(X)
--     long                   -> Int64
--     double                 -> Float64
--     enum                   -> String
--     long/timestamp-millis  -> DateTime64(3)
--     map<string,string>     -> Map(String, String)
--
-- ts is the one renamed column: the Kafka engine table in 04_*.sql must spell
-- it ts_epoch_ms (ClickHouse binds Avro fields to columns by NAME), and the
-- materialized view -- this path's one transformation step -- carries the
-- rename along with the projection. One rename, at one boundary.
-- tests/test_hot_path_contract.py asserts it is the only one. See ADR-005 #8.
--
-- No LowCardinality anywhere, and no Enum8 for event_type. At ~50K rows both
-- are noise, and String keeps a straight comparison against the contract in
-- tests/test_hot_path_contract.py. status in particular must NOT be an enum:
-- it is a free-form contract string, and services/sdk/telemetry.py promotes
-- whatever set(status=...) is given (tests already cover "degraded").
CREATE TABLE IF NOT EXISTS agentlake.trace_events_rt
(
    trace_id          String,
    span_id           String,
    parent_span_id    Nullable(String),
    session_id        String,
    event_type        String,
    model             Nullable(String),
    prompt_tokens     Nullable(Int64),
    completion_tokens Nullable(Int64),
    latency_ms        Float64,
    cost_usd          Nullable(Float64),
    status            String,
    ts                DateTime64(3),
    attributes        Map(String, String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
-- (event_type, model, ts) is the query shape -- it is what every dashboard
-- panel and every whitelisted metric filters and groups on.
--
-- span_id is appended for a second reason: ReplacingMergeTree deduplicates on
-- the sorting key and on nothing else. The Kafka engine is at-least-once
-- (ADR-005 #2), so a rebalance can re-deliver a batch; appending span_id -- a
-- full uuid4 hex per ADR-000 #4 -- makes the key unique per span, so a
-- re-delivered row is byte-identical to the one already stored and collapses
-- on the next merge instead of accumulating. Byte-identical includes ts, so
-- every copy lands in the same toDate(ts) partition and partition-local
-- merging is sufficient.
ORDER BY (event_type, model, ts, span_id)
-- Hot means recent. Iceberg (lake.raw.trace_events, ADR-004) is the archive and
-- has no TTL; this side answers "what is happening now" over a window that fits
-- in memory on a laptop. Note ttl_only_drop_parts below: expiry drops whole
-- parts rather than rewriting them to delete rows, which pairs with the daily
-- partitioning above. Expect up to merge_with_ttl_timeout (4h default) of lag
-- before an expired part actually goes.
TTL toDateTime(ts) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1,
         -- Required because `model` is Nullable and is in the ORDER BY.
         -- Keeping the contract's nullability rather than ifNull(model, '') is
         -- what lets tests/test_hot_path_contract.py compare this DDL to the
         -- .avsc directly. NULL sorts as its own value, so the NULL-model rows
         -- (RETRIEVAL, TOOL_CALL, AGENT_STEP -- real data, see ADR-004's
         -- verification log) still deduplicate correctly.
         allow_nullable_key = 1
