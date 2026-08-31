-- Job 2 -- event-time 5-minute tumbling aggregates by (event_type, model)
-- into lake.curated.agg_model_5m.
--
--     ./stream/flink/submit.sh 02_agg_model_5m.sql
--
-- Deliberately no latency_p95. Flink 1.20's SQL dialect has no percentile
-- aggregate at all -- COUNT/AVG/SUM/MAX/MIN/STDDEV*/VAR*/COLLECT is the whole
-- list; PERCENTILE arrived in Flink 2.0 (FLINK-36123) and APPROX_PERCENTILE has
-- never existed in Flink. Percentiles are computed downstream from
-- lake.raw.trace_events, which holds every individual latency_ms. The sums and
-- max below are what this window can honestly produce. See ADR-004 #6.
--
-- Also honest about lateness: a row arriving after the watermark has passed its
-- window end is DROPPED here, silently, by Flink. Window TVF aggregation has no
-- side output in SQL. See ADR-004 #5 for the gap and what would close it.

SET 'pipeline.name' = 'agentlake-agg-model-5m';

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- Keep the last checkpoint when the job is cancelled, so the job can be
-- resumed instead of replayed. Without it, stopping the job to free a task
-- slot means the next start reads from earliest-offset into a table that
-- already holds those rows. This is a JOB-level option, which is why it lives
-- here beside the other execution.checkpointing.* settings and not in
-- docker-compose.yml: the JobGraph is built by whoever submits it -- the SQL
-- client -- so cluster config on the JobManager does not reach it. Setting it
-- there looked right and silently retained nothing. See ADR-004 #11.
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.source.idle-timeout' = '60s';

CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://lake/',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true'
);

-- Identical to job 1's source apart from the consumer group: two independent
-- jobs reading the same topic, each with its own offsets and its own state.
CREATE TEMPORARY TABLE kafka_trace_events (
    trace_id          STRING       NOT NULL,
    span_id           STRING       NOT NULL,
    parent_span_id    STRING,
    session_id        STRING       NOT NULL,
    event_type        STRING       NOT NULL,
    `model`           STRING,
    prompt_tokens     BIGINT,
    completion_tokens BIGINT,
    latency_ms        DOUBLE       NOT NULL,
    cost_usd          DOUBLE,
    status            STRING       NOT NULL,
    ts_epoch_ms       TIMESTAMP(3) NOT NULL,
    attributes        MAP<STRING NOT NULL, STRING NOT NULL> NOT NULL,
    WATERMARK FOR ts_epoch_ms AS ts_epoch_ms - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'traces.events.v1',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'flink-agg-model-5m-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081',
    -- The reader schema, and it has to be spelled out. Without it Flink derives
    -- one from the table columns above, which makes event_type a plain Avro
    -- `string` -- and Avro's schema resolution does not promote an enum to a
    -- string, so every record fails with "Found agentlake.v1.EventType,
    -- expecting string". Handing Flink the real contract makes reader and
    -- writer schemas identical; the enum symbol then reaches the STRING column
    -- through Flink's own toString() conversion.
    --
    -- This is contracts/trace_event_v1.avsc, minified. It is the one place in
    -- the repo where the contract is duplicated, so
    -- tests/test_stream_sql_contract.py asserts the two stay byte-identical.
    'avro-confluent.schema' = '{"type":"record","name":"TraceEvent","namespace":"agentlake.v1","fields":[{"name":"trace_id","type":"string"},{"name":"span_id","type":"string"},{"name":"parent_span_id","type":["null","string"],"default":null},{"name":"session_id","type":"string"},{"name":"event_type","type":{"type":"enum","name":"EventType","symbols":["LLM_CALL","TOOL_CALL","RETRIEVAL","AGENT_STEP","GATEWAY","ERROR"]}},{"name":"model","type":["null","string"],"default":null},{"name":"prompt_tokens","type":["null","long"],"default":null},{"name":"completion_tokens","type":["null","long"],"default":null},{"name":"latency_ms","type":"double"},{"name":"cost_usd","type":["null","double"],"default":null},{"name":"status","type":"string"},{"name":"ts_epoch_ms","type":{"type":"long","logicalType":"timestamp-millis"}},{"name":"attributes","type":{"type":"map","values":"string"},"default":{}}]}'
);

-- TUMBLE as a window table-valued function, not a legacy GROUP BY TUMBLE(...).
-- The distinction matters at the sink: a windowed TVF aggregation emits each
-- window exactly once, when the watermark passes its end, so the stream is
-- append-only. A plain GROUP BY would emit an updating stream of retractions,
-- and the Iceberg append sink rejects retractions outright.
INSERT INTO lake.curated.agg_model_5m
SELECT
    window_start,
    window_end,
    event_type,
    `model`,
    COUNT(*)                                   AS event_count,
    COUNT(*) FILTER (WHERE status = 'error')   AS error_count,
    SUM(prompt_tokens)                         AS prompt_tokens_sum,
    SUM(completion_tokens)                     AS completion_tokens_sum,
    SUM(cost_usd)                              AS cost_usd_sum,
    SUM(latency_ms)                            AS latency_sum_ms,
    MAX(latency_ms)                            AS latency_max_ms
FROM TABLE(
    TUMBLE(TABLE kafka_trace_events, DESCRIPTOR(ts_epoch_ms), INTERVAL '5' MINUTES)
)
-- model is NULL for every non-LLM span (TOOL_CALL, RETRIEVAL, AGENT_STEP), and
-- that is a real group, not a defect: it is how "how much did tool use cost us"
-- stays answerable. Flink groups NULL keys together rather than dropping them.
GROUP BY window_start, window_end, event_type, `model`;
