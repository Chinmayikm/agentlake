-- Job 1 -- raw sink: traces.events.v1 -> lake.raw.trace_events, append only.
--
--     ./stream/flink/submit.sh 01_raw_sink.sql
--
-- All 13 fields of contracts/trace_event_v1.avsc pass straight through, the
-- attributes map included. The table itself is NOT created here: its
-- day(ts_epoch_ms) hidden partitioning cannot be expressed in Flink DDL, so
-- stream/flink/create_tables.py creates it against the REST catalog first and
-- this job only appends. See ADR-004 #2.

SET 'pipeline.name' = 'agentlake-raw-sink';

-- Exactly-once end to end: Kafka offsets live in checkpointed state and the
-- Iceberg sink commits its data files in the checkpoint's second phase, so a
-- committed Iceberg snapshot and the offsets that produced it are the same
-- atomic fact. Consequence: rows become visible in Iceberg once per interval,
-- not continuously. See ADR-004 #4.
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

-- traces.events.v1 has more partitions than there is traffic to fill them. A
-- partition that goes quiet holds its watermark at the last event it saw, and
-- the operator watermark is the minimum across partitions -- so one idle
-- partition stalls every window. 60s marks a silent partition idle and drops
-- it out of that minimum. It does nothing for job 1 (no windows here), but the
-- source DDL below is shared with job 2 and the setting travels with it.
SET 'table.exec.source.idle-timeout' = '60s';

CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://lake/',
    's3.endpoint' = 'http://minio:9000',
    -- MinIO serves buckets as a path (minio:9000/lake), not as a virtual host
    -- (lake.minio:9000), which is what the AWS SDK would otherwise assume.
    's3.path-style-access' = 'true'
);

-- Column order, types and nullability mirror the Avro record exactly: the
-- avro-confluent format converts this row type into the reader schema it
-- resolves the registry's writer schema against. A nullable column here is an
-- ["null", X] union there.
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
    -- Avro long/timestamp-millis. The contract name is kept even though the
    -- Flink/Iceberg type is a timestamp: the column IS the contract field.
    ts_epoch_ms       TIMESTAMP(3) NOT NULL,
    attributes        MAP<STRING NOT NULL, STRING NOT NULL> NOT NULL,
    -- 30s of bounded out-of-orderness. The SDK stamps ts_epoch_ms in-process at
    -- span open and emits from the span's finally, so the skew this absorbs is
    -- span duration plus producer batching, not clock drift.
    WATERMARK FOR ts_epoch_ms AS ts_epoch_ms - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'traces.events.v1',
    -- The INTERNAL listener. localhost:9092 is the EXTERNAL one and resolves to
    -- the wrong host from inside a container.
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'flink-raw-sink-v1',
    -- Offsets come from checkpointed state, not from the broker's committed
    -- offsets; this only decides where a job with no state starts.
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

-- Backticks are not decoration: `raw` is a reserved word in Flink SQL (the RAW
-- type) and `model` collides with the ML model syntax, so both need quoting.
-- The names stay as they are -- raw/curated is the layer vocabulary the rest of
-- the lakehouse will use, and Trino/dbt have no such conflict.
INSERT INTO lake.`raw`.trace_events
SELECT
    trace_id,
    span_id,
    parent_span_id,
    session_id,
    event_type,
    `model`,
    prompt_tokens,
    completion_tokens,
    latency_ms,
    cost_usd,
    status,
    ts_epoch_ms,
    attributes
FROM kafka_trace_events;
