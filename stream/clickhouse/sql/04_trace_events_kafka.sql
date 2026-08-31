-- The consumer. A Kafka engine table is a queue, not storage: it holds nothing,
-- and reading from it consumes. The materialized views in 05_*.sql and 06_*.sql
-- are what actually attach a consumer and start it running -- creating THIS
-- table starts nothing.
--
-- Column names must match the Avro field names exactly, because ClickHouse
-- binds Avro fields to columns by name (extra Avro fields are skipped; missing
-- ones error). That is why this table keeps the contract's ts_epoch_ms while
-- trace_events_rt uses ts -- see ADR-005 #8.
--
-- Note what is NOT here: a copy of the contract. ADR-004 #7 had to spell the
-- whole .avsc out as an `avro-confluent.schema` literal in both Flink jobs,
-- because Flink derived a reader schema from the column types and Avro's
-- resolution will not promote an enum to a string. ClickHouse's AvroConfluent
-- reader resolves the enum into a String column on its own, so the contract
-- lives in exactly one place on this path. One fewer drift surface.
CREATE TABLE IF NOT EXISTS agentlake.trace_events_kafka
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
    ts_epoch_ms       DateTime64(3),
    attributes        Map(String, String)
)
ENGINE = Kafka
SETTINGS
    -- The INTERNAL listener, same as stream/flink/jobs/*.sql. localhost:9092 is
    -- EXTERNAL and resolves to the wrong host from inside a container.
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'traces.events.v1',
    -- Its own group, so this consumer's offsets are independent of the two
    -- Flink jobs' (flink-raw-sink-v1, flink-agg-model-5m-v1). Unlike Flink,
    -- these offsets live in the broker rather than in checkpointed state, which
    -- is why the hot path needs no equivalent of ADR-004 #11's resume guard:
    -- a restart resumes from the committed offset by itself.
    kafka_group_name = 'clickhouse-hotpath',
    -- Confluent wire format: magic byte + 4-byte schema id + Avro payload. The
    -- format strips the prefix and resolves the id against the registry itself.
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    -- One consumer: the topic has 3 partitions, and the docs are explicit that
    -- consumers must not exceed partitions. One is also all the parallelism a
    -- 768 MiB container wants (kafka_thread_per_consumer stays at its 0
    -- default for the same reason).
    kafka_num_consumers = 1,
    -- 8192, not the 65536 default. A block is buffered in memory before it is
    -- flushed to the MergeTree, and these rows carry a Map column.
    kafka_max_block_size = 8192,
    kafka_poll_max_batch_size = 1000,
    -- This is the freshness knob. The default is stream_flush_interval_ms
    -- (7500), which would put the NFR-2 p95 target of 5s out of reach on its
    -- own. 1s is what makes emit->queryable sub-second at this volume.
    kafka_flush_interval_ms = 1000,
    -- 'default' throws on a parse failure and stalls the consumer. 'stream'
    -- surfaces it in the _error/_raw_message virtual columns instead, which is
    -- what 06_trace_events_dlq_mv.sql routes into the dead-letter table.
    kafka_handle_error_mode = 'stream'
