-- Dead letters: messages the AvroConfluent reader could not parse.
--
-- This table exists because of ADR-004 #7. On the Flink side, an Avro enum that
-- would not resolve to a SQL STRING failed EVERY record on the topic, and the
-- only symptom was an empty sink -- the pipeline looked like it was running.
-- A hot path that drops unparseable messages silently would reproduce that
-- failure mode without the error message.
--
-- 04_trace_events_kafka.sql sets kafka_handle_error_mode = 'stream', which is
-- what makes _error and _raw_message available; 06_*.sql routes them here
-- instead of throwing. A non-empty table is a schema problem, and
-- `SELECT error, count() FROM agentlake.trace_events_dlq GROUP BY error` says
-- which one.
CREATE TABLE IF NOT EXISTS agentlake.trace_events_dlq
(
    topic     String,
    partition UInt64,
    offset    UInt64,
    raw       String,
    error     String,
    seen_at   DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(seen_at)
ORDER BY (seen_at, partition, offset)
TTL seen_at + INTERVAL 7 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
