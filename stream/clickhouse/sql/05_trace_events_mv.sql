-- Kafka queue -> hot table. Creating this is what starts consumption, which is
-- why apply order matters and why the filenames are numbered: both target
-- tables (02, 03) and the queue (04) have to exist first. `CREATE MATERIALIZED
-- VIEW ... TO` does not create its target.
--
-- To pause ingest, DETACH the Kafka table, not this view -- detaching the queue
-- preserves the committed offsets, detaching the view does not.
--
-- This is the path's one transformation step, so it is where the ts_epoch_ms ->
-- ts rename rides along with the projection (ADR-005 #8). The cold path has no
-- equivalent: stream/flink/jobs/01_raw_sink.sql is a straight column-for-column
-- INSERT ... SELECT with no boundary for a rename to attach to.
CREATE MATERIALIZED VIEW IF NOT EXISTS agentlake.trace_events_mv
TO agentlake.trace_events_rt
AS SELECT
    trace_id,
    span_id,
    parent_span_id,
    session_id,
    event_type,
    model,
    prompt_tokens,
    completion_tokens,
    latency_ms,
    cost_usd,
    status,
    ts_epoch_ms AS ts,
    attributes
FROM agentlake.trace_events_kafka
-- Unparseable messages go to trace_events_dlq via 06_*.sql instead of landing
-- here as a row of zeroes. Under kafka_handle_error_mode = 'stream' a failed
-- record still arrives, with _error set and the columns unfilled.
WHERE length(_error) = 0
