-- The other half of the same consumer: everything 05_*.sql filtered out.
--
-- Two views over one Kafka table share its consumer, so this costs no extra
-- Kafka connection -- and the WHERE clauses are complements, so every message
-- lands in exactly one of the two tables. That is the property worth having:
-- rows_in_rt + rows_in_dlq == messages consumed, with nothing falling between
-- them. See ADR-005 #1.
CREATE MATERIALIZED VIEW IF NOT EXISTS agentlake.trace_events_dlq_mv
TO agentlake.trace_events_dlq
AS SELECT
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    _raw_message AS raw,
    _error AS error,
    now() AS seen_at
FROM agentlake.trace_events_kafka
WHERE length(_error) > 0
