-- Verification 3 (cont.): hand-check every aggregate row against raw.
--
-- Rather than recompute TUMBLE's 5-minute floor (fiddly, and a bug there would
-- look like a pipeline bug), this joins raw straight onto each window's own
-- [window_start, window_end) range. Every row should report match = TRUE.
--
-- A mismatch is one of exactly two things: a late event that the streaming
-- aggregate dropped and the raw sink kept -- the known gap in ADR-004 #5, and
-- the reason this check is worth having -- or a real defect.
-- Shared prelude for every verification query: batch mode over the Iceberg
-- tables (bounded scans that terminate and print), tableau output so results
-- are readable in a terminal.
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG lake WITH (
    'type' = 'iceberg',
    'catalog-type' = 'rest',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://lake/',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true'
);
SELECT
    a.window_start,
    a.event_type,
    a.`model`,
    MAX(a.event_count)            AS streamed_count,
    COUNT(r.span_id)              AS recomputed_count,
    MAX(a.event_count) = COUNT(r.span_id) AS `match`
FROM lake.curated.agg_model_5m a
LEFT JOIN lake.`raw`.trace_events r
       ON r.ts_epoch_ms >= a.window_start
      AND r.ts_epoch_ms <  a.window_end
      AND r.event_type   = a.event_type
      AND (r.`model` = a.`model` OR (r.`model` IS NULL AND a.`model` IS NULL))
GROUP BY a.window_start, a.window_end, a.event_type, a.`model`
ORDER BY a.window_start, a.event_type;
