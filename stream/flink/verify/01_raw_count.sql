-- Verification 2: does lake.raw.trace_events hold exactly what was produced?
-- Compare row_count against the count scripts/gen_traffic.py printed, and
-- distinct_span_ids against row_count -- span_id is a full uuid4 hex (ADR-000
-- #4), so any gap between the two is a duplicate.
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
    COUNT(*)                  AS row_count,
    COUNT(DISTINCT span_id)   AS distinct_span_ids,
    COUNT(*) - COUNT(DISTINCT span_id) AS duplicates,
    MIN(ts_epoch_ms)          AS earliest,
    MAX(ts_epoch_ms)          AS latest
FROM lake.`raw`.trace_events;
