-- Verification 3: the closed 5-minute windows.
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
SELECT * FROM lake.curated.agg_model_5m ORDER BY window_start, event_type;
