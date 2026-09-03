-- The streaming 5-minute aggregate must never claim more events than the raw
-- table holds for the same window.
--
-- One-directional on purpose, and the direction is the whole design of this
-- test. Equality would be wrong: ADR-004 #3 records that when every Kafka
-- partition goes idle the watermark stops and the final window never closes, so
-- lake.curated.agg_model_5m legitimately lags lake.raw.trace_events by up to one
-- open window; ADR-004 #5 records that a late event is silently dropped from
-- the aggregate while still reaching raw. Both make streamed < recomputed a
-- normal state. Neither can make streamed > recomputed. That direction is only
-- reachable if the append-only guarantee broke -- a window emitted twice, or a
-- resume that replayed committed rows (the failure ADR-004 #11's submit guard
-- exists to prevent).
--
-- This is stream/flink/verify/04_agg_vs_raw.sql's reconciliation, which is a
-- hand-run Flink batch job needing a free task slot, turned into something that
-- runs on every dbt build with the Flink cluster stopped.

with recomputed as (

    select
        a.window_start,
        a.event_type,
        a.model,
        a.event_count as streamed_count,
        (
            select count(*)
            from {{ ref('stg_trace_events') }} r
            where r.ts >= a.window_start
              and r.ts <  a.window_end
              and r.event_type = a.event_type
              -- NULL model is a real group (RETRIEVAL, TOOL_CALL, AGENT_STEP),
              -- so the join has to be null-safe or every one of those windows
              -- would recompute as zero and fail.
              and r.model is not distinct from a.model
        ) as recomputed_count
    from {{ ref('stg_agg_model_5m') }} a

)

select *
from recomputed
where streamed_count > recomputed_count
