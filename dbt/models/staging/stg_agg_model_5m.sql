-- The streaming aggregate, typed and given the mean it implies.
--
-- No mart reads this model, and that is deliberate rather than an oversight:
-- every mart reads stg_trace_events, because percentiles need the individual
-- values (ADR-004 #6). What this model is for is the check --
-- tests/assert_streamed_agg_does_not_overcount.sql recomputes the same windows
-- from raw and asserts the streamed counts never exceed them, which is the
-- batch-vs-streaming reconciliation stream/flink/verify/04_agg_vs_raw.sql does
-- as a one-off Flink batch job, run here on every build instead.
--
-- latency_avg_ms is the mean the 5-minute table can honestly produce: it
-- carries latency_sum_ms and event_count precisely so that the mean survives
-- even though the percentile could not.

select
    window_start,
    window_end,
    cast(window_start as date) as window_day,
    event_type,
    model,
    event_count,
    error_count,
    prompt_tokens_sum,
    completion_tokens_sum,
    cost_usd_sum,
    latency_sum_ms,
    latency_max_ms,
    case
        when event_count > 0 then latency_sum_ms / event_count
    end as latency_avg_ms

from {{ source('lake_curated', 'agg_model_5m') }}
