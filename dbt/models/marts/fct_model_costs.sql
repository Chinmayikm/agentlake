-- What each model cost per day, and how slow it was -- including the exact p95
-- that ADR-004 #6 promised would be computed downstream.
--
-- This is where that promise is kept for the batch side. Flink 1.20 has no
-- percentile aggregate, so lake.curated.agg_model_5m carries latency_sum_ms and
-- latency_max_ms and nothing else; ADR-005 #4 answered "what is p95 right now"
-- with ClickHouse's quantile(), which samples. Here the whole population is
-- available -- lake.raw.trace_events keeps every latency_ms -- so the answer is
-- exact. See dbt/macros/exact_percentile.sql for why that matters and where it
-- stops being affordable.
--
-- The NULL model group is real and is not filtered out. It is how "what did
-- non-LLM work cost" stays answerable; ADR-004's verification log makes the
-- same point about the streaming aggregate. Note that this model filters to
-- LLM_CALL, so in practice model is populated -- but the column stays nullable
-- because the contract says it is, and a NOT NULL here would be this layer
-- asserting something the contract does not.
--
-- Grain: one row per (event_day, model). Named event_day rather than day
-- because DAY is a keyword in Trino's interval syntax and an unquoted column
-- called day is a trap waiting for whoever writes the next query.

select
    ts_day as event_day,
    model,

    count(*) as calls,
    coalesce(sum(prompt_tokens), 0) as prompt_tokens,
    coalesce(sum(completion_tokens), 0) as completion_tokens,
    coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0) as total_tokens,
    coalesce(sum(cost_usd), 0) as cost_usd,

    avg(latency_ms) as latency_avg_ms,
    max(latency_ms) as latency_max_ms,
    {{ exact_percentile('latency_ms', 0.95) }} as latency_p95_ms,

    count(*) filter (where is_error) as error_count

from {{ ref('stg_trace_events') }}
where event_type = 'LLM_CALL'
group by ts_day, model
