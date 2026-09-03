-- How each tool behaved, per day: how often it was called, how often it failed,
-- how often it timed out specifically, and how slow it was.
--
-- Two failure columns, not one, because they answer different questions. A tool
-- that returns an error is working and saying no; a tool that times out is not
-- answering at all, and the fix is different. The distinction is available
-- because the SDK records type(exc).__name__ into attributes["error_class"]
-- (services/sdk/telemetry.py) rather than flattening every failure to
-- status='error'.
--
-- timeout is deliberately two conditions OR'd. status='timeout' is the direct
-- form -- status is a free-form contract string and set(status="timeout") is
-- legal (ADR-005 #2) -- and error_class='TimeoutError' is what actually appears
-- today, because scripts/gen_traffic.py's error turns raise TimeoutError and
-- the SDK's except-block forces status='error'. Matching only the first would
-- report a timeout rate of zero against traffic that is entirely timeouts.
--
-- Grain: one row per (event_day, tool_name).

select
    ts_day as event_day,
    tool_name,

    count(*) as calls,
    count(*) filter (where is_error) as error_count,
    count(*) filter (where status = 'timeout' or error_class = 'TimeoutError') as timeout_count,

    -- cast before dividing: both operands are BIGINT and Trino's integer
    -- division would floor every rate to 0 or 1.
    cast(count(*) filter (where is_error) as double) / count(*) as error_rate,
    cast(count(*) filter (where status = 'timeout' or error_class = 'TimeoutError') as double)
        / count(*) as timeout_rate,

    avg(latency_ms) as latency_avg_ms,
    max(latency_ms) as latency_max_ms,
    {{ exact_percentile('latency_ms', 0.95) }} as latency_p95_ms

from {{ ref('stg_trace_events') }}
where event_type = 'TOOL_CALL'
group by ts_day, tool_name
