-- The 95th percentile of a set cannot be below its mean by more than the mean's
-- own skew allows -- and for the nearest-rank definition used here, p95 is an
-- actual member of the set at or above the 95th position, so p95 >= avg holds
-- for any distribution with a non-negative tail.
--
-- This is the test that a percentile is a percentile. dbt/macros/exact_percentile.sql
-- computes an index into a sorted array, and the two ways to get that wrong --
-- an off-by-one at the boundary, or a 0-based index against Trino's 1-based
-- arrays -- both show up as a p95 that has drifted toward the middle of the
-- distribution. Comparing against the mean catches that without needing a
-- second implementation to compare against.
--
-- Both marts, in one test, because both call the same macro.

with p95_vs_avg as (

    select 'fct_model_costs' as mart, event_day, latency_p95_ms, latency_avg_ms
    from {{ ref('fct_model_costs') }}

    union all

    select 'fct_tool_reliability' as mart, event_day, latency_p95_ms, latency_avg_ms
    from {{ ref('fct_tool_reliability') }}

)

select *
from p95_vs_avg
where latency_p95_ms < latency_avg_ms
