-- Every mart must account for exactly the staging rows it claims to summarise.
--
-- The failure this catches is the quiet one: a WHERE clause that drifts, a
-- GROUP BY that drops a NULL key, a join that fans out. None of those produce
-- an error -- they produce a mart that is internally consistent, passes every
-- column-level test, and is simply about the wrong rows. Comparing each mart's
-- summed call count against a direct count over staging is the only check that
-- notices.
--
-- fct_sessions is the total; the other three are filtered to one event_type
-- each, which is why they are counted against the same filter rather than the
-- whole table.
--
-- fct_cost_by_prompt is the one that most needs this. It is the only mart that
-- JOINS -- the CDC dimension to the trace facts (ADR-007 #6) -- and the two
-- ways a join goes wrong are both invisible from inside the mart: an inner join
-- would silently drop every LLM_CALL span carrying no prompt_version, and a
-- fanned-out dimension would silently double a group. Because the mart
-- aggregates before it joins, sum(calls) must still equal the LLM_CALL count
-- exactly, and this is where that is checked.

with expected as (

    select
        'fct_sessions'          as mart,
        count(*)                as staging_rows
    from {{ ref('stg_trace_events') }}

    union all

    select
        'fct_model_costs',
        count(*)
    from {{ ref('stg_trace_events') }}
    where event_type = 'LLM_CALL'

    union all

    select
        'fct_tool_reliability',
        count(*)
    from {{ ref('stg_trace_events') }}
    where event_type = 'TOOL_CALL'

    union all

    select
        'fct_cost_by_prompt',
        count(*)
    from {{ ref('stg_trace_events') }}
    where event_type = 'LLM_CALL'

),

actual as (

    select 'fct_sessions' as mart, coalesce(sum(span_count), 0) as mart_rows
    from {{ ref('fct_sessions') }}

    union all

    select 'fct_model_costs', coalesce(sum(calls), 0)
    from {{ ref('fct_model_costs') }}

    union all

    select 'fct_tool_reliability', coalesce(sum(calls), 0)
    from {{ ref('fct_tool_reliability') }}

    union all

    select 'fct_cost_by_prompt', coalesce(sum(calls), 0)
    from {{ ref('fct_cost_by_prompt') }}

)

select
    expected.mart,
    expected.staging_rows,
    actual.mart_rows
from expected
join actual on actual.mart = expected.mart
where expected.staging_rows <> actual.mart_rows
