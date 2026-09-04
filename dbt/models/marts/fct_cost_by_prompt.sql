-- What each prompt version cost, per day.
--
-- The first mart in this project that joins two SOURCES rather than reshaping
-- one. The fact side is the same stg_trace_events every other mart reads; the
-- dimension side came from Postgres through Debezium (ADR-007):
-- prompt_versions -> cdc.metadata.prompt_versions -> lake.cdc.prompt_versions
-- -> stg_prompt_versions. Both of the ways that goes wrong are handled
-- structurally here rather than by being careful.
--
-- 1. AGGREGATE FIRST, JOIN SECOND. by_prompt is grouped before prompt_dim is
--    touched, so a dimension that fanned out could add mart ROWS but could not
--    inflate calls, cost_usd or tokens -- every measure is computed from the
--    fact table alone. That is the property that makes
--    tests/assert_marts_reconcile_with_staging.sql's arithmetic hold no matter
--    what the dimension does, and the fan-out itself is caught by the
--    unique_combination_of_columns test on (event_day, prompt_version).
--
-- 2. LEFT JOIN, and it has to be. An inner join would drop every LLM_CALL span
--    carrying no prompt_version -- the entire history from before the gateway
--    started stamping it -- SILENTLY: the mart would stay internally
--    consistent, pass every column test, and simply be about the wrong rows.
--    A right or full join is wrong in the other direction: it would invent a
--    row for a prompt version nobody ever called, and a dimension row is not a
--    cost fact.
--
--    So nothing is dropped (every LLM_CALL span is in exactly one group) and
--    nothing is invented (an unmatched group carries NULL dimension columns
--    rather than a guess).
--
-- 3. NULL prompt_version is a REAL group, not a sentinel string. Trino's GROUP
--    BY treats NULL as a group, so those spans collapse to one row per day
--    instead of disappearing -- exactly the decision fct_model_costs already
--    records for its NULL `model` group. A sentinel like '(unattributed)' would
--    have been a value this layer invented and that nothing upstream can ever
--    produce.
--
-- prompt_attribution names which of three states each row is in, so "no
-- prompt_version was stamped" is never confused with "a prompt_version was
-- stamped and the dimension has never heard of it" -- one is history, the other
-- means scripts/cdc_land.py has not run.
--
-- No latency_p95_ms, deliberately. The exact percentile belongs at the model and
-- tool grain where ADR-004 #6 left it and ADR-006 #4 delivered it; a third one
-- would need assert_p95_ge_avg.sql extended and would answer no new question.
--
-- Grain: one row per (event_day, prompt_version), NULL prompt_version included.

with llm_calls as (

    select
        ts_day,
        trace_id,
        prompt_tokens,
        completion_tokens,
        cost_usd,
        latency_ms,
        is_error,
        prompt_version
    from {{ ref('stg_trace_events') }}
    where event_type = 'LLM_CALL'

),

by_prompt as (

    select
        ts_day as event_day,
        prompt_version,

        count(*) as calls,

        -- The same definition ADR-006 #4 fixes and fct_sessions uses: ADR-000
        -- scopes a trace to one turn, so count(distinct trace_id) IS the number
        -- of turns, not an estimate of it.
        count(distinct trace_id) as turns,

        -- coalesce because prompt_tokens/completion_tokens/cost_usd are
        -- nullable by contract, and sum() over an all-NULL group is NULL, not 0.
        coalesce(sum(prompt_tokens), 0) as prompt_tokens,
        coalesce(sum(completion_tokens), 0) as completion_tokens,
        coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0)
            as total_tokens,
        coalesce(sum(cost_usd), 0) as cost_usd,

        count(*) filter (where is_error) as error_count,
        avg(latency_ms) as latency_avg_ms,
        max(latency_ms) as latency_max_ms

    from llm_calls
    group by ts_day, prompt_version

),

prompt_dim as (

    -- One row per version string. stg_prompt_versions' grain is the Postgres
    -- primary key, and (name, version) CAN legitimately repeat there: delete v2
    -- and add it again and the changelog holds two ids, the older one
    -- is_deleted. Highest LSN is the current state of that version; id desc is
    -- the tiebreak for a dimension that arrived by snapshot, where every row
    -- shares one LSN.
    select prompt_version_id, prompt_name, version, is_deleted
    from (
        select
            prompt_version_id,
            prompt_name,
            version,
            is_deleted,
            row_number() over (
                partition by version
                order by last_source_lsn desc nulls last, prompt_version_id desc
            ) as recency
        from {{ ref('stg_prompt_versions') }}
        where version is not null
    )
    where recency = 1

)

select
    by_prompt.event_day,
    by_prompt.prompt_version,

    prompt_dim.prompt_version_id,
    prompt_dim.prompt_name,
    prompt_dim.is_deleted as prompt_is_deleted,

    -- Three states, told apart rather than collapsed into a null:
    --   unversioned  the span carries no prompt_version attribute. Normal for
    --                every span older than the gateway change, and for anything
    --                producing to the topic that is not the gateway.
    --   known        the dimension holds it. prompt_is_deleted then says whether
    --                the prompt has since been retired -- the join still
    --                matches, because the cost was real.
    --   unknown      the span named a version the dimension does not hold.
    --                Legitimately reachable between a prompt being created in
    --                Postgres and cdc_land.py next running, which is exactly why
    --                the check on it warns instead of gating.
    case
        when by_prompt.prompt_version is null then 'unversioned'
        when prompt_dim.version is null then 'unknown'
        else 'known'
    end as prompt_attribution,

    by_prompt.calls,
    by_prompt.turns,
    by_prompt.prompt_tokens,
    by_prompt.completion_tokens,
    by_prompt.total_tokens,
    by_prompt.cost_usd,

    -- The number the Grafana panel plots, computed once here so the dashboard
    -- and the mart cannot drift into two definitions of "per turn".
    by_prompt.cost_usd / by_prompt.turns as cost_per_turn_usd,

    by_prompt.error_count,
    by_prompt.latency_avg_ms,
    by_prompt.latency_max_ms

from by_prompt
-- NULL = NULL is unknown in SQL, so the unversioned group never matches a
-- dimension row and correctly keeps NULL prompt_name / prompt_is_deleted.
left join prompt_dim
       on prompt_dim.version = by_prompt.prompt_version
