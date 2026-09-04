-- Typed, projected view of every span. All 13 contract fields survive, plus the
-- handful of derived columns every mart would otherwise re-derive.
--
-- ts_epoch_ms -> ts is the one rename, and it follows ADR-005 #8's rule --
-- "rename where you already transform" -- rather than the shape of the name.
-- This model IS a projection, so the rename rides along with it, exactly as the
-- hot path's materialized view does. The happy consequence is that ts means the
-- same thing in ClickHouse and here, which is what makes
-- scripts/analytics_verify.py's cross-engine comparison a comparison of numbers
-- rather than of column conventions.
--
-- attributes stays whole AND is unpacked. The map is what makes the contract
-- extensible without a schema change, so dropping it would be lossy; but a mart
-- that has to write element_at(attributes, 'name') is a mart that will spell it
-- differently the second time. Both.

select
    trace_id,
    span_id,
    parent_span_id,
    session_id,
    event_type,
    model,
    prompt_tokens,
    completion_tokens,
    latency_ms,
    cost_usd,
    status,
    ts_epoch_ms as ts,
    cast(ts_epoch_ms as date) as ts_day,
    attributes,

    -- Span name. services/sdk/telemetry.py sets attributes["name"] on every
    -- span unconditionally -- it is the first key of the dict literal -- which
    -- is why not_null on the marts' tool_name is a blocking test rather than a
    -- hopeful one.
    element_at(attributes, 'name') as span_name,

    -- The tool a TOOL_CALL invoked. scripts/gen_traffic.py labels it `tool`
    -- (tool="orders_api") while services/mcp_server names the span after the
    -- tool itself, so the coalesce is not defensiveness -- it is two real
    -- producers that spell the same fact differently.
    coalesce(element_at(attributes, 'tool'), element_at(attributes, 'name')) as tool_name,

    -- Set by the SDK's own except-block (telemetry.py) from type(exc).__name__.
    element_at(attributes, 'error_class') as error_class,

    -- The prompt template this LLM call was made under. services/gateway stamps
    -- it onto the LLM_CALL span from the X-Prompt-Version header the agent
    -- sends (ADR-007 #6); services/agent stamps it onto AGENT_STEP too, so a
    -- turn is self-describing. NULL on every other span type, and on every span
    -- older than that change -- which fct_cost_by_prompt reports as
    -- prompt_attribution='unversioned' rather than dropping.
    --
    -- Unpacked here rather than left as element_at(...) in the mart, for the
    -- reason at the top of this file: a mart that spells out the map lookup is
    -- a mart that will spell it differently the second time. This is the join
    -- key to stg_prompt_versions.version.
    element_at(attributes, 'prompt_version') as prompt_version,

    -- status != 'ok', never status = 'error'. status is a free-form contract
    -- string and the SDK promotes whatever set(status=...) is handed it -- its
    -- own tests already cover "degraded" -- so equality against 'error' would
    -- silently under-count. Same rule the dashboards and the whitelisted MCP
    -- metrics follow; see ADR-005 #2.
    status <> 'ok' as is_error

from {{ source('lake_raw', 'trace_events') }}
