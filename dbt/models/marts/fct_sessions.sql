-- One row per session: what a whole conversation cost and how it went.
--
-- turn_count is count(distinct trace_id), and that is the definition, not an
-- approximation of one. ADR-000 fixes the scope: a session is one conversation
-- and is what Kafka partitions on; a trace is one turn's causal graph. So
-- distinct traces within a session IS the number of turns, and
-- span_count >= turn_count is an invariant rather than a heuristic -- every
-- trace has at least its root span. tests/assert_span_count_ge_turn_count.sql
-- makes it blocking.
--
-- duration_ms understates, and by a known amount. ts is stamped when a span
-- OPENS (ADR-004 #3: the event is emitted from the span's finally block, so the
-- timestamp is older than the produce time by the span's duration), so
-- max(ts) - min(ts) is first-open to last-open and misses the last span's own
-- latency. Adding it back would need to know which span is last, and "the span
-- that opened last" is not necessarily "the span that closed last" once spans
-- nest. The honest column is the one that says what it measures.

select
    session_id,

    count(distinct trace_id) as turn_count,
    count(*) as span_count,
    count(*) filter (where event_type = 'TOOL_CALL') as tool_call_count,
    count(*) filter (where event_type = 'LLM_CALL') as llm_call_count,
    count(*) filter (where is_error) as error_count,

    -- coalesce, because prompt_tokens/completion_tokens/cost_usd are NULL for
    -- every non-LLM span and sum() over an all-NULL group is NULL, not 0. A
    -- session with no LLM_CALL cost nothing; it did not cost an unknown amount.
    coalesce(sum(prompt_tokens), 0) as prompt_tokens,
    coalesce(sum(completion_tokens), 0) as completion_tokens,
    coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0) as total_tokens,
    coalesce(sum(cost_usd), 0) as total_cost_usd,

    min(ts) as first_span_ts,
    max(ts) as last_span_ts,
    date_diff('millisecond', min(ts), max(ts)) as duration_ms

from {{ ref('stg_trace_events') }}
group by session_id
