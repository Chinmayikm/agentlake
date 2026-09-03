-- A session cannot contain more turns than spans.
--
-- turn_count is count(distinct trace_id) and span_count is count(*) over the
-- same rows, so every turn contributes at least its own root span. This is an
-- invariant of ADR-000's scoping rule -- session = conversation, trace = one
-- turn -- and not a statistical property, which is why it blocks.
--
-- What it would actually catch: a change to how the SDK opens traces (say, a
-- trace_id minted per span rather than per turn) would make turn_count and
-- span_count converge and then cross, and nothing else in the repo would
-- notice. It is the cheapest available check on the thing ADR-003 #4 had to
-- rebuild after a single turn showed up as six separate traces.

select
    session_id,
    turn_count,
    span_count
from {{ ref('fct_sessions') }}
where span_count < turn_count
