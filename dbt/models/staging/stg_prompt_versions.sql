-- The current state of public.prompt_versions, out of the Debezium changelog.
--
-- lake.cdc.prompt_versions is append-only and at-least-once: one Iceberg row per
-- Kafka record, duplicates included, tombstones excluded (scripts/cdc_land.py
-- counts those rather than landing them -- ADR-007 #4). This model is where that
-- log becomes a dimension: the latest record per primary key, under a
-- deterministic total order.
--
-- The order, in the order it is applied, and what each key is for:
--
--   source_lsn desc nulls last
--       Postgres' own total order over WAL records -- the only key that orders a
--       change the way the DATABASE committed it, so it survives a topic
--       recreate or a consumer-group reset, both of which renumber
--       kafka_offset. NULLS LAST is written out rather than left to Trino's
--       default, because a record with no LSN must lose to one that has an LSN,
--       never win by accident.
--
--   case op when 'r' then 0 else 1 end desc
--       Debezium stamps every op='r' row of an initial snapshot with the SAME
--       LSN -- the snapshot's consistent point. Measured, not assumed: the three
--       seeded rows all landed at lsn 26723880 (ADR-007's verification log). A
--       streaming change at that same LSN is the later fact, so a non-snapshot
--       record outranks a snapshot one on a tie. This is the only tie LSN
--       genuinely produces in normal operation.
--
--   kafka_offset desc
--       Debezium keys every record by the row's primary key, so every record for
--       one id is in one partition, so offset order IS per-key commit order.
--
--   ingest_ts desc
--       Last resort, and the only key an at-least-once replay can differ on.
--
-- What survives all four is exactly one class: two rows from the same
-- (partition, offset), landed twice by a replayed batch. Those are identical in
-- every payload column, so which one ROW_NUMBER picks is unobservable and the
-- model's OUTPUT is deterministic even though the ordering is not total. That
-- is precisely what lets cdc_land.py commit offsets after the insert rather
-- than before it -- and committing before would be at-most-once, i.e. silent
-- data loss, which no amount of dedup repairs.
--
-- Deleted rows are KEPT, carrying is_deleted, not filtered out. The facts this
-- dimension explains are historical: a prompt retired last Tuesday still has
-- three weeks of cost attributed to it, and dropping it here would turn those
-- spans into fct_cost_by_prompt's prompt_attribution='unknown' -- the state that
-- means the LANDER is broken. A model must not make a routine event look like a
-- pipeline failure. Filtering downstream is `where not is_deleted`; un-filtering
-- is impossible. Same asymmetry stg_trace_events invokes when it keeps
-- `attributes` whole and unpacks it.
--
-- QUALIFY would fold the two steps into one clause and Trino 483 has it. This
-- is ROW_NUMBER plus a subquery because a reviewer who has not met QUALIFY can
-- still read it -- the repo's boring-over-clever rule.

with ranked as (

    select
        id,
        name,
        version,
        template_text,
        params_json,
        created_at,

        op,
        source_lsn,
        source_tx_id,
        source_snapshot,
        source_ts,
        event_ts,
        ingest_ts,
        kafka_partition,
        kafka_offset,

        row_number() over (
            partition by id
            order by
                source_lsn desc nulls last,
                case op when 'r' then 0 else 1 end desc,
                kafka_offset desc,
                ingest_ts desc
        ) as recency

    from {{ source('lake_cdc', 'prompt_versions') }}

    -- Second line of defence. scripts/cdc_land.py already refuses to land
    -- anything outside these four (op='t' is TRUNCATE and op='m' is a logical
    -- decoding message -- neither carries a row image), and this is what lets
    -- the accepted_values test on last_op below be a gate rather than a hope.
    where op in ('c', 'u', 'd', 'r')

)

select
    id as prompt_version_id,
    name as prompt_name,

    -- The exact string services/gateway stamps into
    -- attributes['prompt_version'], which is what makes fct_cost_by_prompt a
    -- join. metadata/sql/02_prompt_versions.sql carries a UNIQUE index on
    -- version, so this is a key at the source rather than only by convention --
    -- though the mart still collapses the dimension defensively, because a
    -- version deleted and then recreated legitimately produces two ids.
    version,

    template_text,
    params_json,
    created_at,

    op as last_op,

    -- The delete, carried rather than applied. Same idiom as stg_trace_events'
    -- `status <> 'ok' as is_error`: name the fact, let the consumer decide.
    -- A deleted row still carries its name/version/template_text because
    -- prompt_versions runs REPLICA IDENTITY FULL -- under Postgres' default
    -- replica identity the `before` image is the primary key and nothing else,
    -- and this row would resolve to a NULL version that silently stopped
    -- joining.
    op = 'd' as is_deleted,

    source_lsn as last_source_lsn,
    source_ts as last_change_ts,
    kafka_offset as last_kafka_offset,
    ingest_ts as landed_at

from ranked
where recency = 1
