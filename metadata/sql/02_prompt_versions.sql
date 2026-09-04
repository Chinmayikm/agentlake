-- prompt_versions -- the dimension the trace facts join to.
--
-- services/agent stamps `version` into attributes['prompt_version'] on every
-- LLM_CALL span (ADR-007 #6), which is what makes
-- lake.analytics.fct_cost_by_prompt a join rather than a second fact table.
--
-- Append-only BY CONVENTION, not by trigger. Nothing here stops an UPDATE or a
-- DELETE, and ADR-007's verification log deliberately performs both -- the
-- point of a CDC pipeline is that it resolves change correctly, and a
-- constraint that made change impossible would leave that unproven. The
-- convention is what the eval harness will follow; the pipeline is built for
-- the case where something does not.

CREATE TABLE IF NOT EXISTS prompt_versions (
    id            bigserial PRIMARY KEY,
    name          text        NOT NULL,
    version       text        NOT NULL,
    template_text text        NOT NULL,
    params_json   jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- `version` is the join key the gateway stamps, so two rows sharing one version
-- would fan the mart's dimension out. A UNIQUE constraint makes that
-- impossible at the source rather than deduplicating it downstream -- though
-- fct_cost_by_prompt still collapses the dimension defensively, because a
-- deleted-then-recreated version legitimately produces two ids.
CREATE UNIQUE INDEX IF NOT EXISTS prompt_versions_version_key
    ON prompt_versions (version);

-- REPLICA IDENTITY FULL, and this one is load-bearing.
--
-- Under Postgres' default replica identity a DELETE's WAL record carries only
-- the primary key, so Debezium's `before` image arrives with every other column
-- NULL. The deleted row would then land in the changelog with no `version`, and
-- the latest-record resolution downstream could not say WHICH prompt version
-- was deleted -- only that some id was. That is indistinguishable, from the
-- mart's side, from a row the lander never saw, which is precisely the state
-- fct_cost_by_prompt's prompt_attribution='unknown' exists to make visible.
--
-- The cost is real and worth stating: every UPDATE and DELETE now writes the
-- full old row to the WAL, not just the key. On a table of a few dozen rows
-- that is free. On a wide, hot table it is not, and ADR-007 #7 says so.
ALTER TABLE prompt_versions REPLICA IDENTITY FULL;
