-- The publication pgoutput streams through.
--
-- Created here, by a migration, rather than by the connector -- which is what
-- publication.autocreate.mode=disabled in scripts/register_connector.py means.
-- Three reasons, in the order they matter:
--
--   1. `all_tables`, the Debezium default, needs the connection role to be
--      superuser (CREATE PUBLICATION ... FOR ALL TABLES does), and the whole
--      point of the debezium role in 01_* is that it is not one.
--   2. `all_tables` also captures every table public ever gains, silently. A
--      publication that widens itself is a data-exfiltration surface that
--      nobody reviewed.
--   3. `disabled` turns a missing publication into a loud connector failure at
--      startup instead of a silent re-creation with different contents. Same
--      instinct as submit.sh refusing a replaying submit: make the surprising
--      thing impossible rather than merely unlikely.
--
-- CREATE PUBLICATION has no IF NOT EXISTS before Postgres 18, hence the guard.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'agentlake_cdc') THEN
        CREATE PUBLICATION agentlake_cdc
            FOR TABLE prompt_versions, golden_examples, eval_runs, eval_results;
    END IF;
END
$$;
