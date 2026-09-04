-- The replication role Debezium connects as.
--
-- Apply order is the filename order, and it is load-bearing exactly as it is in
-- stream/clickhouse/sql/: the role has to exist before anything is granted to
-- it, the tables have to exist before the publication names them, and the seed
-- rows have to be last so the initial snapshot has something to carry.
--
-- REPLICATION is the privilege that matters, and it is not superuser: a
-- REPLICATION role may open a logical replication slot and read the WAL through
-- it, and nothing else. It still needs SELECT on the captured tables, because
-- Debezium's initial snapshot is an ordinary query -- only the streaming phase
-- reads WAL. That split is easy to miss and shows up as a snapshot that fails
-- with "permission denied" against a connector that otherwise looks configured.
--
-- CREATE ROLE has no IF NOT EXISTS, hence the DO block: every file here has to
-- be safe to re-apply, because metadata-init runs on every `docker compose up`.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION;
    END IF;
END
$$;

-- Set unconditionally rather than only at creation, so rotating
-- DEBEZIUM_PASSWORD in .env and re-running metadata-init actually rotates it.
--
-- :'dbz_password' is a psql variable, handed in by metadata-init as
-- `-v dbz_password="$DEBEZIUM_PASSWORD"`, and psql renders it as a properly
-- quoted literal. It has to be a plain statement rather than a DO block:
-- psql does not interpolate variables inside dollar-quoted strings, so the
-- ALTER would be sent with the literal text :'dbz_password' as the password.
-- The secret is never written into this file, and never into the WAL as
-- plaintext -- Postgres stores the SCRAM verifier.
ALTER ROLE debezium WITH PASSWORD :'dbz_password';

GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- The tables in 02..05 are created after this file runs, so the grant above
-- cannot reach them on a first bring-up. This makes the grant apply to whatever
-- public ends up holding, including anything a later migration adds.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
