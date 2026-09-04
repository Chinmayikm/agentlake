-- golden_examples -- the labelled questions an eval run scores against.
--
-- Written by the eval harness (roadmap, not this slice). Created here because
-- the CDC connector captures the whole metadata schema in one publication, and
-- a table added later means a publication change and a connector restart.

CREATE TABLE IF NOT EXISTS golden_examples (
    id                     bigserial PRIMARY KEY,
    question               text        NOT NULL,
    expected_answer        text,
    -- The chunk ids / source URLs a faithful answer should have cited. jsonb
    -- rather than a text[] because the shape is a list of objects, not a list
    -- of labels -- see tags below for the other choice.
    expected_sources_json  jsonb       NOT NULL DEFAULT '[]'::jsonb,
    -- text[] here, because these ARE labels: flat, unordered, and something you
    -- want to filter on with `@>`. Note that Debezium renders a Postgres array
    -- as a JSON array, which is why nothing downstream has to know the
    -- difference.
    tags                   text[]      NOT NULL DEFAULT '{}',
    dataset_version        text        NOT NULL,
    created_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS golden_examples_dataset_version_idx
    ON golden_examples (dataset_version);

-- The natural key, and it exists so 07_seed.sql's ON CONFLICT DO NOTHING has
-- something to conflict ON. Without a unique constraint that clause is a no-op
-- that silently succeeds, and metadata-init -- which runs on every bring-up --
-- would insert the seed set again every time.
CREATE UNIQUE INDEX IF NOT EXISTS golden_examples_question_dataset_key
    ON golden_examples (question, dataset_version);
