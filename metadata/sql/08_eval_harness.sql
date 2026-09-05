-- Columns the eval harness needs, added to the three tables ADR-007 created
-- for it. Schema only -- no prompt or golden rows are inserted here.
--
-- Why no rows: prompt text lives in services/agent/prompts/*.md and golden
-- examples in eval/golden/*.yaml, and scripts/load_prompts.py and
-- `python -m eval load-golden` publish them. Copying that text into a
-- migration would create a second copy to keep in sync, guarded only by a
-- test -- which is the drift 07_seed.sql's three rows are already the maximum
-- acceptable amount of. The seed exists so the connector's first snapshot is
-- not empty; the loaders own everything after that.
--
-- Every statement is IF NOT EXISTS, because metadata-init re-applies every
-- file on every bring-up (ADR-007 #7).

-- --------------------------------------------------------------------------
-- golden_examples: a stable identity that survives rewording
-- --------------------------------------------------------------------------
--
-- The existing natural key is (question, dataset_version). That makes a fixed
-- typo a NEW example: the old row stays, the new row has no history, and
-- eval_results split across the two -- so a score becomes uncomparable for a
-- reason that has nothing to do with quality. example_key is assigned by hand
-- in the YAML and never changes, so an edit is an UPDATE and the CDC changelog
-- keeps the old wording at its own LSN. A historical score stays traceable to
-- the exact text that produced it, which is what landing the log buys.
--
-- Nullable, and the unique index therefore ignores the three seed-v0 rows
-- (Postgres treats NULLs as distinct in a unique index) -- they predate this
-- and are not the harness's to adopt.
ALTER TABLE golden_examples ADD COLUMN IF NOT EXISTS example_key text;

CREATE UNIQUE INDEX IF NOT EXISTS golden_examples_example_key_dataset_key
    ON golden_examples (example_key, dataset_version);

-- --------------------------------------------------------------------------
-- eval_runs: the rest of what makes two runs comparable
-- --------------------------------------------------------------------------
--
-- ADR-007 pinned five things (git_sha, prompt_version_id, retriever config,
-- corpus_version, judge_model) and said a quality number without them is not
-- comparable to any other quality number. Running the harness proved four more
-- belong on that list: which examples were scored (dataset_version, subset),
-- which model answered (model_alias), and the two knobs that change what the
-- agent can do within a turn (max_steps) and how a score was measured
-- (hit_source, judge_seed). A gate that compared across any of these would be
-- reporting a configuration change as a quality regression.
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS dataset_version text;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS subset           text;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS model_alias      text;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS max_steps        integer;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS judge_seed       integer;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS hit_source       text;
ALTER TABLE eval_runs ADD COLUMN IF NOT EXISTS notes            text;

-- --------------------------------------------------------------------------
-- eval_results: what a score has to carry to be auditable, and to be resumable
-- --------------------------------------------------------------------------
--
-- The UNIQUE is the load-bearing one. The harness commits one row per example
-- as it finishes, so a crash costs the example in flight and nothing else --
-- but only if a re-run UPSERTs. Without a constraint to conflict on, ON
-- CONFLICT is not merely useless, it is a syntax error, and a resumed run
-- would silently double every completed example. Exactly the failure ADR-007
-- #7 recorded for the seed's ON CONFLICT DO NOTHING with nothing to conflict
-- on: the clause reads like a guard and is not one until something can conflict.
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS trace_id   text;
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS session_id text;
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS answer     text;
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS cost_usd   numeric(12, 6);

-- The chunks that were actually retrieved, so hit_at_k = false can be argued
-- with a month later without the trace -- which has a 7-day TTL on the hot
-- path. A score whose evidence expired before the score did cannot be audited,
-- only believed; same argument judge_rationale is stored under (ADR-007's
-- 05_eval_results.sql).
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS retrieved_sources_json jsonb
    NOT NULL DEFAULT '[]'::jsonb;

-- Which of the two randomised orders the judge saw, recorded rather than
-- recomputed: a bias control nobody can check after the fact is a claim, not
-- a control.
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS judge_order    text;
-- false when the judge's reply could not be parsed even after one retry. The
-- score columns are then NULL -- never a defaulted 3, which would be a
-- fabricated measurement (ADR-003 #3).
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS judge_parse_ok boolean;
-- 'observed' | 'trace' | 'observed_fallback'. A fallback that did not record
-- itself would make two runs look identically measured when they were not.
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS hit_source     text;
-- Set when the example failed outright; scores stay NULL. A failure counted as
-- a low score is a harness problem reported as a quality number.
ALTER TABLE eval_results ADD COLUMN IF NOT EXISTS error          text;

CREATE UNIQUE INDEX IF NOT EXISTS eval_results_run_example_key
    ON eval_results (eval_run_id, golden_example_id);
