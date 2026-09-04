-- eval_runs -- one row per execution of the eval harness.
--
-- Every column here is something you need to reproduce a score, which is the
-- whole reason it is a table rather than a log line: git_sha pins the code,
-- prompt_version_id pins the prompt, retriever_config_json pins the retrieval
-- knobs, corpus_version pins what was retrievable, judge_model pins the grader.
-- A quality number without those five is not comparable to any other quality
-- number.

CREATE TABLE IF NOT EXISTS eval_runs (
    id                    bigserial PRIMARY KEY,
    git_sha               text        NOT NULL,
    prompt_version_id     bigint      NOT NULL REFERENCES prompt_versions (id),
    retriever_config_json jsonb       NOT NULL DEFAULT '{}'::jsonb,
    corpus_version        text        NOT NULL,
    judge_model           text        NOT NULL,
    started_at            timestamptz NOT NULL DEFAULT now(),
    finished_at           timestamptz,
    -- numeric, not double: this is money, and it is summed. Debezium is
    -- configured with decimal.handling.mode=string precisely so that this
    -- crosses the wire as "0.004420" rather than as a base64-encoded
    -- unscaled-value/scale pair, which is the default and is unreadable.
    total_cost_usd        numeric(12, 6)
);

CREATE INDEX IF NOT EXISTS eval_runs_prompt_version_id_idx
    ON eval_runs (prompt_version_id);
