-- eval_results -- one row per (eval_run, golden_example): the graded answer.
--
-- hit_at_k and citation_ok are mechanical checks the harness can compute.
-- faithfulness and answer_quality are LLM-judge scores, kept as smallint on a
-- small ordinal scale rather than as a float, because a judge that emits 3.7
-- is reporting precision it does not have. judge_rationale is stored beside
-- them so a score is always auditable -- a number whose reasoning was thrown
-- away cannot be argued with, only believed.

CREATE TABLE IF NOT EXISTS eval_results (
    id                bigserial PRIMARY KEY,
    eval_run_id       bigint   NOT NULL REFERENCES eval_runs (id) ON DELETE CASCADE,
    golden_example_id bigint   NOT NULL REFERENCES golden_examples (id),
    hit_at_k          boolean,
    citation_ok       boolean,
    faithfulness      smallint,
    answer_quality    smallint,
    judge_rationale   text,
    answer_len_tokens integer,
    latency_ms        double precision
);

CREATE INDEX IF NOT EXISTS eval_results_eval_run_id_idx
    ON eval_results (eval_run_id);
