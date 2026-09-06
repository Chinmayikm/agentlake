"""Where an eval run is recorded.

A `ResultSink` Protocol with two implementations, mirroring the shape
`services/rag/store.py` already uses for `Store`: `PostgresSink` in production,
`ListSink` as the test fake. That is what keeps a database out of the test
suite entirely -- `tests/test_eval_harness.py` drives a whole run through
`ListSink` and never opens a socket.

Rows land in the metadata database, so they flow through the existing Debezium
connector onto `cdc.metadata.eval_runs` / `cdc.metadata.eval_results` with no
new configuration (the publication already names all four tables, ADR-007 #2).
Landing those topics into Iceberg is deliberately NOT built here -- it is a
copy of `scripts/cdc_land.py` rather than a design.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from metadata.client import connect

# ---------------------------------------------------------------------------
# SQL, as named constants -- same convention as scripts/cdc_land.py
# ---------------------------------------------------------------------------

INSERT_RUN = """
INSERT INTO eval_runs (
    git_sha, prompt_version_id, retriever_config_json, corpus_version, judge_model,
    dataset_version, subset, model_alias, max_steps, judge_seed, hit_source, notes
)
VALUES (
    %(git_sha)s, %(prompt_version_id)s, %(retriever_config_json)s, %(corpus_version)s,
    %(judge_model)s, %(dataset_version)s, %(subset)s, %(model_alias)s, %(max_steps)s,
    %(judge_seed)s, %(hit_source)s, %(notes)s
)
RETURNING id
"""

# The UPSERT is what makes `--resume` free rather than a duplicate-generator.
# It needs UNIQUE (eval_run_id, golden_example_id), added by
# metadata/sql/08_eval_harness.sql -- without a constraint to conflict on, this
# is not a weak guard, it is a syntax error.
UPSERT_RESULT = """
INSERT INTO eval_results (
    eval_run_id, golden_example_id, hit_at_k, citation_ok, faithfulness, answer_quality,
    judge_rationale, answer_len_tokens, latency_ms, trace_id, session_id, answer,
    cost_usd, retrieved_sources_json, judge_order, judge_parse_ok, hit_source, error
)
VALUES (
    %(eval_run_id)s, %(golden_example_id)s, %(hit_at_k)s, %(citation_ok)s, %(faithfulness)s,
    %(answer_quality)s, %(judge_rationale)s, %(answer_len_tokens)s, %(latency_ms)s,
    %(trace_id)s, %(session_id)s, %(answer)s, %(cost_usd)s, %(retrieved_sources_json)s,
    %(judge_order)s, %(judge_parse_ok)s, %(hit_source)s, %(error)s
)
ON CONFLICT (eval_run_id, golden_example_id) DO UPDATE SET
    hit_at_k = EXCLUDED.hit_at_k,
    citation_ok = EXCLUDED.citation_ok,
    faithfulness = EXCLUDED.faithfulness,
    answer_quality = EXCLUDED.answer_quality,
    judge_rationale = EXCLUDED.judge_rationale,
    answer_len_tokens = EXCLUDED.answer_len_tokens,
    latency_ms = EXCLUDED.latency_ms,
    trace_id = EXCLUDED.trace_id,
    session_id = EXCLUDED.session_id,
    answer = EXCLUDED.answer,
    cost_usd = EXCLUDED.cost_usd,
    retrieved_sources_json = EXCLUDED.retrieved_sources_json,
    judge_order = EXCLUDED.judge_order,
    judge_parse_ok = EXCLUDED.judge_parse_ok,
    hit_source = EXCLUDED.hit_source,
    error = EXCLUDED.error
"""

FINISH_RUN = """
UPDATE eval_runs SET finished_at = now(), total_cost_usd = %(total_cost_usd)s
WHERE id = %(id)s
"""

SET_JUDGE_MODEL = "UPDATE eval_runs SET judge_model = %(judge_model)s WHERE id = %(id)s"

SELECT_PROMPT_VERSION_ID = "SELECT id FROM prompt_versions WHERE version = %(version)s"

SELECT_GOLDEN_IDS = """
SELECT example_key, id FROM golden_examples
WHERE dataset_version = %(dataset_version)s AND example_key IS NOT NULL
"""

# Kept in sync with eval/dataset.py by the loader, not by hand.
UPSERT_GOLDEN = """
INSERT INTO golden_examples
    (example_key, question, expected_answer, expected_sources_json, tags, dataset_version)
VALUES
    (%(example_key)s, %(question)s, %(expected_answer)s, %(expected_sources_json)s,
     %(tags)s, %(dataset_version)s)
ON CONFLICT (example_key, dataset_version) DO UPDATE SET
    question = EXCLUDED.question,
    expected_answer = EXCLUDED.expected_answer,
    expected_sources_json = EXCLUDED.expected_sources_json,
    tags = EXCLUDED.tags
 WHERE golden_examples.question IS DISTINCT FROM EXCLUDED.question
    OR golden_examples.expected_answer IS DISTINCT FROM EXCLUDED.expected_answer
    OR golden_examples.expected_sources_json IS DISTINCT FROM EXCLUDED.expected_sources_json
    OR golden_examples.tags IS DISTINCT FROM EXCLUDED.tags
RETURNING id, (xmax = 0) AS inserted
"""

SELECT_RESULTS_FOR_RUN = """
SELECT er.id, ge.example_key, ge.question, er.answer, er.faithfulness,
       er.retrieved_sources_json
FROM eval_results er
JOIN golden_examples ge ON ge.id = er.golden_example_id
WHERE er.eval_run_id = %(eval_run_id)s
ORDER BY ge.example_key
"""

LATEST_RUN = """
SELECT id FROM eval_runs
WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1
"""


@dataclass(slots=True)
class ExampleOutcome:
    """One graded example, as it will be stored."""

    example_key: str
    hit_at_k: bool | None = None
    citation_ok: bool | None = None
    faithfulness: int | None = None
    answer_quality: int | None = None
    judge_rationale: str = ""
    answer_len_tokens: int = 0
    latency_ms: float = 0.0
    trace_id: str = ""
    session_id: str = ""
    answer: str = ""
    cost_usd: float = 0.0
    retrieved_sources: list[str] = field(default_factory=list)
    judge_order: str = "n/a"
    judge_parse_ok: bool | None = None
    hit_source: str = "observed"
    error: str | None = None
    #: Reported, never gated -- see eval/metrics.py.
    searched: bool = False
    any_call_hit: bool = False
    truncated: bool = False
    steps_used: int = 0


class ResultSink(Protocol):
    def create_run(self, **config: Any) -> int: ...
    def write_result(self, run_id: int, outcome: ExampleOutcome) -> None: ...
    def finish_run(self, run_id: int, total_cost_usd: float) -> None: ...
    def set_judge_model(self, run_id: int, model: str) -> None: ...


@dataclass(slots=True)
class ListSink:
    """The test fake. Same shape as `CorpusStore` is to `QdrantStore`."""

    runs: list[dict[str, Any]] = field(default_factory=list)
    results: list[tuple[int, ExampleOutcome]] = field(default_factory=list)
    finished: list[tuple[int, float]] = field(default_factory=list)
    judge_models: list[tuple[int, str]] = field(default_factory=list)

    def create_run(self, **config: Any) -> int:
        self.runs.append(config)
        return len(self.runs)

    def write_result(self, run_id: int, outcome: ExampleOutcome) -> None:
        self.results.append((run_id, outcome))

    def finish_run(self, run_id: int, total_cost_usd: float) -> None:
        self.finished.append((run_id, total_cost_usd))

    def set_judge_model(self, run_id: int, model: str) -> None:
        self.judge_models.append((run_id, model))


class PostgresSink:
    """Production sink. One connection, committed PER RESULT.

    Committing per example rather than batching at the end is the whole
    resumability story: a 25-example run costs real money, and a crash at
    example 20 must not throw away nineteen paid-for rows.
    """

    def __init__(self, dataset_version: str, *, dsn: str | None = None) -> None:
        self._conn = connect(dsn)
        self._conn.autocommit = True
        self._dataset_version = dataset_version
        self._golden_ids = self._load_golden_ids()

    def _load_golden_ids(self) -> dict[str, int]:
        with self._conn.cursor() as cur:
            cur.execute(SELECT_GOLDEN_IDS, {"dataset_version": self._dataset_version})
            return {key: gid for key, gid in cur.fetchall()}

    def golden_id(self, example_key: str) -> int:
        try:
            return self._golden_ids[example_key]
        except KeyError:
            raise KeyError(
                f"golden example {example_key!r} is not loaded in dataset_version "
                f"{self._dataset_version!r} -- run `make eval-load` first"
            ) from None

    def prompt_version_id(self, version: str) -> int:
        with self._conn.cursor() as cur:
            cur.execute(SELECT_PROMPT_VERSION_ID, {"version": version})
            row = cur.fetchone()
        if row is None:
            raise KeyError(
                f"prompt version {version!r} is not in prompt_versions -- "
                f"run `make prompts-load` first"
            )
        return row[0]

    def create_run(self, **config: Any) -> int:
        params = dict(config)
        params["retriever_config_json"] = json.dumps(params["retriever_config_json"])
        with self._conn.cursor() as cur:
            cur.execute(INSERT_RUN, params)
            return cur.fetchone()[0]

    def write_result(self, run_id: int, outcome: ExampleOutcome) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                UPSERT_RESULT,
                {
                    "eval_run_id": run_id,
                    "golden_example_id": self.golden_id(outcome.example_key),
                    "hit_at_k": outcome.hit_at_k,
                    "citation_ok": outcome.citation_ok,
                    "faithfulness": outcome.faithfulness,
                    "answer_quality": outcome.answer_quality,
                    "judge_rationale": outcome.judge_rationale,
                    "answer_len_tokens": outcome.answer_len_tokens,
                    "latency_ms": outcome.latency_ms,
                    "trace_id": outcome.trace_id,
                    "session_id": outcome.session_id,
                    "answer": outcome.answer,
                    "cost_usd": outcome.cost_usd,
                    "retrieved_sources_json": json.dumps(outcome.retrieved_sources),
                    "judge_order": outcome.judge_order,
                    "judge_parse_ok": outcome.judge_parse_ok,
                    "hit_source": outcome.hit_source,
                    "error": outcome.error,
                },
            )

    def finish_run(self, run_id: int, total_cost_usd: float) -> None:
        with self._conn.cursor() as cur:
            cur.execute(FINISH_RUN, {"id": run_id, "total_cost_usd": total_cost_usd})

    def set_judge_model(self, run_id: int, model: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(SET_JUDGE_MODEL, {"id": run_id, "judge_model": model})

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> PostgresSink:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
