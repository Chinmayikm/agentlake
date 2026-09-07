"""Tests for eval/harness.py, eval/db.py and eval/budget.py.

**No gateway, no Postgres, no API key, no money.** A complete `run_eval` is
driven here through injected fakes -- a scripted turn runner, a `ListSink`, a
canned judge -- and one test asserts by construction that nothing in the module
can reach a real gateway.

That matters more than usual for this file: the code under test is the one
piece of the repo whose job is to spend money.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from eval.budget import AUTHORISED_USD, BudgetExceeded, BudgetGuard, Ledger
from eval.dataset import GoldenExample
from eval.db import ExampleOutcome, ListSink
from eval.harness import (
    MAX_RETRIES,
    RunSummary,
    _percentile,
    format_summary,
    git_sha,
    run_eval,
    score_example,
)
from eval.judge import JudgeResult, JudgeScore


def example(key: str = "kafka-x-001", *, answerable: bool = True) -> GoldenExample:
    return GoldenExample(
        key=key,
        project="kafka",
        question="does Kafka do exactly-once by default?",
        expected_answer="No, at-least-once by default.",
        expected_sources=("docs/design.html",) if answerable else (),
        written_from="docs/design.html#4.6" if answerable else "(not in the corpus)",
        difficulty="medium",
        type="conceptual",
        answerable=answerable,
    )


@dataclass
class Ref:
    source_path: str
    section_path: str = "S"
    score: float = 0.9
    call_index: int = 0
    text: str = "Kafka guarantees at-least-once delivery by default."

    @property
    def chunk_id(self) -> str:
        return f"{self.source_path}:{self.text}"


@dataclass
class FakeResult:
    answer: str = "Kafka is at-least-once by default; see design.html."
    truncated: bool = False
    steps_used: int = 2
    tools_called: list[str] = field(default_factory=lambda: ["search_docs"])
    session_id: str = "s1"
    trace_id: str = "t1"
    total_tokens: int = 100
    total_cost_usd: float = 0.05
    retrieved: list[Ref] = field(default_factory=lambda: [Ref("docs/design.html")])
    final_completion_tokens: int = 42
    retrieval_calls: list[dict] = field(default_factory=lambda: [{"k": 5, "mode": "hybrid"}])


def turn_runner(result: Any = None, *, raises: Exception | None = None):
    calls = []

    async def run(question, **kwargs):
        calls.append((question, kwargs))
        if raises:
            raise raises
        return result if result is not None else FakeResult()

    run.calls = calls  # type: ignore[attr-defined]
    return run


def judge_returning(faith: int | None = 4, quality: int | None = 5, *, parse_ok: bool = True):
    async def judge_fn(**kwargs):
        return JudgeResult(
            faithfulness=JudgeScore(faith, "r", parse_ok, "n/a", "claude-sonnet-5", 0.01),
            answer_quality=JudgeScore(
                quality, "r", parse_ok, "reference_first", "claude-sonnet-5", 0.01
            ),
        )

    return judge_fn


RUN_CONFIG = dict(
    prompt_version="v4",
    corpus_version="2026-08-27-pinned",
    dataset_version="v1",
    subset="ci",
    prompt_version_id=4,
    judge_model="claude-sonnet-5",
    retriever_config={"k": 5, "mode": "hybrid"},
)


# ---------------------------------------------------------------------------
# 1. The suite cannot spend money
# ---------------------------------------------------------------------------


def test_the_harness_module_never_constructs_a_real_gateway() -> None:
    """A source-text contract. Every IO seam is injected, so if this module
    ever imports HttpGatewayClient or PostgresSink directly, the "tests make
    zero API calls" property has been lost by construction rather than by
    accident."""
    from pathlib import Path

    source = Path("eval/harness.py").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    for forbidden in ("HttpGatewayClient(", "PostgresSink(", "StdioToolExecutor("):
        assert forbidden not in code, f"eval/harness.py constructs {forbidden}"
    assert "gateway_client import" not in code
    assert "ANTHROPIC_API_KEY" not in code


# ---------------------------------------------------------------------------
# 2. One example, scored
# ---------------------------------------------------------------------------


def _score(ex, run, judge_fn=None, **over):
    kwargs = dict(
        run_turn=run, judge_fn=judge_fn, gateway=None, tool_executor=None,
        prompt_version="v4", model_alias="fast", max_steps=8, tool_timeout=30.0,
        k=5, seed=7, session_prefix="eval-1",
    )
    kwargs.update(over)
    return asyncio.run(score_example(ex, **kwargs))


def test_a_scored_example_carries_everything_needed_to_audit_it() -> None:
    """retrieved_sources is stored because the hot path's trace has a 7-day
    TTL: a score whose evidence expired before the score did cannot be argued
    with, only believed."""
    outcome = _score(example(), turn_runner(), judge_returning())

    assert outcome.hit_at_k is True
    assert outcome.citation_ok is True
    assert outcome.faithfulness == 4
    assert outcome.answer_quality == 5
    assert outcome.judge_order == "reference_first"
    assert outcome.retrieved_sources == ["docs/design.html"]
    assert outcome.trace_id == "t1"
    assert outcome.answer_len_tokens == 42
    assert outcome.error is None


def test_judge_cost_is_added_to_the_turn_cost() -> None:
    """Judging is spend. Recording only the agent's half would make the ledger
    under-count by roughly a third."""
    outcome = _score(example(), turn_runner(), judge_returning())
    assert outcome.cost_usd == pytest.approx(0.05 + 0.02)


def test_hit_is_false_when_the_first_search_missed() -> None:
    result = FakeResult(retrieved=[Ref("docs/ops.html")])
    assert _score(example(), turn_runner(result)).hit_at_k is False


def test_hit_is_none_when_the_agent_never_searched() -> None:
    """The v5 case: a prompt that stops asking the agent to search must be
    diagnosable as that, not as a retrieval regression."""
    result = FakeResult(retrieved=[], retrieval_calls=[])
    outcome = _score(example(), turn_runner(result))

    assert outcome.hit_at_k is None
    assert outcome.searched is False


def test_an_unanswerable_example_is_excluded_from_the_hit_denominator() -> None:
    """It has no source to hit. Marking it n/a keeps it out of the rate without
    pretending it was a miss -- it is still judged on faithfulness, which is
    the whole reason it exists."""
    outcome = _score(example("kafka-unanswerable-001", answerable=False), turn_runner())

    assert outcome.hit_source == "n/a"
    assert outcome.hit_at_k is None


def test_a_failing_example_becomes_a_row_rather_than_a_crash() -> None:
    """A run that dies at example 12 of 25 has paid for twelve and produced
    nothing comparable. `failures` makes the gap loud instead of letting an
    error average in as a low score."""
    outcome = _score(example(), turn_runner(raises=RuntimeError("gateway exploded")))

    assert outcome.error is not None
    assert "gateway exploded" in outcome.error
    assert outcome.faithfulness is None
    assert outcome.latency_ms > 0


def test_the_harness_passes_the_prompt_version_through_to_the_turn() -> None:
    """The whole v4-vs-v5 comparison rests on this argument reaching run_turn."""
    run = turn_runner()
    _score(example(), run, prompt_version="v5")
    assert run.calls[0][1]["prompt_version"] == "v5"


# ---------------------------------------------------------------------------
# 3. Retry on a rate limit -- which does NOT arrive as a tool_result
# ---------------------------------------------------------------------------


def _no_sleep(monkeypatch) -> None:
    """Make backoff instant.

    The real asyncio.sleep has to be captured FIRST -- a lambda that calls
    `asyncio.sleep` after the patch calls itself, and the RecursionError gets
    swallowed as a failed example, which looks exactly like "retries did not
    happen".
    """
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda _: real_sleep(0))


def _http_error(status: int, retry_after: str | None = None):
    import httpx

    request = httpx.Request("POST", "http://localhost:8100/v1/chat")
    headers = {"retry-after": retry_after} if retry_after else {}
    response = httpx.Response(status, request=request, headers=headers)
    return httpx.HTTPStatusError("boom", request=request, response=response)


def test_a_rate_limit_is_retried_and_then_succeeds(monkeypatch) -> None:
    """HttpGatewayClient calls raise_for_status() and run_turn's try/except
    wraps only tool execution, so a 429 propagates OUT of the turn. ADR-003 #2
    put retries out of scope for the agent, so the harness owns them."""
    _no_sleep(monkeypatch)
    attempts = {"n": 0}

    async def flaky(question, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise _http_error(429, "1")
        return FakeResult()

    outcome = _score(example(), flaky)

    assert attempts["n"] == 2
    assert outcome.error is None


def test_a_bad_request_is_not_retried(monkeypatch) -> None:
    """A 400 will be a 400 again. Retrying it three times spends three times as
    much on the same mistake."""
    _no_sleep(monkeypatch)
    attempts = {"n": 0}

    async def bad(question, **kwargs):
        attempts["n"] += 1
        raise _http_error(400)

    outcome = _score(example(), bad)

    assert attempts["n"] == 1
    assert outcome.error is not None


def test_retries_are_bounded(monkeypatch) -> None:
    _no_sleep(monkeypatch)
    attempts = {"n": 0}

    async def always_limited(question, **kwargs):
        attempts["n"] += 1
        raise _http_error(429)

    assert _score(example(), always_limited).error is not None
    assert attempts["n"] == MAX_RETRIES


# ---------------------------------------------------------------------------
# 4. A whole run, and the per-example commit that makes resume free
# ---------------------------------------------------------------------------


def _run(examples, **over):
    sink = ListSink()
    kwargs = dict(
        run_turn=turn_runner(), gateway=None, tool_executors=[None], sink=sink,
        judge_fn=judge_returning(), **RUN_CONFIG,
    )
    kwargs.update(over)
    summary = asyncio.run(run_eval(examples, **kwargs))
    return summary, sink


def test_a_run_writes_every_result_and_finishes_the_run_row() -> None:
    examples = [example(f"kafka-x-{i:03d}") for i in range(3)]
    summary, sink = _run(examples)

    assert summary.n == 3
    assert len(sink.results) == 3
    assert sink.finished == [(1, pytest.approx(0.21))]
    assert sink.runs[0]["prompt_version_id"] == 4
    assert sink.runs[0]["dataset_version"] == "v1"


def test_each_result_is_written_before_the_next_example_runs() -> None:
    """The whole resumability story: a crash costs the example in flight and
    nothing else. If results were batched at the end, a 25-example run dying at
    24 would throw away everything it had paid for."""
    seen: list[int] = []
    examples = [example(f"kafka-x-{i:03d}") for i in range(3)]

    class RecordingSink(ListSink):
        def write_result(self, run_id, outcome):
            seen.append(len(self.results))
            super().write_result(run_id, outcome)

    sink = RecordingSink()
    asyncio.run(
        run_eval(
            examples, run_turn=turn_runner(), gateway=None, tool_executors=[None],
            sink=sink, judge_fn=judge_returning(), **RUN_CONFIG,
        )
    )
    assert seen == [0, 1, 2]


def test_run_metrics_separate_the_two_hit_rate_questions() -> None:
    """hit_at_5_rate counts "never searched" as a miss (what a user
    experiences); hit_at_5_given_search excludes it (what the retriever was
    asked). Reporting only one of them would hide which question was answered."""
    hit = FakeResult(retrieved=[Ref("docs/design.html")])
    miss = FakeResult(retrieved=[Ref("docs/ops.html")])
    no_search = FakeResult(retrieved=[], retrieval_calls=[])
    results = iter([hit, miss, no_search])

    async def run(question, **kwargs):
        return next(results)

    summary, _ = _run([example(f"kafka-x-{i:03d}") for i in range(3)], run_turn=run)
    metrics = summary.metrics()

    assert metrics["hit_at_5_rate"] == pytest.approx(1 / 3)        # 1 of 3
    assert metrics["hit_at_5_given_search"] == pytest.approx(0.5)  # 1 of 2
    assert metrics["search_rate"] == pytest.approx(2 / 3)


def test_a_judge_parse_failure_is_counted_and_does_not_become_a_score() -> None:
    summary, _ = _run([example()], judge_fn=judge_returning(None, None, parse_ok=False))

    assert summary.judge_parse_failures == 1
    assert summary.metrics()["faithfulness_mean"] is None


def test_failures_are_excluded_from_quality_metrics() -> None:
    """An error is a harness problem. Averaging it in as a zero would report it
    as a quality regression."""
    summary, _ = _run([example()], run_turn=turn_runner(raises=RuntimeError("x")))

    assert summary.failures == 1
    assert summary.metrics()["faithfulness_mean"] is None


def test_format_summary_renders_without_a_judge() -> None:
    summary, _ = _run([example()], judge_fn=None)
    rendered = format_summary(summary)
    assert "faithfulness_mean" in rendered and "n/a" in rendered


# ---------------------------------------------------------------------------
# 5. The budget guard
# ---------------------------------------------------------------------------


def test_preflight_refuses_a_run_the_arithmetic_says_cannot_finish(tmp_path) -> None:
    """"Do not start a run that projected math says can't finish" -- refusing
    BEFORE spending is the only point at which that promise can be kept."""
    ledger = Ledger(path=tmp_path / "spend.json")
    ledger.record("earlier run", 4.00)
    guard = BudgetGuard(ledger=ledger, label="baseline A", estimate_usd=1.50, cap_usd=4.25)

    with pytest.raises(BudgetExceeded, match="REFUSED before spending anything"):
        guard.preflight()


def test_preflight_allows_a_run_that_fits(tmp_path) -> None:
    ledger = Ledger(path=tmp_path / "spend.json")
    ledger.record("pilot", 0.30)
    BudgetGuard(ledger=ledger, label="baseline A", estimate_usd=1.50).preflight()


def test_the_first_observation_is_a_baseline_not_spend(tmp_path) -> None:
    """`make gateway` is usually already running and may have served other
    traffic. Counting its lifetime total as this run's cost would charge the
    eval for someone else's requests."""
    guard = BudgetGuard(ledger=Ledger(path=tmp_path / "s.json"), label="r", estimate_usd=1.0)

    assert guard.observe(2.50) == 0.0        # gateway had already spent 2.50
    assert guard.observe(2.75) == pytest.approx(0.25)


def test_the_guard_aborts_once_the_cap_is_crossed(tmp_path) -> None:
    ledger = Ledger(path=tmp_path / "s.json")
    ledger.record("earlier", 4.00)
    guard = BudgetGuard(ledger=ledger, label="r", estimate_usd=0.20, cap_usd=4.25)
    guard.observe(0.0)
    guard.observe(0.30)

    with pytest.raises(BudgetExceeded, match="ABORTED mid-run"):
        guard.check()


def test_a_run_stops_at_the_cap_and_keeps_what_it_completed(tmp_path) -> None:
    """The abort must not throw away paid-for rows -- they are already
    committed, and the summary says why it stopped."""
    ledger = Ledger(path=tmp_path / "s.json")
    ledger.record("earlier", 4.20)
    guard = BudgetGuard(ledger=ledger, label="r", estimate_usd=0.01, cap_usd=4.25)
    # First value is the pre-run baseline; the second is after example 1,
    # which takes the total past the cap.
    costs = iter([0.0, 0.10, 0.20, 0.30])

    summary, sink = _run(
        [example(f"kafka-x-{i:03d}") for i in range(4)],
        budget=guard,
        poll_cost=lambda: next(costs),
    )

    assert summary.aborted
    assert "ABORTED mid-run" in summary.aborted
    assert summary.n == 1, "the abort must stop the run, not merely be noted"
    assert len(sink.results) == 1, "the completed example is still committed"


def test_the_ledger_round_trips_and_totals(tmp_path) -> None:
    """/v1/stats is process-lifetime and resets on gateway restart, so a cap
    enforced against it alone would silently reset every restart. The ledger is
    what makes the total survive."""
    path = tmp_path / "spend.json"
    ledger = Ledger(path=path)
    ledger.record("pilot", 0.30, "3 examples")
    ledger.record("baseline A", 1.55)

    reloaded = Ledger.load(path)

    assert reloaded.spent == pytest.approx(1.85)
    assert [e.label for e in reloaded.entries] == ["pilot", "baseline A"]
    assert "baseline A" in reloaded.render()


def test_the_cap_leaves_headroom_under_what_was_authorised() -> None:
    """The gap is deliberate: an abort AT the cap must still leave room for the
    in-flight example to finish being recorded."""
    from eval.budget import DEFAULT_CAP_USD

    assert DEFAULT_CAP_USD < AUTHORISED_USD


# ---------------------------------------------------------------------------
# 6. Odds and ends
# ---------------------------------------------------------------------------


def test_percentile_matches_hand_computed_values() -> None:
    assert _percentile([10, 20, 30, 40, 50], 0.50) == 30
    assert _percentile([10, 20, 30, 40, 50], 0.95) == 50
    assert _percentile([], 0.5) is None


def test_git_sha_marks_a_dirty_tree() -> None:
    """make eval-baseline refuses a dirty tree: a baseline pinned to a commit
    nobody can check out is not a baseline."""
    sha = git_sha()
    assert sha == "unknown" or len(sha.split("-")[0]) == 40


def test_run_summary_metrics_are_all_none_for_an_empty_run() -> None:
    """An empty run must not report 0.0 for everything -- that is a number, and
    a number would be compared against the baseline."""
    metrics = RunSummary(run_id=1).metrics()
    assert all(v is None for v in metrics.values())


def test_example_outcome_defaults_are_null_not_zero() -> None:
    outcome = ExampleOutcome(example_key="k")
    assert outcome.faithfulness is None
    assert outcome.hit_at_k is None
    assert outcome.judge_parse_ok is None


# ---------------------------------------------------------------------------
# 7. The judge sees the evidence (the bug the pilot caught)
# ---------------------------------------------------------------------------


def test_the_judge_receives_the_chunk_text_not_just_the_paths() -> None:
    """The pilot found this the expensive way: RetrievedRef carried no `text`,
    so the faithfulness judge was handed passage HEADERS with empty bodies and
    correctly scored every answer as fabricated -- faithfulness 1.67 against a
    hit rate of 1.00, which is incoherent on its face.

    The RETRIEVAL span records which documents came back, not what they said,
    so AgentResult is the only in-process copy of the evidence.
    """
    captured: dict = {}

    inner = judge_returning()

    async def judge_fn(**kwargs):
        captured.update(kwargs)
        return await inner(**kwargs)

    result = FakeResult(retrieved=[Ref("docs/design.html", text="THE EVIDENCE")])
    _score(example(), turn_runner(result), judge_fn)

    assert captured["chunks"] == [
        {
            "source_path": "docs/design.html",
            "section_path": "S",
            "text": "THE EVIDENCE",
        }
    ]


def test_repeated_chunks_are_shown_to_the_judge_once() -> None:
    """Across calls an agent frequently re-retrieves the same chunk. Showing it
    twice spends context to tell the judge nothing."""
    from eval.harness import _judge_chunks

    chunks = _judge_chunks(
        [Ref("a.md", text="one"), Ref("a.md", text="one"), Ref("b.md", text="two")]
    )
    assert len(chunks) == 2


def test_the_judge_sees_every_call_not_just_the_first() -> None:
    """hit@k is scored on the first search_docs call, because that measures the
    RETRIEVER. Faithfulness asks whether the ANSWER is supported, and the
    answer may rest on anything the agent was shown -- judging it against a
    subset of its own evidence would score a correct answer as fabricated."""
    from eval.harness import _judge_chunks

    chunks = _judge_chunks(
        [Ref("a.md", call_index=0, text="first"), Ref("b.md", call_index=3, text="later")]
    )
    assert [c["text"] for c in chunks] == ["first", "later"]
