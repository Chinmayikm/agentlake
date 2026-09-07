"""The eval run: question in, graded row out.

Everything that touches the world is injected -- the gateway, the tool
executors, the sink, the judge -- so `tests/test_eval_harness.py` drives a
complete run against fakes and makes zero API calls. `eval/__main__.py` is what
builds the real objects, and it is the only place the expensive-step fence
lives.

Sequential by default. Each `StdioToolExecutor` spawns an MCP subprocess that
loads fastembed's ONNX model (~400-500 MB RSS), and three of those do not fit
beside the containers on a 3.9 GB box -- see ADR-008's runbook. `--concurrency`
raises it for a CI runner, which has 7 GB.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from eval.budget import BudgetGuard
from eval.dataset import GoldenExample
from eval.db import ExampleOutcome, ResultSink
from eval.judge import JudgeResult
from eval.metrics import (
    Retrieved,
    any_call_hit,
    citation_ok,
    first_call,
    hit_at_k,
    mean,
    pearson,
    rate,
)

#: Longer than services/agent's production 15s, and recorded in the run config.
#: Under concurrency a contended search_docs can exceed 15s, and a tool timeout
#: becomes a tool_result the model reasons around -- silently turning a machine
#: problem into what looks like a retrieval regression.
DEFAULT_TOOL_TIMEOUT = 30.0

#: Retries for a 429/503 from the gateway. Note that these do NOT arrive as
#: tool_results: HttpGatewayClient calls raise_for_status(), and run_turn's
#: try/except wraps only tool execution, so a rate limit propagates out as an
#: httpx.HTTPStatusError. ADR-003 #2 put retry policy explicitly out of scope
#: for the agent, so the harness owns it.
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2.0


class TurnRunner(Protocol):
    """`services.agent.loop.run_turn`, or a fake."""

    async def __call__(self, question: str, **kwargs: Any) -> Any: ...


class JudgeFn(Protocol):
    async def __call__(self, **kwargs: Any) -> JudgeResult: ...


@dataclass(slots=True)
class RunSummary:
    run_id: int
    n: int = 0
    failures: int = 0
    judge_parse_failures: int = 0
    outcomes: list[ExampleOutcome] = field(default_factory=list)
    wall_s: float = 0.0
    total_cost_usd: float = 0.0
    aborted: str = ""

    def metrics(self) -> dict[str, float | None]:
        """Everything the run measured. What is GATED is decided in
        eval/baseline.py, not here -- a metric's value and its authority to
        fail a build are separate questions."""
        scorable = [o for o in self.outcomes if o.error is None]
        answerable = [o for o in scorable if o.hit_source != "n/a"]
        return {
            "faithfulness_mean": mean([o.faithfulness for o in scorable]),
            "answer_quality_mean": mean([o.answer_quality for o in scorable]),
            # Counts "never searched" as a miss: that is what a user
            # experiences end to end.
            "hit_at_5_rate": rate([o.hit_at_k for o in answerable], none_counts_as=False),
            # Excludes it: that is what the retriever was actually asked.
            "hit_at_5_given_search": rate(
                [o.hit_at_k for o in answerable], none_counts_as=None
            ),
            "search_rate": rate([o.searched for o in answerable]),
            "any_call_hit_rate": rate([o.any_call_hit for o in answerable]),
            "citation_ok_rate": rate([o.citation_ok for o in scorable]),
            "truncation_rate": rate([o.truncated for o in scorable]),
            "mean_cost_usd": mean([o.cost_usd for o in scorable]),
            "p50_latency_ms": _percentile([o.latency_ms for o in scorable], 0.50),
            "p95_latency_ms": _percentile([o.latency_ms for o in scorable], 0.95),
            "length_r_quality": _length_r(scorable, "answer_quality"),
            "length_r_faithfulness": _length_r(scorable, "faithfulness"),
        }


def _percentile(values: Sequence[float], q: float) -> float | None:
    present = sorted(v for v in values if v is not None)
    if not present:
        return None
    index = min(len(present) - 1, round(q * (len(present) - 1)))
    return present[index]


def _length_r(outcomes: Sequence[ExampleOutcome], attribute: str) -> float | None:
    pairs = [
        (o.answer_len_tokens, getattr(o, attribute))
        for o in outcomes
        if getattr(o, attribute) is not None and o.answer_len_tokens
    ]
    if len(pairs) < 2:
        return None
    return pearson([p[0] for p in pairs], [p[1] for p in pairs])


def git_sha() -> str:
    """HEAD, with `-dirty` appended when the tree is not clean.

    `make eval` allows a dirty tree with a warning, because exploration is the
    normal case. `make eval-baseline` and `make eval-ci` refuse it: a baseline
    pinned to a commit nobody can check out is not a baseline.
    """
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=10
        )
        if sha.returncode != 0:
            return "unknown"
        status = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True, timeout=10
        )
        suffix = "-dirty" if status.stdout.strip() else ""
        return sha.stdout.strip() + suffix
    except (OSError, subprocess.SubprocessError):
        return "unknown"


async def _with_retries(coro_factory, *, on_retry) -> Any:
    """Run `coro_factory()`, retrying a rate limit or a transient upstream.

    Only 429 and 5xx: a 400 means the request is wrong and will be wrong again,
    and retrying it three times just spends three times as much on the same
    mistake.
    """
    import httpx

    delay = RETRY_BASE_DELAY
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await coro_factory()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status != 429 and status < 500:
                raise
            if attempt == MAX_RETRIES:
                raise
            retry_after = exc.response.headers.get("retry-after")
            wait = float(retry_after) if (retry_after or "").isdigit() else delay
            on_retry(attempt, status, wait)
            await asyncio.sleep(wait)
            delay *= 2
    raise AssertionError("unreachable")


def _to_retrieved(refs: Sequence[Any]) -> list[Retrieved]:
    return [
        Retrieved(
            source_path=getattr(r, "source_path", ""),
            section_path=getattr(r, "section_path", ""),
            call_index=getattr(r, "call_index", 0),
        )
        for r in refs
    ]


async def score_example(
    example: GoldenExample,
    *,
    run_turn: TurnRunner,
    judge_fn: JudgeFn | None,
    gateway,
    tool_executor,
    prompt_version: str,
    model_alias: str,
    max_steps: int,
    tool_timeout: float,
    k: int,
    seed: int,
    session_prefix: str,
    on_retry=lambda *a: None,
) -> ExampleOutcome:
    """One example, end to end. Never raises -- a failure becomes a row.

    A run that dies on example 12 of 25 has spent the money for twelve examples
    and produced nothing comparable. An error row with NULL scores keeps the
    other twenty-four, and `RunSummary.failures` makes the gap loud rather than
    letting it average in as a low score.
    """
    outcome = ExampleOutcome(example_key=example.key, hit_source="observed")
    started = time.perf_counter()
    try:
        result = await _with_retries(
            lambda: run_turn(
                example.question,
                gateway=gateway,
                tool_executor=tool_executor,
                session_id=f"{session_prefix}-{example.key}",
                model_alias=model_alias,
                max_steps=max_steps,
                tool_timeout=tool_timeout,
                prompt_version=prompt_version,
            ),
            on_retry=on_retry,
        )
    except Exception as exc:
        outcome.latency_ms = (time.perf_counter() - started) * 1000
        outcome.error = f"{type(exc).__name__}: {exc}"[:500]
        return outcome

    outcome.latency_ms = (time.perf_counter() - started) * 1000
    outcome.answer = result.answer
    outcome.trace_id = result.trace_id
    outcome.session_id = result.session_id
    outcome.cost_usd = result.total_cost_usd
    outcome.answer_len_tokens = result.final_completion_tokens
    outcome.truncated = result.truncated
    outcome.steps_used = result.steps_used

    retrieved = _to_retrieved(result.retrieved)
    outcome.searched = bool(result.retrieval_calls)
    outcome.retrieved_sources = sorted({r.source_path for r in first_call(retrieved)})
    outcome.citation_ok = citation_ok(result.answer, retrieved)

    if example.answerable:
        outcome.hit_at_k = hit_at_k(
            example.expected_sources, retrieved, k=k, searched=outcome.searched
        )
        outcome.any_call_hit = any_call_hit(example.expected_sources, retrieved)
    else:
        # An unanswerable example has no source to hit. Marking it "n/a" keeps
        # it out of the hit-rate denominator without pretending it was a miss.
        outcome.hit_source = "n/a"

    if judge_fn is not None:
        chunks = _judge_chunks(result.retrieved)
        verdict = await judge_fn(
            question=example.question,
            answer=result.answer,
            reference=example.expected_answer,
            chunks=chunks,
            example_key=example.key,
            seed=seed,
        )
        outcome.faithfulness = verdict.faithfulness.score
        outcome.answer_quality = verdict.answer_quality.score
        outcome.judge_rationale = verdict.rationale
        outcome.judge_order = verdict.answer_quality.order
        outcome.judge_parse_ok = verdict.parse_failures == 0
        outcome.cost_usd += verdict.cost_usd

    return outcome


def _judge_chunks(refs: Sequence[Any]) -> list[dict[str, str]]:
    """The passages the faithfulness judge grades against.

    Deduplicated by chunk_id and kept in the order the agent saw them. Across
    calls the agent frequently re-retrieves the same chunk, and showing it
    twice would spend context to tell the judge nothing.

    NOT restricted to the first search_docs call: hit@k is scored on the first
    call because that measures the retriever, but faithfulness asks whether the
    ANSWER is supported, and the answer may rest on anything the agent was
    shown. Judging it against a subset of its own evidence would score a
    correct answer as fabricated -- which is exactly what the pilot caught when
    the text was missing entirely.
    """
    seen: set[str] = set()
    chunks: list[dict[str, str]] = []
    for ref in refs:
        chunk_id = getattr(ref, "chunk_id", "")
        if chunk_id and chunk_id in seen:
            continue
        seen.add(chunk_id)
        chunks.append(
            {
                "source_path": getattr(ref, "source_path", ""),
                "section_path": getattr(ref, "section_path", ""),
                "text": getattr(ref, "text", ""),
            }
        )
    return chunks


async def run_eval(
    examples: Sequence[GoldenExample],
    *,
    run_turn: TurnRunner,
    gateway,
    tool_executors: Sequence[Any],
    sink: ResultSink,
    judge_fn: JudgeFn | None,
    prompt_version: str,
    corpus_version: str,
    dataset_version: str,
    subset: str,
    prompt_version_id: int,
    judge_model: str,
    retriever_config: dict[str, Any],
    model_alias: str = "fast",
    max_steps: int = 8,
    tool_timeout: float = DEFAULT_TOOL_TIMEOUT,
    k: int = 5,
    seed: int = 7,
    hit_source: str = "observed",
    budget: BudgetGuard | None = None,
    poll_cost=lambda: 0.0,
    notes: str = "",
) -> RunSummary:
    run_id = sink.create_run(
        git_sha=git_sha(),
        prompt_version_id=prompt_version_id,
        retriever_config_json=retriever_config,
        corpus_version=corpus_version,
        judge_model=judge_model,
        dataset_version=dataset_version,
        subset=subset,
        model_alias=model_alias,
        max_steps=max_steps,
        judge_seed=seed,
        hit_source=hit_source,
        notes=notes,
    )
    summary = RunSummary(run_id=run_id)
    started = time.perf_counter()
    executors = list(tool_executors) or [None]
    judge_model_recorded = False

    # Take the budget baseline BEFORE the first example, not after it. The
    # gateway's total is process-lifetime and may already include other
    # traffic, so a baseline is needed -- but establishing it on the first
    # post-example poll would make example 1's spend invisible to the cap.
    if budget is not None:
        budget.observe(poll_cost())

    for index, example in enumerate(examples):
        outcome = await score_example(
            example,
            run_turn=run_turn,
            judge_fn=judge_fn,
            gateway=gateway,
            tool_executor=executors[index % len(executors)],
            prompt_version=prompt_version,
            model_alias=model_alias,
            max_steps=max_steps,
            tool_timeout=tool_timeout,
            k=k,
            seed=seed,
            session_prefix=f"eval-{run_id}",
            on_retry=lambda attempt, status, wait: print(
                f"    rate limited ({status}); retry {attempt}/{MAX_RETRIES} in {wait:.0f}s"
            ),
        )

        # Committed immediately. A crash costs the example in flight and
        # nothing else -- which is the whole reason for the UNIQUE constraint
        # that makes this an upsert.
        sink.write_result(run_id, outcome)
        summary.outcomes.append(outcome)
        summary.n += 1
        if outcome.error:
            summary.failures += 1
        if outcome.judge_parse_ok is False:
            summary.judge_parse_failures += 1

        if not judge_model_recorded and outcome.judge_parse_ok is not None:
            judge_model_recorded = True

        print(_progress_line(index + 1, len(examples), example, outcome))

        if budget is not None:
            budget.observe(poll_cost())
            try:
                budget.check()
            except Exception as exc:  # BudgetExceeded
                summary.aborted = str(exc)
                break

    summary.wall_s = time.perf_counter() - started
    summary.total_cost_usd = (
        budget.run_cost if budget is not None else sum(o.cost_usd for o in summary.outcomes)
    )
    sink.finish_run(run_id, summary.total_cost_usd)
    return summary


def _progress_line(i: int, total: int, example: GoldenExample, outcome: ExampleOutcome) -> str:
    if outcome.error:
        return f"  [{i:>3}/{total}] {example.key:<28} FAILED  {outcome.error[:60]}"
    hit = {True: "hit", False: "miss", None: "no-search"}[outcome.hit_at_k]
    scores = (
        f"f={outcome.faithfulness or '-'} q={outcome.answer_quality or '-'}"
        if outcome.judge_parse_ok is not None
        else "unjudged"
    )
    return (
        f"  [{i:>3}/{total}] {example.key:<28} {hit:<9} {scores}  "
        f"${outcome.cost_usd:.4f}  {outcome.latency_ms / 1000:.1f}s"
    )


def format_summary(summary: RunSummary, *, label: str = "") -> str:
    metrics = summary.metrics()
    lines = [
        "",
        f"RUN {summary.run_id}{'  ' + label if label else ''}",
        "-" * 58,
        f"  examples            {summary.n}",
        f"  failures            {summary.failures}",
        f"  judge parse fails   {summary.judge_parse_failures}",
        "",
    ]
    for key in (
        "faithfulness_mean",
        "answer_quality_mean",
        "hit_at_5_rate",
        "hit_at_5_given_search",
        "search_rate",
        "any_call_hit_rate",
        "citation_ok_rate",
        "truncation_rate",
        "length_r_quality",
        "length_r_faithfulness",
    ):
        value = metrics[key]
        lines.append(f"  {key:<22}{'n/a' if value is None else f'{value:.4f}'}")
    lines += [
        "",
        f"  mean cost           ${metrics['mean_cost_usd'] or 0:.4f}",
        f"  total cost          ${summary.total_cost_usd:.4f}",
        f"  p50 / p95 latency   {(metrics['p50_latency_ms'] or 0) / 1000:.1f}s / "
        f"{(metrics['p95_latency_ms'] or 0) / 1000:.1f}s",
        f"  wall time           {summary.wall_s / 60:.1f} min",
        "-" * 58,
    ]
    if summary.aborted:
        lines += ["", summary.aborted]
    return "\n".join(lines)
