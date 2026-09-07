"""The regression gate, reading a FINISHED run out of the metadata database.

`python -m eval run --gate` gates a run it just executed, in-process. This
module gates a run that already happened: `make eval-gate RUN=3` reads
`eval_results` back and compares it to `eval/baseline.json`. That split matters
for two reasons.

  - A gated run costs ~$1. Re-running it to re-check a threshold spends a
    dollar to answer a question the rows on disk already answer.
  - A run that ABORTED (budget, timeout, Ctrl-C) never reaches the in-process
    gate at all, and its paid-for rows would otherwise be ungradeable. Run 4 is
    exactly that case -- see ADR-008 #14.

## What is gated here, and why it differs from eval/baseline.py's floors

`eval/baseline.py`'s floors (0.20 / 0.08) are derived from `sigma` measured
across TWO identical runs. The current `eval/baseline.json` rests on ONE run
(run 3; run 4 aborted at 17 of 25), so **there is no sigma and the floors have
nothing behind them**. Rather than present an underived floor as if it were
calibrated, this gate uses wider, explicitly HAND-SET thresholds and says so:

    faithfulness_mean   -0.30   (~7.5 single-point judge flips at n=25)
    hit_at_5_rate       -0.10   (~2.4 examples at n=24 answerable)
    citation_ok_rate    ANY drop

citation_ok is gated here even though ADR-008 #6 says it is a smoke detector
and its VALUE is not to be believed. That is not a contradiction: the gate is
on the DELTA, at zero tolerance, and run 3 measured 1.0000. A smoke detector
that has never once gone off is a fine thing to wire to an alarm; what would be
wrong is gating on "citation_ok >= 0.8" as though 0.8 meant something.

These thresholds are stored IN baseline.json when it is written, so the gate
reads them rather than hardcoding them -- swapping in a two-run baseline swaps
in its measured thresholds with no code change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from eval.baseline import Baseline
from eval.metrics import mean, pearson, rate

#: The fallback for a baseline written before thresholds were stored. Any
#: metric absent from `baseline.thresholds` falls back to these; a metric in
#: neither is not gated at all.
DECLARED_THRESHOLDS: dict[str, float] = {
    "faithfulness_mean": 0.30,
    "hit_at_5_rate": 0.10,
    "citation_ok_rate": 0.0,
}

#: Order is the report's order. Gated one-sided: only a DROP fails.
GATED = ("faithfulness_mean", "hit_at_5_rate", "citation_ok_rate")

REPORTED = (
    "answer_quality_mean",
    "hit_at_5_given_search",
    "mean_cost_usd",
    "p50_latency_ms",
    "p95_latency_ms",
    "length_r_quality",
    "length_r_faithfulness",
)

#: A mismatch here is not a regression and must not be reported as one --
#: same argument as eval/baseline.py's PINNED.
PINNED = ("dataset_version", "corpus_version", "prompt_version", "judge_model", "n")

SELECT_RUN = """
SELECT r.id, r.git_sha, r.corpus_version, r.judge_model, r.dataset_version, r.subset,
       r.model_alias, r.max_steps, r.judge_seed, r.hit_source, r.started_at,
       r.finished_at, r.total_cost_usd, r.notes, pv.version
FROM eval_runs r
LEFT JOIN prompt_versions pv ON pv.id = r.prompt_version_id
WHERE r.id = %(id)s
"""

SELECT_ROWS = """
SELECT hit_at_k, citation_ok, faithfulness, answer_quality, answer_len_tokens,
       latency_ms, cost_usd, hit_source, error, judge_parse_ok, retrieved_sources_json
FROM eval_results
WHERE eval_run_id = %(id)s
ORDER BY golden_example_id
"""

LATEST_FINISHED = "SELECT id FROM eval_runs WHERE finished_at IS NOT NULL ORDER BY id DESC LIMIT 1"


class RunNotFound(LookupError):
    pass


@dataclass(slots=True)
class StoredRun:
    """A finished run, reduced to what the gate needs."""

    run_id: int
    config: dict[str, Any]
    metrics: dict[str, float | None]
    n: int
    failures: int
    judge_parse_failures: int
    total_cost_usd: float
    finished: bool


def _percentile(values: list[float], q: float) -> float | None:
    present = sorted(v for v in values if v is not None)
    if not present:
        return None
    return present[min(len(present) - 1, round(q * (len(present) - 1)))]


def _length_r(rows: list[dict[str, Any]], key: str) -> float | None:
    pairs = [
        (float(r["answer_len_tokens"]), float(r[key]))
        for r in rows
        if r.get(key) is not None and r.get("answer_len_tokens")
    ]
    if len(pairs) < 2:
        return None
    return pearson([p[0] for p in pairs], [p[1] for p in pairs])


def metrics_from_rows(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    """The same arithmetic as `RunSummary.metrics()`, over stored rows.

    Three metrics the in-process summary reports are ABSENT rather than zero:
    `search_rate`, `any_call_hit_rate` and `truncation_rate` are computed from
    fields `eval_results` does not persist (ADR-008 #6 kept the table to what
    a row means, not to every counter the runner held). Emitting 0.0 for them
    would be a fabricated number; None says "not measurable from here", and
    `compare()` prints n/a.
    """
    scorable = [r for r in rows if r["error"] is None]
    answerable = [r for r in scorable if r["hit_source"] != "n/a"]
    return {
        "faithfulness_mean": mean([r["faithfulness"] for r in scorable]),
        "answer_quality_mean": mean([r["answer_quality"] for r in scorable]),
        "hit_at_5_rate": rate([r["hit_at_k"] for r in answerable], none_counts_as=False),
        "hit_at_5_given_search": rate([r["hit_at_k"] for r in answerable], none_counts_as=None),
        "search_rate": None,
        "any_call_hit_rate": None,
        "citation_ok_rate": rate([r["citation_ok"] for r in scorable]),
        "truncation_rate": None,
        "mean_cost_usd": mean(
            [float(r["cost_usd"]) for r in scorable if r["cost_usd"] is not None]
        ),
        "p50_latency_ms": _percentile([r["latency_ms"] for r in scorable], 0.50),
        "p95_latency_ms": _percentile([r["latency_ms"] for r in scorable], 0.95),
        "length_r_quality": _length_r(scorable, "answer_quality"),
        "length_r_faithfulness": _length_r(scorable, "faithfulness"),
    }


def load_run(cur, run_id: int | str) -> StoredRun:
    """Read one run and its results. `run_id` may be 'latest'."""
    if run_id == "latest":
        cur.execute(LATEST_FINISHED)
        row = cur.fetchone()
        if row is None:
            raise RunNotFound("no finished eval run to gate")
        run_id = row[0]
    run_id = int(run_id)

    cur.execute(SELECT_RUN, {"id": run_id})
    run = cur.fetchone()
    if run is None:
        raise RunNotFound(f"eval run {run_id} does not exist")

    cur.execute(SELECT_ROWS, {"id": run_id})
    columns = [d[0] for d in cur.description]
    rows = [dict(zip(columns, r, strict=True)) for r in cur.fetchall()]
    if not rows:
        raise RunNotFound(f"eval run {run_id} has no results")

    return StoredRun(
        run_id=run_id,
        config={
            "git_sha": run[1],
            "corpus_version": run[2],
            "judge_model": run[3],
            "dataset_version": run[4],
            "subset": run[5],
            "model_alias": run[6],
            "max_steps": run[7],
            "judge_seed": run[8],
            "prompt_version": run[14],
            "n": len(rows),
        },
        metrics=metrics_from_rows(rows),
        n=len(rows),
        failures=sum(1 for r in rows if r["error"] is not None),
        judge_parse_failures=sum(1 for r in rows if r["judge_parse_ok"] is False),
        total_cost_usd=float(run[12] or 0.0),
        finished=run[11] is not None,
    )


@dataclass(slots=True)
class GateResult:
    passed: bool
    config_drift: list[str] = field(default_factory=list)
    regressions: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)

    def render(self) -> str:
        return "\n".join(self.lines)


def thresholds_for(baseline: Baseline) -> dict[str, float]:
    return {key: baseline.thresholds.get(key, DECLARED_THRESHOLDS[key]) for key in GATED}


def check(
    baseline: Baseline, run: StoredRun, *, allow_config_drift: bool = False
) -> GateResult:
    limits = thresholds_for(baseline)
    lines = [
        "",
        f"EVAL GATE -- run {run.run_id} vs eval/baseline.json",
        "=" * 72,
        f"  baseline: {baseline.config.get('n')} examples from run(s) "
        f"{baseline.config.get('run_ids')} @ {str(baseline.config.get('git_sha'))[:12]}",
        f"  current : {run.n} examples @ {str(run.config.get('git_sha'))[:12]}"
        f"{'' if run.finished else '   (UNFINISHED RUN)'}",
        "",
    ]

    drift = [
        f"{key}: baseline {baseline.config.get(key)!r} != run {run.config.get(key)!r}"
        for key in PINNED
        if baseline.config.get(key) != run.config.get(key)
    ]
    if drift and not allow_config_drift:
        lines += [
            "  CONFIG DRIFT -- these runs are not comparable:",
            *(f"    {d}" for d in drift),
            "",
            "  Comparing across configurations is the most common way eval numbers",
            "  lie. Pass --allow-config-drift if you genuinely mean to compare these.",
            "=" * 72,
        ]
        return GateResult(passed=False, config_drift=drift, lines=lines)
    if drift:
        lines += ["  config drift ALLOWED by flag:", *(f"    {d}" for d in drift), ""]

    regressions: list[str] = []
    lines.append(f"  {'metric':<24}{'baseline':>10}{'run':>10}{'delta':>10}{'limit':>10}   verdict")
    for key in GATED:
        base, now, limit = baseline.metrics.get(key), run.metrics.get(key), limits[key]
        if base is None or now is None:
            lines.append(f"  {key:<24}{'n/a':>10}{'n/a':>10}{'':>10}{'':>10}   MISSING")
            regressions.append(f"{key}: missing from {'baseline' if base is None else 'run'}")
            continue
        delta = now - base
        # One-sided. An improvement is reported, never adopted: a gate that
        # ratchets itself up on a lucky run cannot tell a gain from noise.
        failed = delta < -limit
        lines.append(
            f"  {key:<24}{base:>10.4f}{now:>10.4f}{delta:>+10.4f}{-limit:>10.4f}   "
            f"{'FAIL' if failed else 'ok'}"
        )
        if failed:
            regressions.append(f"{key}: {base:.4f} -> {now:.4f} ({delta:+.4f}, limit {-limit:.4f})")

    lines += ["", "  reported, never gated:"]
    for key in REPORTED:
        base, now = baseline.metrics.get(key), run.metrics.get(key)
        fmt = lambda v: "n/a" if v is None else f"{v:.4f}"  # noqa: E731
        lines.append(f"  {key:<24}{fmt(base):>12}{fmt(now):>12}")

    lines += [
        "",
        f"  failures {run.failures}   judge parse failures {run.judge_parse_failures}"
        f"   total ${run.total_cost_usd:.4f}",
        "=" * 72,
        "  RESULT: " + ("FAIL" if regressions else "PASS"),
    ]
    lines += [f"    regression: {r}" for r in regressions]
    lines.append("")
    return GateResult(passed=not regressions, regressions=regressions, lines=lines)
