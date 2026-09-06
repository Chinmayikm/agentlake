"""The regression gate: thresholds derived from two identical runs.

## The arithmetic

`make eval-baseline` runs the 25-example CI subset TWICE, identically -- same
commit, prompt version, corpus version, dataset version, judge model and seed.
For two observations the sample standard deviation is

    s = |x1 - x2| / sqrt(2)

(from s^2 = sum (xi - xbar)^2 / (n-1) = 2 * (d/2)^2 / 1 = d^2 / 2). The gate
threshold per metric is then

    T = max(2s, floor) = max(sqrt(2) * |d|, floor)

## The floors, and why they carry the gate

The judge emits integers, so one example flipping by one point moves a
25-example mean by exactly 1/25 = 0.04. That discreteness is what sets the
floors, rather than taste:

  - faithfulness_mean, floor 0.20 -- five single-point flips. The smallest
    change that cannot be a handful of borderline 3-vs-4 calls.
  - hit_at_5_rate, floor 0.08 = 2/25 -- one example flipping is 0.04, so two is
    the smallest movement that cannot be a single flaky retrieval.

**And the floors are doing most of the work.** sigma-hat from n=2 has one
degree of freedom; the chi-squared 95% interval on sigma at 1 df spans roughly
[0.45 s, 32 s] -- an order of magnitude either way. The 2s term is a sanity
check, not a calibration, and ADR-008 says so rather than presenting a
two-sample estimate as if it were tight.

The rule that follows: **if 2s exceeds twice the floor, do not widen the
threshold.** That reading means the harness is too noisy to gate at this n, and
the answer is more examples, not more tolerance. `compare()` says so.

## Why 25 fixed examples is defensible

Binomial sampling error at n=25 would be about 0.08 on a hit rate near 0.8 --
the size of the floor. That does not apply here, because the SAME 25 questions
run every time; there is no resampling. The only run-to-run variance is the
agent's and the judge's nondeterminism (no temperature is set; both sample at
provider defaults), and that is exactly what the two baseline runs measure.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BASELINE_PATH = Path(__file__).parent / "baseline.json"

SCHEMA = 1

#: Gated, one-sided: only a regression fails. An improvement is reported and
#: prompts a deliberate `make eval-baseline`, because a gate that auto-adopts
#: every improvement cannot distinguish a real gain from a lucky run.
GATED = ("faithfulness_mean", "hit_at_5_rate")

#: Derived from 1/25 = 0.04 -- see the module docstring.
FLOORS = {"faithfulness_mean": 0.20, "hit_at_5_rate": 0.08}

#: Reported on every comparison, gated on none of them.
REPORTED = (
    "answer_quality_mean",
    "hit_at_5_given_search",
    "search_rate",
    "citation_ok_rate",
    "truncation_rate",
    "mean_cost_usd",
    "p50_latency_ms",
    "length_r_quality",
)

#: Changing any of these makes two runs incomparable, so a mismatch fails on a
#: DIFFERENT exit path from a quality regression. Comparing across
#: configurations is the most common way eval numbers lie.
PINNED = ("dataset_version", "corpus_version", "prompt_version", "judge_model", "n")


class BaselineError(RuntimeError):
    """A baseline that cannot be compared against, for a stated reason."""


def sigma_from_two(a: float, b: float) -> float:
    """Sample standard deviation of exactly two observations."""
    return abs(a - b) / math.sqrt(2)


def threshold(sigma: float, floor: float) -> float:
    return max(2 * sigma, floor)


@dataclass(slots=True)
class Baseline:
    config: dict[str, Any]
    runs: list[dict[str, float | None]]
    metrics: dict[str, float | None]
    sigma: dict[str, float]
    thresholds: dict[str, float]
    schema: int = SCHEMA
    notes: str = ""

    @classmethod
    def from_runs(
        cls, run_a: dict[str, float | None], run_b: dict[str, float | None], config: dict[str, Any]
    ) -> Baseline:
        sigma: dict[str, float] = {}
        metrics: dict[str, float | None] = {}
        for key in set(run_a) | set(run_b):
            a, b = run_a.get(key), run_b.get(key)
            if a is None or b is None:
                metrics[key] = a if b is None else b
                continue
            metrics[key] = (a + b) / 2
            sigma[key] = sigma_from_two(a, b)
        thresholds = {
            key: threshold(sigma.get(key, 0.0), FLOORS[key]) for key in GATED if key in metrics
        }
        return cls(
            config=config,
            runs=[run_a, run_b],
            metrics=metrics,
            sigma=sigma,
            thresholds=thresholds,
        )

    def save(self, path: Path = BASELINE_PATH) -> None:
        path.write_text(json.dumps(_asdict(self), indent=2) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path = BASELINE_PATH) -> Baseline:
        if not path.is_file():
            raise BaselineError(
                f"no baseline at {path}. Run `make eval-baseline` -- the gate cannot "
                f"compare against a baseline that does not exist, and defaulting to "
                f"'pass' would make the gate decorative."
            )
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema") != SCHEMA:
            raise BaselineError(
                f"baseline schema {raw.get('schema')} != {SCHEMA}; regenerate it"
            )
        return cls(
            config=raw["config"],
            runs=raw["runs"],
            metrics=raw["metrics"],
            sigma=raw["sigma"],
            thresholds=raw["thresholds"],
            schema=raw["schema"],
            notes=raw.get("notes", ""),
        )


def _asdict(baseline: Baseline) -> dict[str, Any]:
    return {
        "schema": baseline.schema,
        "config": baseline.config,
        "runs": baseline.runs,
        "metrics": baseline.metrics,
        "sigma": baseline.sigma,
        "thresholds": baseline.thresholds,
        "gated": list(GATED),
        "floors": FLOORS,
        "notes": baseline.notes,
    }


@dataclass(slots=True)
class Comparison:
    passed: bool
    config_drift: list[str] = field(default_factory=list)
    regressions: list[str] = field(default_factory=list)
    improvements: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)

    def render(self) -> str:
        return "\n".join(self.lines)


def compare(
    baseline: Baseline,
    current: dict[str, float | None],
    config: dict[str, Any],
    *,
    allow_config_drift: bool = False,
) -> Comparison:
    """Current run against the pinned baseline.

    Fails on a gated regression, and SEPARATELY on config drift -- because
    "faithfulness dropped 0.3" and "you compared v4 against v5" need different
    responses, and reporting the second as the first sends someone hunting a
    regression that is not there.
    """
    lines = ["", "GATE", "-" * 68]
    drift = [
        f"{key}: baseline {baseline.config.get(key)!r} != current {config.get(key)!r}"
        for key in PINNED
        if baseline.config.get(key) != config.get(key)
    ]
    if drift and not allow_config_drift:
        lines += ["  CONFIG DRIFT -- these runs are not comparable:", *(f"    {d}" for d in drift)]
        lines += [
            "",
            "  Comparing across configurations is the most common way eval numbers",
            "  lie. Re-baseline deliberately, or pass --allow-config-drift if you",
            "  genuinely mean to compare these.",
            "-" * 68,
        ]
        return Comparison(passed=False, config_drift=drift, lines=lines)

    regressions: list[str] = []
    improvements: list[str] = []

    lines.append(f"  {'metric':<24}{'baseline':>10}{'current':>10}{'delta':>10}{'threshold':>12}")
    for key in GATED:
        base, now = baseline.metrics.get(key), current.get(key)
        limit = baseline.thresholds.get(key, FLOORS[key])
        if base is None or now is None:
            lines.append(f"  {key:<24}{'n/a':>10}{'n/a':>10}{'':>10}{'':>12}   MISSING")
            regressions.append(f"{key}: missing from {'baseline' if base is None else 'run'}")
            continue
        delta = now - base
        # One-sided: only a drop fails.
        failed = delta < -limit
        verdict = "FAIL" if failed else ("improved" if delta > limit else "ok")
        lines.append(
            f"  {key:<24}{base:>10.4f}{now:>10.4f}{delta:>+10.4f}{limit:>12.4f}   {verdict}"
        )
        if failed:
            regressions.append(f"{key}: {base:.4f} -> {now:.4f} ({delta:+.4f}, limit {limit:.4f})")
        elif delta > limit:
            improvements.append(f"{key}: {base:.4f} -> {now:.4f} ({delta:+.4f})")

    lines += ["", "  reported (never gated):"]
    for key in REPORTED:
        base, now = baseline.metrics.get(key), current.get(key)
        fmt = lambda v: "n/a" if v is None else f"{v:.4f}"  # noqa: E731
        lines.append(f"  {key:<24}{fmt(base):>10}{fmt(now):>10}")

    # The noise rule from the module docstring, stated where it is actionable.
    noisy = [
        key
        for key in GATED
        if baseline.sigma.get(key, 0.0) * 2 > 2 * FLOORS[key]
    ]
    if noisy:
        lines += [
            "",
            f"  NOTE: 2*sigma exceeds twice the floor for {', '.join(noisy)}.",
            "  That means the harness is too noisy to gate at this n. The answer is",
            "  MORE EXAMPLES, not a wider threshold -- do not raise the floor.",
        ]

    if improvements:
        lines += ["", "  improved (not adopted automatically -- run `make eval-baseline`):"]
        lines += [f"    {i}" for i in improvements]
    lines += ["-" * 68]
    lines.append("  RESULT: " + ("FAIL" if regressions else "PASS"))
    for regression in regressions:
        lines.append(f"    regression: {regression}")
    lines.append("")

    return Comparison(
        passed=not regressions,
        regressions=regressions,
        improvements=improvements,
        lines=lines,
    )
