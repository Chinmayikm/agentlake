"""Human labelling, and judge-vs-human agreement.

A judge score is only worth publishing if somebody has checked that it tracks a
human's. This is that check: a stratified sample of 30 from a completed run,
emitted as a CSV for hand-labelling, and an agreement report computed against
the returned sheet.

**The judge's score is deliberately absent from the sheet.** Showing it would
anchor the labeller and make the agreement number partly self-fulfilling --
the one failure mode that would make the whole exercise decorative.

Kappa is implemented here in stdlib rather than by adding scikit-learn, the
same instinct that keeps httpx doing the Iceberg REST calls (ADR-004 §2).
"""

from __future__ import annotations

import csv
import hashlib
import json
import random
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

LABEL_DIR = Path("docs/eval/labels")

DEFAULT_SAMPLE = 30

COLUMNS = [
    "eval_result_id",
    "example_key",
    "question",
    "retrieved_sources",
    "answer",
    "human_faithfulness",
    "human_notes",
]


class LabelSheetError(RuntimeError):
    """A sheet that does not belong to the run it is being scored against."""


@dataclass(frozen=True, slots=True)
class LabelRow:
    eval_result_id: int
    example_key: str
    question: str
    answer: str
    retrieved_sources: str = ""


def row_digest(rows: list[LabelRow]) -> str:
    """sha256 over (id, question, answer) for every row.

    The stale-sheet guard, and it is exact rather than advisory: if the run was
    re-run, or rows were edited, or the sheet came from a different run, the
    digest will not match and `agreement()` refuses. Scoring a judge against
    answers it never produced would be worse than not measuring at all.
    """
    digest = hashlib.sha256()
    for row in sorted(rows, key=lambda r: r.eval_result_id):
        digest.update(f"{row.eval_result_id}\x00{row.question}\x00{row.answer}\x00".encode())
    return digest.hexdigest()


def stratified_sample(
    rows: list[LabelRow], keys_by_stratum: dict[int, tuple[str, ...]], n: int, run_id: int
) -> list[LabelRow]:
    """`n` rows spread proportionally across strata, deterministically.

    Seeded off the run id, so re-running `make eval-label` for a run produces
    the IDENTICAL sheet -- a labeller who has half-filled one must not have it
    reshuffled underneath them.
    """
    rng = random.Random(f"label:{run_id}")
    buckets: dict[tuple[str, ...], list[LabelRow]] = {}
    for row in rows:
        buckets.setdefault(keys_by_stratum.get(row.eval_result_id, ()), []).append(row)

    for bucket in buckets.values():
        rng.shuffle(bucket)

    # Round-robin across strata rather than proportional-with-rounding: at
    # n=30 over ~9 strata, rounding leaves the remainder concentrated in
    # whichever bucket happens to sort first.
    chosen: list[LabelRow] = []
    order = sorted(buckets)
    while len(chosen) < n and any(buckets[s] for s in order):
        for stratum in order:
            if buckets[stratum] and len(chosen) < n:
                chosen.append(buckets[stratum].pop())
    return sorted(chosen, key=lambda r: r.example_key)


def write_sheet(
    rows: list[LabelRow],
    run_id: int,
    config: dict[str, Any],
    directory: Path = LABEL_DIR,
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    csv_path = directory / f"run-{run_id}.csv"
    manifest_path = directory / f"run-{run_id}.manifest.json"

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "eval_result_id": row.eval_result_id,
                    "example_key": row.example_key,
                    "question": row.question,
                    "retrieved_sources": row.retrieved_sources,
                    "answer": row.answer,
                    "human_faithfulness": "",
                    "human_notes": "",
                }
            )

    manifest_path.write_text(
        json.dumps(
            {
                "eval_run_id": run_id,
                "n": len(rows),
                "row_digest": row_digest(rows),
                "columns": COLUMNS,
                **config,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return csv_path, manifest_path


def read_sheet(path: Path) -> dict[int, int]:
    """eval_result_id -> human_faithfulness, skipping unlabelled rows.

    A blank row is "not labelled", not zero. Reading it as a score would drag
    the human mean down and manufacture disagreement.
    """
    labels: dict[int, int] = {}
    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            raw = (row.get("human_faithfulness") or "").strip()
            if not raw:
                continue
            try:
                score = int(float(raw))
            except ValueError:
                raise LabelSheetError(
                    f"row {row.get('eval_result_id')}: human_faithfulness {raw!r} "
                    f"is not a whole number 1-5"
                ) from None
            if not 1 <= score <= 5:
                raise LabelSheetError(
                    f"row {row.get('eval_result_id')}: human_faithfulness {score} "
                    f"is outside 1-5"
                )
            labels[int(row["eval_result_id"])] = score
    return labels


# ---------------------------------------------------------------------------
# Agreement
# ---------------------------------------------------------------------------


def cohens_kappa(pairs: list[tuple[int, int]], *, weighted: bool = False) -> float | None:
    """Cohen's kappa, quadratically weighted on request.

    Returns None when it is undefined -- if one rater used a single category,
    expected agreement is 1 and the denominator vanishes. Reporting 0.0 there
    would say "no better than chance" about data that cannot support the claim.
    """
    if len(pairs) < 2:
        return None
    categories = sorted({c for pair in pairs for c in pair})
    if len(categories) < 2:
        return None
    index = {c: i for i, c in enumerate(categories)}
    k = len(categories)

    observed = [[0.0] * k for _ in range(k)]
    for a, b in pairs:
        observed[index[a]][index[b]] += 1
    n = len(pairs)

    rows = [sum(r) / n for r in observed]
    cols = [sum(observed[i][j] for i in range(k)) / n for j in range(k)]

    def weight(i: int, j: int) -> float:
        if not weighted:
            return 0.0 if i == j else 1.0
        return ((categories[i] - categories[j]) / (categories[-1] - categories[0])) ** 2

    po = sum(weight(i, j) * observed[i][j] / n for i in range(k) for j in range(k))
    pe = sum(weight(i, j) * rows[i] * cols[j] for i in range(k) for j in range(k))
    if pe == 0:
        return None
    return 1 - po / pe


@dataclass(slots=True)
class Agreement:
    n: int
    exact: float
    within_one: float
    mean_signed_difference: float
    kappa: float | None
    weighted_kappa: float | None
    confusion: dict[tuple[int, int], int]
    skipped_null_judge: int
    unlabelled: int


def agreement(pairs: list[tuple[int, int]], *, skipped: int = 0, unlabelled: int = 0) -> Agreement:
    """judge-vs-human, as (judge, human) pairs."""
    n = len(pairs)
    exact = sum(1 for j, h in pairs if j == h) / n if n else 0.0
    within = sum(1 for j, h in pairs if abs(j - h) <= 1) / n if n else 0.0
    signed = sum(j - h for j, h in pairs) / n if n else 0.0
    return Agreement(
        n=n,
        exact=exact,
        within_one=within,
        mean_signed_difference=signed,
        kappa=cohens_kappa(pairs),
        weighted_kappa=cohens_kappa(pairs, weighted=True),
        confusion=dict(Counter(pairs)),
        skipped_null_judge=skipped,
        unlabelled=unlabelled,
    )


def render_agreement(result: Agreement, config: dict[str, Any]) -> str:
    """docs/eval/agreement.md.

    The honest-limits paragraph is not decoration: at n=30 a proportion carries
    a +/-18 point 95% CI at p=0.5 and +/-11 at p=0.9, so "80% within-1" really
    means "62-91% within-1". Publishing the number without that would invite it
    to be read as precision it does not have.
    """
    direction = (
        "generous" if result.mean_signed_difference > 0.1
        else "harsh" if result.mean_signed_difference < -0.1
        else "neither systematically generous nor harsh"
    )
    lines = [
        "# Judge validity — agreement with a human labeller",
        "",
        f"Faithfulness scored by the judge, against **{result.n} hand labels** on run "
        f"`{config.get('eval_run_id')}`.",
        "",
        "| | |",
        "|---|---|",
        f"| exact agreement | **{result.exact:.1%}** |",
        f"| within-1 agreement | **{result.within_one:.1%}** |",
        f"| mean signed difference (judge minus human) | {result.mean_signed_difference:+.2f} |",
        f"| Cohen's κ | {_fmt(result.kappa)} |",
        f"| quadratic-weighted κ | {_fmt(result.weighted_kappa)} |",
        f"| judge scores that were NULL (unparseable, excluded) | {result.skipped_null_judge} |",
        f"| rows left unlabelled (excluded) | {result.unlabelled} |",
        "",
        f"The judge is {direction} relative to the human on this sample.",
        "",
        "## Confusion matrix (judge → human)",
        "",
        "| judge \\ human | 1 | 2 | 3 | 4 | 5 |",
        "|---|---|---|---|---|---|",
    ]
    for j in range(1, 6):
        cells = " | ".join(str(result.confusion.get((j, h), 0)) for h in range(1, 6))
        lines.append(f"| **{j}** | {cells} |")

    lines += [
        "",
        "## What n=30 honestly buys",
        "",
        "A 95% confidence interval on a proportion at n=30 is about ±18 points at",
        "p=0.5 and ±11 at p=0.9. So a within-1 agreement of 80% really means",
        "\"somewhere between 62% and 91%\", and two judges differing by ten points on",
        "this sample are not distinguishable.",
        "",
        "Quadratic-weighted κ is unstable at n=30 against a skewed marginal — and this",
        "marginal is skewed, since most answers score 4 or 5. It can sit near zero",
        "while raw agreement is 90%, which is the base-rate problem rather than a",
        "broken judge. Both numbers are reported and neither should be read alone.",
        "",
        "**n=30 detects a judge that is grossly broken** — within-1 agreement below",
        "about 50%. **It cannot distinguish a good judge from a slightly better one.**",
        "That is the claim this file supports, and no more.",
        "",
        "## Method",
        "",
        "- The sample is stratified across (project x type) and seeded off the run id,",
        "  so re-running `make eval-label` produces the identical sheet.",
        "- **The judge's score is not shown on the labelling sheet.** Showing it would",
        "  anchor the labeller and make this number partly self-fulfilling.",
        "- The sheet carries a manifest with a sha256 over its (id, question, answer)",
        "  rows; `make eval-agreement` recomputes it from Postgres and refuses on a",
        "  mismatch, so a sheet from a different or re-run job cannot be scored by",
        "  accident.",
        "- Rows where the judge produced no score (an unparseable reply) are excluded",
        "  rather than counted as disagreement: a measurement that did not happen is",
        "  not a disagreement.",
        "",
        "```",
        json.dumps(config, indent=2),
        "```",
    ]
    return "\n".join(lines) + "\n"


def _fmt(value: float | None) -> str:
    return "n/a (undefined — a rater used a single category)" if value is None else f"{value:.3f}"
