"""Tests for eval/baseline.py and eval/label.py.

No database, no API key: the threshold arithmetic is worked by hand and the
label sheet round-trips through tmp_path.

The gate is the piece most likely to be believed without being checked -- a
threshold is a number that decides whether a build fails, and if the arithmetic
behind it is wrong, nobody finds out from the output.
"""

from __future__ import annotations

import csv
import json
import math

import pytest

from eval.baseline import (
    FLOORS,
    GATED,
    Baseline,
    BaselineError,
    compare,
    sigma_from_two,
    threshold,
)
from eval.label import (
    LabelRow,
    LabelSheetError,
    agreement,
    cohens_kappa,
    read_sheet,
    render_agreement,
    row_digest,
    stratified_sample,
    write_sheet,
)

CONFIG = {
    "dataset_version": "v1",
    "corpus_version": "2026-08-27-pinned",
    "prompt_version": "v4",
    "judge_model": "claude-sonnet-5",
    "n": 25,
}


def make_baseline(a: dict, b: dict, config: dict | None = None) -> Baseline:
    return Baseline.from_runs(a, b, {**CONFIG, **(config or {})})


# ---------------------------------------------------------------------------
# 1. The threshold arithmetic, by hand
# ---------------------------------------------------------------------------


def test_sigma_from_two_observations_matches_the_closed_form() -> None:
    """s = |x1 - x2| / sqrt(2), from s^2 = sum(xi - xbar)^2/(n-1) = d^2/2.
    Worked here rather than trusted, because every threshold is derived from
    it."""
    assert sigma_from_two(0.80, 0.90) == pytest.approx(0.10 / math.sqrt(2))
    assert sigma_from_two(4.2, 4.2) == 0.0
    assert sigma_from_two(0.9, 0.8) == sigma_from_two(0.8, 0.9)  # symmetric


def test_threshold_is_the_larger_of_two_sigma_and_the_floor() -> None:
    assert threshold(0.01, 0.20) == 0.20            # floor dominates
    assert threshold(0.50, 0.20) == pytest.approx(1.0)  # 2*sigma dominates


def test_two_identical_runs_fall_back_to_the_floor() -> None:
    """sigma = 0 from two identical runs would make the threshold 0 and fail
    the build on any movement at all. The floor is what stops that -- and is
    why the floors, not the 2s term, carry the gate."""
    baseline = make_baseline(
        {"faithfulness_mean": 4.2, "hit_at_5_rate": 0.88},
        {"faithfulness_mean": 4.2, "hit_at_5_rate": 0.88},
    )
    assert baseline.sigma["faithfulness_mean"] == 0.0
    assert baseline.thresholds["faithfulness_mean"] == FLOORS["faithfulness_mean"]
    assert baseline.thresholds["hit_at_5_rate"] == FLOORS["hit_at_5_rate"]


def test_the_floors_are_multiples_of_one_example_at_n_25() -> None:
    """One example flipping moves a 25-example mean by exactly 1/25 = 0.04.
    A floor that was not a multiple of that would fire, or fail to fire, on
    fractions of an example -- which do not exist."""
    for floor in FLOORS.values():
        assert floor % 0.04 == pytest.approx(0.0, abs=1e-9)
    assert FLOORS["hit_at_5_rate"] == pytest.approx(2 / 25)


def test_baseline_averages_the_two_runs() -> None:
    baseline = make_baseline(
        {"faithfulness_mean": 4.0, "hit_at_5_rate": 0.80},
        {"faithfulness_mean": 4.4, "hit_at_5_rate": 0.88},
    )
    assert baseline.metrics["faithfulness_mean"] == pytest.approx(4.2)
    assert baseline.metrics["hit_at_5_rate"] == pytest.approx(0.84)
    assert baseline.sigma["faithfulness_mean"] == pytest.approx(0.4 / math.sqrt(2))


# ---------------------------------------------------------------------------
# 2. The gate: one-sided, and separating drift from regression
# ---------------------------------------------------------------------------


IDENTICAL = {"faithfulness_mean": 4.2, "hit_at_5_rate": 0.88}


def test_an_unchanged_run_passes() -> None:
    result = compare(make_baseline(IDENTICAL, IDENTICAL), dict(IDENTICAL), CONFIG)
    assert result.passed is True
    assert result.regressions == []


def test_a_regression_past_the_threshold_fails() -> None:
    current = {"faithfulness_mean": 3.9, "hit_at_5_rate": 0.88}  # -0.30 vs floor 0.20
    result = compare(make_baseline(IDENTICAL, IDENTICAL), current, CONFIG)

    assert result.passed is False
    assert any("faithfulness_mean" in r for r in result.regressions)


def test_a_drop_within_the_threshold_passes() -> None:
    """The floor exists so a handful of borderline 3-vs-4 judge calls do not
    fail a build."""
    current = {"faithfulness_mean": 4.05, "hit_at_5_rate": 0.88}  # -0.15 < 0.20
    assert compare(make_baseline(IDENTICAL, IDENTICAL), current, CONFIG).passed is True


def test_the_gate_is_one_sided_so_an_improvement_never_fails() -> None:
    """An improvement is reported and prompts a deliberate re-baseline. A gate
    that auto-adopted every improvement could not tell a real gain from a lucky
    run."""
    current = {"faithfulness_mean": 4.9, "hit_at_5_rate": 0.99}
    result = compare(make_baseline(IDENTICAL, IDENTICAL), current, CONFIG)

    assert result.passed is True
    assert result.improvements


def test_config_drift_fails_on_its_own_path_not_as_a_regression() -> None:
    """"faithfulness dropped 0.3" and "you compared v4 against v5" need
    different responses. Reporting the second as the first sends someone
    hunting a regression that is not there."""
    result = compare(
        make_baseline(IDENTICAL, IDENTICAL), dict(IDENTICAL), {**CONFIG, "prompt_version": "v5"}
    )

    assert result.passed is False
    assert result.config_drift
    assert result.regressions == []
    assert "not comparable" in result.render()


def test_config_drift_can_be_allowed_deliberately() -> None:
    result = compare(
        make_baseline(IDENTICAL, IDENTICAL),
        dict(IDENTICAL),
        {**CONFIG, "prompt_version": "v5"},
        allow_config_drift=True,
    )
    assert result.passed is True


def test_a_metric_missing_from_the_run_fails_rather_than_passing_by_omission() -> None:
    """A gate that ignored a metric it could not find would pass a run that
    stopped measuring the thing being gated."""
    result = compare(make_baseline(IDENTICAL, IDENTICAL), {"hit_at_5_rate": 0.88}, CONFIG)

    assert result.passed is False
    assert any("missing" in r for r in result.regressions)


def test_a_noisy_baseline_says_to_add_examples_not_to_widen_the_threshold() -> None:
    """If 2*sigma exceeds twice the floor, the harness is too noisy to gate at
    this n. Widening the threshold would hide the noise instead of fixing it."""
    noisy = compare(
        make_baseline(
            {"faithfulness_mean": 3.0, "hit_at_5_rate": 0.50},
            {"faithfulness_mean": 4.5, "hit_at_5_rate": 0.95},
        ),
        {"faithfulness_mean": 3.75, "hit_at_5_rate": 0.725},
        CONFIG,
    )
    assert "MORE EXAMPLES" in noisy.render()


def test_a_baseline_round_trips_through_disk(tmp_path) -> None:
    path = tmp_path / "baseline.json"
    make_baseline(IDENTICAL, {"faithfulness_mean": 4.0, "hit_at_5_rate": 0.84}).save(path)
    reloaded = Baseline.load(path)

    assert reloaded.metrics["faithfulness_mean"] == pytest.approx(4.1)
    assert set(reloaded.thresholds) == set(GATED)
    assert json.loads(path.read_text())["floors"] == FLOORS


def test_a_missing_baseline_is_an_error_not_a_pass(tmp_path) -> None:
    """Defaulting to "pass" when there is nothing to compare against would make
    the gate decorative -- green on every PR, including the one that broke it."""
    with pytest.raises(BaselineError, match="no baseline"):
        Baseline.load(tmp_path / "absent.json")


def test_a_baseline_from_an_old_schema_is_rejected(tmp_path) -> None:
    path = tmp_path / "baseline.json"
    make_baseline(IDENTICAL, IDENTICAL).save(path)
    raw = json.loads(path.read_text())
    raw["schema"] = 0
    path.write_text(json.dumps(raw))

    with pytest.raises(BaselineError, match="schema"):
        Baseline.load(path)


# ---------------------------------------------------------------------------
# 3. The labelling sheet
# ---------------------------------------------------------------------------


ROWS = [
    LabelRow(eval_result_id=i, example_key=f"kafka-x-{i:03d}", question=f"q{i}", answer=f"a{i}")
    for i in range(1, 13)
]


def test_the_sheet_has_a_blank_human_column_and_no_judge_score(tmp_path) -> None:
    """The single most important property of this file. Showing the judge's
    score would anchor the labeller and make the agreement number partly
    self-fulfilling -- which would make the whole exercise decorative."""
    csv_path, _ = write_sheet(ROWS[:3], 7, {}, directory=tmp_path)
    text = csv_path.read_text(encoding="utf-8")
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))

    assert all(row["human_faithfulness"] == "" for row in rows)
    assert "faithfulness_judge" not in text
    assert "judge" not in text.lower().replace("human_faithfulness", "")


def test_the_sample_is_identical_when_regenerated(tmp_path) -> None:
    """A labeller who has half-filled a sheet must not have it reshuffled
    underneath them by a re-run."""
    strata = {r.eval_result_id: ("kafka", "conceptual") for r in ROWS}
    first = stratified_sample(ROWS, strata, 6, run_id=7)
    second = stratified_sample(ROWS, strata, 6, run_id=7)

    assert [r.eval_result_id for r in first] == [r.eval_result_id for r in second]


def test_the_sample_spreads_across_strata() -> None:
    """A sheet drawn entirely from one project would validate the judge on that
    project only."""
    strata = {
        r.eval_result_id: ("kafka" if r.eval_result_id <= 6 else "flink", "conceptual")
        for r in ROWS
    }
    sample = stratified_sample(ROWS, strata, 6, run_id=7)
    kafka = sum(1 for r in sample if r.eval_result_id <= 6)

    assert kafka == 3


def test_a_blank_label_is_skipped_rather_than_read_as_zero(tmp_path) -> None:
    """A blank means "not labelled". Reading it as a score would drag the human
    mean down and manufacture disagreement."""
    csv_path, _ = write_sheet(ROWS[:3], 7, {}, directory=tmp_path)
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8")))
    rows[0]["human_faithfulness"] = "4"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    assert read_sheet(csv_path) == {1: 4}


@pytest.mark.parametrize("bad", ["9", "0", "high"])
def test_an_out_of_range_or_non_numeric_label_is_rejected(tmp_path, bad: str) -> None:
    csv_path, _ = write_sheet(ROWS[:1], 7, {}, directory=tmp_path)
    text = csv_path.read_text(encoding="utf-8").replace("a1,,", f"a1,{bad},")
    csv_path.write_text(text, encoding="utf-8")

    with pytest.raises(LabelSheetError):
        read_sheet(csv_path)


def test_the_digest_changes_when_an_answer_changes() -> None:
    """The stale-sheet guard. Scoring a judge against answers it never produced
    would be worse than not measuring at all."""
    altered = [*ROWS[:2], LabelRow(3, "kafka-x-003", "q3", "DIFFERENT ANSWER")]

    assert row_digest(ROWS[:3]) != row_digest(altered)
    assert row_digest(ROWS[:3]) == row_digest(list(reversed(ROWS[:3])))  # order-independent


def test_the_manifest_pins_the_run_and_the_digest(tmp_path) -> None:
    _, manifest_path = write_sheet(ROWS[:3], 7, {"source_rows": 3}, directory=tmp_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["eval_run_id"] == 7
    assert manifest["row_digest"] == row_digest(ROWS[:3])


# ---------------------------------------------------------------------------
# 4. Agreement, worked by hand
# ---------------------------------------------------------------------------


def test_agreement_counts_exact_and_within_one() -> None:
    """Worked by hand. pairs are (judge, human), differences 0, -1, -2, -3:

        (4,4) exact, within-1
        (4,5) off by 1, within-1
        (3,5) off by 2
        (1,4) off by 3

    -> exact 1/4 = 0.25, within-1 2/4 = 0.50,
       mean signed = (0 + -1 + -2 + -3) / 4 = -1.5, i.e. a HARSH judge.
    """
    pairs = [(4, 4), (4, 5), (3, 5), (1, 4)]
    result = agreement(pairs)

    assert result.n == 4
    assert result.exact == pytest.approx(0.25)
    assert result.within_one == pytest.approx(0.50)
    assert result.mean_signed_difference == pytest.approx(-1.5)


def test_perfect_agreement_has_kappa_one() -> None:
    assert cohens_kappa([(4, 4), (5, 5), (3, 3), (4, 4)]) == pytest.approx(1.0)


def test_kappa_is_none_when_a_rater_used_one_category() -> None:
    """Expected agreement is 1, so the denominator vanishes. Reporting 0.0
    would say "no better than chance" about data that cannot support it."""
    assert cohens_kappa([(4, 4), (4, 4), (4, 4)]) is None
    assert cohens_kappa([(4, 4)]) is None


def test_weighted_kappa_is_kinder_to_near_misses_than_plain_kappa() -> None:
    """The reason both are reported: on a 1-5 ordinal scale, judging a 4 as a 5
    is not the same error as judging it a 1, and plain kappa cannot tell them
    apart."""
    near = [(4, 5), (5, 4), (3, 4), (4, 3), (5, 5), (3, 3)]
    assert cohens_kappa(near, weighted=True) > cohens_kappa(near)


def test_the_agreement_report_states_what_n_30_cannot_do() -> None:
    """Publishing a number without its confidence interval invites it to be
    read as precision it does not have."""
    report = render_agreement(agreement([(4, 4), (4, 5), (5, 5)]), {"eval_run_id": 7})

    assert "cannot distinguish a good judge from a slightly better one" in report
    assert "62%" in report  # the worked CI at p=0.8
    assert "anchor the labeller" in report


def test_null_judge_scores_are_excluded_rather_than_counted_as_disagreement() -> None:
    result = agreement([(4, 4)], skipped=3)
    assert result.skipped_null_judge == 3
    assert result.n == 1
