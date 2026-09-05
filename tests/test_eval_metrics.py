"""Tests for eval/metrics.py -- pure functions, hand-worked expectations.

No Qdrant, no gateway, no API key: every number here is one a person can check
on paper, which is the only way a metric's own correctness is arguable rather
than assumed.
"""

from __future__ import annotations

import pytest

from eval.metrics import (
    Retrieved,
    any_call_hit,
    approx_tokens,
    citation_ok,
    first_call,
    hit_at_k,
    mean,
    pearson,
    rate,
)


def ret(path: str, call: int = 0, section: str = "") -> Retrieved:
    return Retrieved(source_path=path, section_path=section, call_index=call)


# ---------------------------------------------------------------------------
# 1. hit@k is scored on the FIRST search call
# ---------------------------------------------------------------------------


def test_hit_at_k_scores_only_the_first_search_call() -> None:
    """Scoring the union would measure an agent that brute-forces the metric by
    searching five times, not the retriever -- and would stop `make eval-ab`,
    which issues exactly one retrieval per question, from being comparable."""
    retrieved = [ret("docs/ops.html", call=0), ret("docs/design.html", call=1)]

    assert hit_at_k(["docs/design.html"], retrieved) is False
    assert any_call_hit(["docs/design.html"], retrieved) is True


def test_first_call_uses_the_lowest_call_index_present() -> None:
    """A resumed or filtered list may not start at 0; the first call is the
    lowest index actually present, not the literal zero."""
    retrieved = [ret("a.md", call=2), ret("b.md", call=3), ret("c.md", call=2)]
    assert [r.source_path for r in first_call(retrieved)] == ["a.md", "c.md"]


def test_hit_at_k_honours_k() -> None:
    """k=5 must not silently score a 10-result call."""
    retrieved = [ret(f"doc{i}.md") for i in range(9)]
    assert hit_at_k(["doc3.md"], retrieved, k=5) is True
    assert hit_at_k(["doc7.md"], retrieved, k=5) is False


def test_a_prefix_matches_a_whole_directory() -> None:
    """One mechanism covers both "this file" and "anything under here", which
    is why expected_sources are prefixes and not globs."""
    retrieved = [ret("docs/content/docs/ops/state/checkpoints.md")]
    assert hit_at_k(["docs/content/docs/ops/"], retrieved) is True
    assert hit_at_k(["docs/content/docs/deployment/"], retrieved) is False


def test_expectations_are_ORed_not_ANDed() -> None:
    """Requiring every listed source would measure how redundantly the corpus
    covers a topic rather than how well retrieval finds it."""
    retrieved = [ret("docs/design.html")]
    assert hit_at_k(["docs/design.html", "docs/ops.html"], retrieved) is True


# ---------------------------------------------------------------------------
# 2. "never searched" is None, and that distinction is the point
# ---------------------------------------------------------------------------


def test_no_search_at_all_is_none_not_false() -> None:
    """"The retriever missed" and "the agent never asked" are different
    failures. Collapsing them makes a prompt change that stops the agent
    searching look like a retrieval regression -- which is exactly the v4-to-v5
    case the gate has to diagnose, not merely detect."""
    assert hit_at_k(["docs/design.html"], []) is None


def test_a_search_that_returned_nothing_is_false_not_none() -> None:
    """An empty result set is the retriever answering, so it is a miss."""
    assert hit_at_k(["docs/design.html"], [], searched=True) is False


def test_rate_counts_none_as_a_miss_or_excludes_it_on_request() -> None:
    """The headline rate counts a None as a miss because that is what a user
    experiences; hit_at_5_given_search excludes them because that is what the
    retriever was actually asked. Two questions, and the caller says which."""
    values = [True, True, False, None]

    assert rate(values) == pytest.approx(0.5)                       # 2 of 4
    assert rate(values, none_counts_as=None) == pytest.approx(2 / 3)  # 2 of 3
    assert rate(values, none_counts_as=True) == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# 3. citation_ok, including the false negative it is known to have
# ---------------------------------------------------------------------------


def test_citation_ok_detects_a_filename() -> None:
    retrieved = [ret("docs/docs/evolution.md", section="Schema evolution > Correctness")]
    assert citation_ok("See evolution.md for the rules.", retrieved) is True


def test_citation_ok_detects_a_section_leaf() -> None:
    retrieved = [ret("docs/design.html", section="4.8 Log Compaction > Log Compaction Basics")]
    assert citation_ok("As described under Log Compaction Basics, ...", retrieved) is True


def test_citation_ok_is_false_for_a_correct_answer_that_cites_in_prose() -> None:
    """The documented false negative. It is reported so that a COLLAPSE in the
    rate is visible -- the signal that a prompt stopped asking for citations at
    all -- not so that any single value is believed."""
    retrieved = [ret("docs/design.html", section="4.6 Message Delivery Semantics")]
    assert citation_ok("Kafka guarantees at-least-once delivery by default.", retrieved) is False


def test_citation_ok_ignores_generic_path_words() -> None:
    """Without a stopword list, every answer mentioning "configuration" would
    score as a citation of configuration.md and the metric would read ~1.0
    forever."""
    retrieved = [ret("docs/docs/configuration.md", section="Write properties")]
    assert citation_ok("This is a matter of configuration, broadly.", retrieved) is False


def test_citation_ok_is_false_when_nothing_was_retrieved() -> None:
    assert citation_ok("some answer", []) is False


# ---------------------------------------------------------------------------
# 4. Aggregation, against hand-computed values
# ---------------------------------------------------------------------------


def test_pearson_matches_a_hand_computed_value() -> None:
    """r for a perfect positive line is exactly 1; for a perfect negative one,
    exactly -1. The mixed case below is worked by hand:
      x = [1,2,3,4], y = [2,4,5,9] -> dx = [-1.5,-0.5,0.5,1.5],
      dy = [-3,-1,0,4]; sum(dx*dy) = 4.5+0.5+0+6 = 11;
      |dx| = sqrt(5), |dy| = sqrt(26); r = 11/sqrt(130) = 0.96476...
    """
    assert pearson([1, 2, 3], [2, 4, 6]) == pytest.approx(1.0)
    assert pearson([1, 2, 3], [6, 4, 2]) == pytest.approx(-1.0)
    assert pearson([1, 2, 3, 4], [2, 4, 5, 9]) == pytest.approx(11 / (130**0.5))


def test_pearson_is_none_when_it_is_undefined() -> None:
    """None, not 0.0. A judge that gave every answer a 5 has no measurable
    relationship with length, and reporting "r = 0.0, no length bias" would be
    a claim the data cannot support."""
    assert pearson([1, 2, 3], [4, 4, 4]) is None
    assert pearson([1], [2]) is None
    assert pearson([1, 2], [1, 2, 3]) is None


def test_mean_skips_missing_scores_rather_than_treating_them_as_zero() -> None:
    """A NULL score is a measurement that did not happen -- a judge parse
    failure. Averaging it in as 0 would turn a harness fault into a quality
    regression."""
    assert mean([4, 5, None]) == pytest.approx(4.5)
    assert mean([None, None]) is None


def test_approx_tokens_is_documented_as_an_estimate() -> None:
    """Used only where no provider token count exists. The harness uses the
    gateway's completion_tokens instead, because this measures a tokenizer
    nobody has."""
    assert approx_tokens("a" * 40) == 10
    assert approx_tokens("") == 1
