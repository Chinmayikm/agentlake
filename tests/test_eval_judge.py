"""Tests for eval/judge.py.

**Zero API calls.** The judge takes a `chat` callable, so every test here
drives it with a plain async function returning a canned response. If this file
ever needs an API key, the injectable seam has been lost.

The parser tests matter more than they look: a judge whose reply cannot be
parsed must produce a NULL score, never a default. A fabricated 3 would be
indistinguishable from a real one in eval_results, and would move the gated
faithfulness mean without anything raising.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from eval.judge import (
    JUDGE_ALIAS,
    JUDGE_PROMPT_VERSION,
    MAX_CHUNK_CHARS,
    JudgeParseError,
    answer_quality_prompt,
    faithfulness_prompt,
    judge,
    load_rubric,
    order_for,
    parse_judge_reply,
    render_passages,
    score_once,
)

CHUNKS = [
    {
        "source_path": "docs/design.html",
        "section_path": "4.6 Message Delivery Semantics",
        "text": "Kafka guarantees at-least-once delivery by default.",
    }
]


@dataclass
class FakeUsage:
    cost_usd: float = 0.01


@dataclass
class FakeResponse:
    content: list[dict[str, Any]]
    model: str = "claude-sonnet-5"
    usage: FakeUsage = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.usage is None:
            self.usage = FakeUsage()


def reply(text: str) -> FakeResponse:
    return FakeResponse(content=[{"type": "text", "text": text}])


class FakeChat:
    """Pops one scripted reply per call; records every prompt sent."""

    def __init__(self, replies: list[FakeResponse]) -> None:
        self._replies = list(replies)
        self.prompts: list[str] = []
        self.systems: list[str] = []

    async def __call__(self, messages, *, system):
        self.prompts.append(messages[-1]["content"])
        self.systems.append(system)
        if not self._replies:
            raise AssertionError("FakeChat ran out of scripted replies")
        return self._replies.pop(0)


GOOD = '```json\n{"score": 4, "rationale": "grounded, one small stretch"}\n```'


# ---------------------------------------------------------------------------
# 1. Parsing: what is accepted
# ---------------------------------------------------------------------------


def test_parses_a_fenced_json_block() -> None:
    assert parse_judge_reply(GOOD) == (4, "grounded, one small stretch")


def test_parses_an_unfenced_object() -> None:
    """A model that forgets the fence has still answered."""
    assert parse_judge_reply('{"score": 5, "rationale": "ok"}') == (5, "ok")


def test_parses_json_wrapped_in_prose() -> None:
    text = f"Let me think about this.\n\n{GOOD}\n\nHope that helps."
    assert parse_judge_reply(text)[0] == 4


def test_takes_the_LAST_object_when_the_model_thinks_out_loud() -> None:
    """A model that shows an example object before complying would otherwise be
    graded on its example rather than on its answer."""
    text = (
        'For instance a reply might look like {"score": 1, "rationale": "example"}.\n'
        'My actual assessment:\n```json\n{"score": 5, "rationale": "real"}\n```'
    )
    assert parse_judge_reply(text) == (5, "real")


def test_accepts_an_integral_float() -> None:
    assert parse_judge_reply('{"score": 4.0, "rationale": "ok"}') == (4, "ok")


def test_truncates_an_overlong_rationale_rather_than_rejecting_it() -> None:
    """The rationale is evidence, not a measurement -- a long one is still
    usable, unlike a fractional score."""
    long = "x" * 900
    score, rationale = parse_judge_reply(f'{{"score": 3, "rationale": "{long}"}}')
    assert score == 3
    assert len(rationale) == 400


# ---------------------------------------------------------------------------
# 2. Parsing: what is rejected, and why each one matters
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("I would say about a 4 out of 5.", "no JSON object"),
        ('{"rationale": "forgot the score"}', "no `score` field"),
        ('{"score": "four", "rationale": "ok"}', "not a number"),
        ('{"score": 3.7, "rationale": "ok"}', "fractional"),
        ('{"score": 0, "rationale": "ok"}', "outside"),
        ('{"score": 6, "rationale": "ok"}', "outside"),
        ('{"score": 4, "rationale": ""}', "no usable `rationale`"),
        ('{"score": true, "rationale": "ok"}', "not a number"),
    ],
)
def test_rejects_a_malformed_reply(text: str, match: str) -> None:
    with pytest.raises(JudgeParseError, match=match):
        parse_judge_reply(text)


def test_a_fractional_score_is_rejected_not_rounded() -> None:
    """The single most important parser rule. A judge emitting 3.7 is reporting
    precision it does not have -- which is exactly why eval_results.faithfulness
    is a smallint. Rounding here would silently defeat that choice and let a
    fabricated precision into a gated metric."""
    with pytest.raises(JudgeParseError):
        parse_judge_reply('{"score": 3.5, "rationale": "ok"}')


# ---------------------------------------------------------------------------
# 3. Retry once, then NULL -- never a default score
# ---------------------------------------------------------------------------


def test_a_malformed_reply_is_retried_once_with_a_repair_instruction() -> None:
    """An identical retry re-rolls the same failure and buys nothing but a
    second bill, so the retry appends an instruction instead."""
    chat = FakeChat([reply("no json here"), reply(GOOD)])

    result = asyncio.run(score_once(chat, "the prompt"))

    assert result.parse_ok is True
    assert result.score == 4
    assert len(chat.prompts) == 2
    assert "could not be parsed" in chat.prompts[1]


def test_two_failures_yield_a_null_score_and_never_a_default() -> None:
    """A fabricated 3 is indistinguishable from a real one in eval_results and
    would move the gated faithfulness mean with nothing raising. NULL is a
    measurement that did not happen, which is the truth."""
    chat = FakeChat([reply("nope"), reply("still nope")])

    result = asyncio.run(score_once(chat, "the prompt"))

    assert result.score is None
    assert result.parse_ok is False
    assert "UNPARSEABLE" in result.rationale
    assert "still nope" in result.rationale


def test_a_failed_judge_call_still_reports_what_it_cost() -> None:
    """Two calls were paid for whether or not they parsed. Dropping the cost
    would make the budget ledger under-count exactly when things go wrong."""
    chat = FakeChat([reply("nope"), reply("still nope")])
    assert asyncio.run(score_once(chat, "p")).cost_usd == pytest.approx(0.02)


# ---------------------------------------------------------------------------
# 4. The two-call split, and what each prompt may contain
# ---------------------------------------------------------------------------


def test_the_faithfulness_prompt_never_shows_the_reference_answer() -> None:
    """If it did, the scale would measure agreement-with-reference rather than
    groundedness, and its name would be wrong."""
    prompt = faithfulness_prompt("q?", "the candidate answer", CHUNKS)

    assert "the candidate answer" in prompt
    assert "at-least-once delivery by default" in prompt      # the passage
    assert "REFERENCE-ONLY-SENTINEL" not in prompt
    assert "Reference answer" not in prompt


def test_the_quality_prompt_never_shows_the_retrieved_passages() -> None:
    """If it did, an answer that is chunk-supported but wrong would earn
    credit against a reference it does not match."""
    prompt = answer_quality_prompt("q?", "cand", "ref", reference_first=True)

    assert "cand" in prompt and "ref" in prompt
    assert "at-least-once delivery by default" not in prompt
    assert "<passage" not in prompt


def test_each_prompt_contains_its_rubric_verbatim() -> None:
    """The rubric lives in a file so it can be reviewed as prose. If the prompt
    stopped including it, a rubric edit would change nothing and the file would
    become documentation of something that is not happening."""
    assert load_rubric("faithfulness") in faithfulness_prompt("q", "a", CHUNKS)
    assert load_rubric("answer_quality") in answer_quality_prompt(
        "q", "a", "r", reference_first=False
    )


def test_the_judge_is_told_it_does_not_know_which_system_answered() -> None:
    chat = FakeChat([reply(GOOD), reply(GOOD)])
    asyncio.run(
        judge(chat, question="q", answer="a", reference="r", chunks=CHUNKS,
              example_key="kafka-x-001", seed=7)
    )
    assert all("do not know which system" in s for s in chat.systems)


def test_judge_makes_exactly_two_calls_and_sums_their_cost() -> None:
    chat = FakeChat([reply(GOOD), reply(GOOD)])

    result = asyncio.run(
        judge(chat, question="q", answer="a", reference="r", chunks=CHUNKS,
              example_key="kafka-x-001", seed=7)
    )

    assert len(chat.prompts) == 2
    assert result.faithfulness.score == 4
    assert result.answer_quality.score == 4
    assert result.cost_usd == pytest.approx(0.02)
    assert result.parse_failures == 0
    assert result.model == "claude-sonnet-5"


# ---------------------------------------------------------------------------
# 5. Position randomisation -- reproducible, and per example
# ---------------------------------------------------------------------------


def test_ordering_is_deterministic_for_a_given_seed_and_example() -> None:
    """A run has to be reproducible: re-judging the same example under the same
    seed must present the blocks in the same order, or two runs differ for a
    reason that has nothing to do with the system under test."""
    assert order_for(7, "kafka-x-001") == order_for(7, "kafka-x-001")


def test_ordering_is_independent_of_iteration_order() -> None:
    """Seeded per EXAMPLE rather than drawn from one stream, so a resumed run
    -- which visits a different subset in a different order -- assigns the same
    order to the same example as the full run did."""
    full = {k: order_for(7, k) for k in ("a-x-001", "b-x-002", "c-x-003")}
    resumed = {k: order_for(7, k) for k in ("c-x-003", "a-x-001")}
    assert all(resumed[k] == full[k] for k in resumed)


def test_ordering_actually_varies_across_examples() -> None:
    """A control that always picks the same order is not a control."""
    orders = {order_for(7, f"kafka-x-{i:03d}") for i in range(40)}
    assert orders == {True, False}


def test_the_order_used_is_recorded_on_the_score() -> None:
    """A bias control nobody can check after the fact is a claim, not a
    control -- so the order goes in the row, not just in the RNG."""
    chat = FakeChat([reply(GOOD), reply(GOOD)])
    result = asyncio.run(
        judge(chat, question="q", answer="a", reference="r", chunks=CHUNKS,
              example_key="kafka-x-001", seed=7)
    )
    assert result.answer_quality.order in {"reference_first", "candidate_first"}
    assert result.faithfulness.order == "n/a"


def test_reference_first_actually_reorders_the_prompt() -> None:
    first = answer_quality_prompt("q", "CAND", "REF", reference_first=True)
    second = answer_quality_prompt("q", "CAND", "REF", reference_first=False)
    assert first.index("REF") < first.index("CAND")
    assert second.index("CAND") < second.index("REF")


# ---------------------------------------------------------------------------
# 6. Passage rendering
# ---------------------------------------------------------------------------


def test_a_truncated_passage_says_so() -> None:
    """A judge that cannot tell a truncated passage from a complete one would
    score a correct answer unfaithful for citing something just past the cut."""
    long_chunk = [
        {"source_path": "a.md", "section_path": "S", "text": "y" * (MAX_CHUNK_CHARS + 50)}
    ]
    rendered = render_passages(long_chunk)
    assert "[passage truncated]" in rendered
    assert len(rendered) < MAX_CHUNK_CHARS + 400


def test_no_passages_is_stated_rather_than_left_blank() -> None:
    """An empty passage block would read to the judge as "nothing to check
    against", which scores differently from "the system retrieved nothing"."""
    assert "no passages were retrieved" in render_passages([])


def test_the_judge_alias_and_prompt_version_are_pinned() -> None:
    """judge-v1 keeps judge spend separable from agent spend on the ADR-007
    panels, and must not collide with a services/agent prompt version."""
    from services.agent.prompts import available_versions

    assert JUDGE_ALIAS == "quality"
    assert JUDGE_PROMPT_VERSION not in available_versions()
