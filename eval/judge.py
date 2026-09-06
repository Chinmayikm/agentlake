"""LLM-as-judge: two rubric-anchored 1-5 scores per answer.

**Two calls, and the split is structural rather than cautious.** Faithfulness
is graded against the RETRIEVED CHUNKS and must not see the reference answer,
or it grades agreement-with-reference instead of groundedness. Answer quality
is graded against the REFERENCE and must not see the chunks, or an answer that
is chunk-supported but wrong earns credit. One call means one context holding
both, and a model cannot unsee either. The cost of the split is roughly one
extra sonnet call per example; the cost of not splitting is that neither number
means what its name says.

Every call goes through services/gateway (ADR-001's single door), so judging is
itself traced and costed and shows up on the ADR-007 panels under
`X-Prompt-Version: judge-v1` -- separable from agent spend.

**Never a default score.** A reply that cannot be parsed after one retry yields
`score=None` and `parse_ok=False`, and the row is stored with NULL scores. A
fabricated 3 is exactly the lie ADR-003 #3 forbids, and the smallint column
exists so a judge cannot report precision it does not have.
"""

from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any, Protocol

RUBRIC_DIR = Path(__file__).parent / "rubric"

#: The judge model, by gateway alias. The EXACT provider model id is recorded
#: on eval_runs.judge_model from the gateway's response, not from this constant
#: -- a silently re-pointed alias is precisely what makes two "identical" runs
#: incomparable, and only the response can catch it.
JUDGE_ALIAS = "quality"

#: Sent as X-Prompt-Version so judge spend is separable from agent spend on the
#: dashboards. It is not a services/agent prompt version and deliberately does
#: not collide with one.
JUDGE_PROMPT_VERSION = "judge-v1"

#: How much of each retrieved chunk the faithfulness judge sees. Enough to
#: verify a claim, bounded so a 5-chunk context stays affordable. Truncation is
#: marked in the prompt rather than silent -- a judge that cannot tell a
#: truncated passage from a complete one would score a correct answer
#: unfaithful for citing something just past the cut.
MAX_CHUNK_CHARS = 1200
MAX_CHUNKS = 5

_SCORE_MIN, _SCORE_MAX = 1, 5
_MAX_RATIONALE = 400

_JSON_BLOCK = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.S)


class JudgeParseError(ValueError):
    """The judge's reply did not contain a usable score."""


@dataclass(frozen=True, slots=True)
class JudgeScore:
    score: int | None
    rationale: str
    parse_ok: bool
    #: "reference_first" | "candidate_first" | "n/a" -- recorded per row so the
    #: bias control is auditable without reconstructing the RNG. A control
    #: nobody can check after the fact is a claim, not a control.
    order: str = "n/a"
    #: Provider model id from the gateway's response, for eval_runs.judge_model.
    model: str = ""
    cost_usd: float = 0.0


class ChatFn(Protocol):
    """The one thing the judge needs from the world.

    A callable, not a GatewayClient, so tests drive the whole judge with a
    plain function and the suite makes zero API calls.
    """

    async def __call__(self, messages: list[dict[str, Any]], *, system: str) -> Any: ...


@cache
def load_rubric(name: str) -> str:
    """The rubric text, read from eval/rubric/<name>.md at call time.

    A file rather than a string literal, so the rubric can be reviewed as prose
    and diffed as a change. `tests/test_eval_judge.py` asserts the file's text
    appears in the prompt, so a rubric edit cannot fail to reach the judge.
    """
    path = RUBRIC_DIR / f"{name}.md"
    if not path.is_file():
        raise FileNotFoundError(f"no rubric at {path}")
    return path.read_text(encoding="utf-8").strip()


def _clip(text: str, limit: int) -> tuple[str, bool]:
    text = text.strip()
    if len(text) <= limit:
        return text, False
    return text[:limit].rstrip(), True


def render_passages(chunks: list[dict[str, str]]) -> str:
    parts = []
    for i, chunk in enumerate(chunks[:MAX_CHUNKS], start=1):
        body, truncated = _clip(str(chunk.get("text", "")), MAX_CHUNK_CHARS)
        marker = "\n[passage truncated]" if truncated else ""
        source = chunk.get("source_path", "?")
        section = chunk.get("section_path", "")
        header = f"{source}" + (f" — {section}" if section else "")
        parts.append(f"<passage {i}: {header}>\n{body}{marker}\n</passage {i}>")
    return "\n\n".join(parts) if parts else "(no passages were retrieved)"


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_OUTPUT_CONTRACT = (
    "Reply with EXACTLY one fenced JSON block and nothing else:\n"
    "```json\n"
    '{"score": <integer 1-5>, "rationale": "<at most 400 characters>"}\n'
    "```\n"
    "`score` must be a whole number from 1 to 5. Do not report a fraction — a "
    "score of 3.5 is not a value this scale has."
)

_JUDGE_SYSTEM = (
    "You are grading the output of a documentation question-answering system "
    "against a rubric. You do not know which system produced the answer, and "
    "that is deliberate: grade only what is in front of you, against the "
    "rubric, and never against your expectations of who wrote it."
)


def faithfulness_prompt(question: str, answer: str, chunks: list[dict[str, str]]) -> str:
    """Chunks and the answer. NOT the reference -- see the module docstring."""
    return (
        f"{load_rubric('faithfulness')}\n\n"
        f"---\n\n"
        f"## Question\n\n{question}\n\n"
        f"## Passages the system retrieved\n\n{render_passages(chunks)}\n\n"
        f"## Answer to grade\n\n{answer}\n\n"
        f"---\n\n{_OUTPUT_CONTRACT}"
    )


def answer_quality_prompt(
    question: str, answer: str, reference: str, *, reference_first: bool
) -> str:
    """Reference and candidate. NOT the chunks -- see the module docstring.

    `reference_first` randomises which block appears first, controlling for
    primacy/recency. Note what this is NOT: full A/B blinding. With one system
    and one human reference, the judge must be TOLD which is the reference or
    the rubric ("grade the candidate against the reference") has no meaning.
    Position is the part that can honestly be controlled; true blind pairwise
    comparison needs two candidate systems and is deferred (ADR-008 §5).
    """
    reference_block = f"## Reference answer (written by a domain expert)\n\n{reference}"
    candidate_block = f"## Candidate answer (to grade)\n\n{answer}"
    first, second = (
        (reference_block, candidate_block) if reference_first
        else (candidate_block, reference_block)
    )
    return (
        f"{load_rubric('answer_quality')}\n\n"
        f"---\n\n"
        f"## Question\n\n{question}\n\n"
        f"{first}\n\n{second}\n\n"
        f"---\n\n{_OUTPUT_CONTRACT}"
    )


def order_for(seed: int, example_key: str) -> bool:
    """Whether the reference block comes first, deterministically.

    Seeded per EXAMPLE rather than drawn from one stream, so a resumed or
    partially re-run job assigns the same order to the same example -- an
    ordering that depended on iteration order would silently differ between a
    full run and a resume, and the two would not be comparable.
    """
    return random.Random(f"{seed}:{example_key}").random() < 0.5


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_judge_reply(text: str) -> tuple[int, str]:
    """The last JSON object in the reply, validated hard.

    Last rather than first: a model that thinks out loud before complying
    leaves an example object earlier in the text, and taking the first would
    grade the example.

    A non-integral score is REJECTED, not rounded. A judge emitting 3.7 is
    reporting precision it does not have, which is the failure the smallint
    column was chosen to prevent -- rounding it here would defeat that choice
    silently.
    """
    blocks = _JSON_BLOCK.findall(text or "")
    candidates = list(blocks) or _balanced_objects(text or "")
    if not candidates:
        raise JudgeParseError("no JSON object in the reply")

    payload: Any = None
    for raw in reversed(candidates):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        break
    if not isinstance(payload, dict):
        raise JudgeParseError("no parseable JSON object in the reply")

    if "score" not in payload:
        raise JudgeParseError("reply has no `score` field")
    score = payload["score"]
    if isinstance(score, bool) or not isinstance(score, int | float):
        raise JudgeParseError(f"score {score!r} is not a number")
    if isinstance(score, float) and not score.is_integer():
        raise JudgeParseError(
            f"score {score!r} is fractional; this scale has no value between its anchors"
        )
    score = int(score)
    if not _SCORE_MIN <= score <= _SCORE_MAX:
        raise JudgeParseError(f"score {score} is outside {_SCORE_MIN}-{_SCORE_MAX}")

    rationale = payload.get("rationale") or ""
    if not isinstance(rationale, str) or not rationale.strip():
        raise JudgeParseError("reply has no usable `rationale`")
    return score, rationale.strip()[:_MAX_RATIONALE]


def _balanced_objects(text: str) -> list[str]:
    """Every balanced {...} span, for a reply that forgot the fence."""
    out, depth, start = [], 0, None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}" and depth:
            depth -= 1
            if depth == 0 and start is not None:
                out.append(text[start : i + 1])
    return out


_REPAIR = (
    "Your previous reply could not be parsed. Reply with ONLY the fenced JSON "
    'block: ```json\n{"score": <integer 1-5>, "rationale": "..."}\n```'
)


async def score_once(
    chat: ChatFn, prompt: str, *, order: str = "n/a"
) -> JudgeScore:
    """One graded dimension: call, parse, retry once, then give up honestly.

    The retry APPENDS a repair instruction rather than resending the same
    prompt -- an identical retry re-rolls the same failure mode and buys
    nothing but a second bill.
    """
    messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
    model, cost, last_text = "", 0.0, ""

    for attempt in (1, 2):
        response = await chat(messages, system=_JUDGE_SYSTEM)
        model = getattr(response, "model", "") or model
        usage = getattr(response, "usage", None)
        cost += getattr(usage, "cost_usd", 0.0) or 0.0
        last_text = _text_of(response)
        try:
            score, rationale = parse_judge_reply(last_text)
        except JudgeParseError:
            if attempt == 2:
                break
            messages = [
                *messages,
                {"role": "assistant", "content": last_text or "(empty)"},
                {"role": "user", "content": _REPAIR},
            ]
            continue
        return JudgeScore(
            score=score, rationale=rationale, parse_ok=True,
            order=order, model=model, cost_usd=cost,
        )

    return JudgeScore(
        score=None,
        rationale=f"UNPARSEABLE: {last_text.strip()[:500]}",
        parse_ok=False,
        order=order,
        model=model,
        cost_usd=cost,
    )


def _text_of(response: Any) -> str:
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            block.get("text", "")
            for block in content
            if isinstance(block, dict) and block.get("type") == "text"
        )
    return ""


@dataclass(frozen=True, slots=True)
class JudgeResult:
    faithfulness: JudgeScore
    answer_quality: JudgeScore

    @property
    def cost_usd(self) -> float:
        return self.faithfulness.cost_usd + self.answer_quality.cost_usd

    @property
    def parse_failures(self) -> int:
        return int(not self.faithfulness.parse_ok) + int(not self.answer_quality.parse_ok)

    @property
    def rationale(self) -> str:
        return (
            f"faithfulness: {self.faithfulness.rationale}\n\n"
            f"answer_quality: {self.answer_quality.rationale}"
        )

    @property
    def model(self) -> str:
        return self.faithfulness.model or self.answer_quality.model


async def judge(
    chat: ChatFn,
    *,
    question: str,
    answer: str,
    reference: str,
    chunks: list[dict[str, str]],
    example_key: str,
    seed: int,
) -> JudgeResult:
    reference_first = order_for(seed, example_key)
    faithfulness = await score_once(chat, faithfulness_prompt(question, answer, chunks))
    quality = await score_once(
        chat,
        answer_quality_prompt(question, answer, reference, reference_first=reference_first),
        order="reference_first" if reference_first else "candidate_first",
    )
    return JudgeResult(faithfulness=faithfulness, answer_quality=quality)
