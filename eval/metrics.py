"""Metric computation. Pure functions, no IO, no network -- so every number the
harness publishes is unit-testable against hand-worked examples.

Three decisions here are load-bearing and are argued in ADR-008:

1. hit@k is scored on the FIRST search_docs call, not the union of every call.
2. "the agent never searched" is None, not False.
3. citation_ok is a smoke detector and is reported, never gated.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Retrieved:
    """A retrieved chunk, reduced to what scoring needs.

    Both the agent's in-loop RetrievedRef and a trace-derived source path
    convert to this, which is what lets the two hit@k sources be compared
    against each other rather than merely believed to agree.
    """

    source_path: str
    section_path: str = ""
    call_index: int = 0


def first_call(retrieved: Sequence[Retrieved]) -> list[Retrieved]:
    """The chunks the FIRST search_docs call returned.

    Scoring the union of every call would measure an agent that can brute-force
    the metric by searching five times, not the retriever -- and it would stop
    `make eval-ab`, which issues exactly one retrieval per question, from being
    comparable to a full run. `any_call_hit` exists separately for the
    diagnostic question "did it ever find it".
    """
    if not retrieved:
        return []
    lowest = min(r.call_index for r in retrieved)
    return [r for r in retrieved if r.call_index == lowest]


def matches(prefixes: Iterable[str], retrieved: Iterable[Retrieved]) -> bool:
    """Did any retrieved chunk come from any of the expected source paths?

    OR across expectations, not AND: the question is whether retrieval
    surfaced any of the right places. Requiring all of them would measure how
    redundantly the corpus covers a topic rather than how well the retriever
    finds it.
    """
    prefixes = tuple(prefixes)
    if not prefixes:
        return False
    return any(r.source_path.startswith(p) for r in retrieved for p in prefixes)


def hit_at_k(
    expected_sources: Sequence[str],
    retrieved: Sequence[Retrieved],
    k: int = 5,
    *,
    searched: bool | None = None,
) -> bool | None:
    """True / False / None, and None is the point.

    None means the agent issued no search at all. That is a different failure
    from the retriever missing, and collapsing them to False would make a
    prompt change that stops the agent searching look like a retrieval
    regression -- which is exactly the v4-to-v5 case the gate has to diagnose,
    not merely detect.

    `searched` lets a caller distinguish "no search" from "a search that
    returned nothing", which are also different: an empty result set is the
    retriever answering.
    """
    first = first_call(retrieved)[:k]
    if not first and not (searched or False):
        return None
    return matches(expected_sources, first)


def any_call_hit(expected_sources: Sequence[str], retrieved: Sequence[Retrieved]) -> bool:
    """Reported, never gated -- see first_call()."""
    return matches(expected_sources, retrieved)


# ---------------------------------------------------------------------------
# citation_ok -- a smoke detector, documented as such
# ---------------------------------------------------------------------------
#
# True iff the answer names something identifying about a chunk it was given:
# the basename of a retrieved source_path (with or without extension), or the
# last segment of its section breadcrumb.
#
# What it CANNOT do, stated here rather than discovered later:
#   - tell a citation from a coincidence. An answer about Iceberg that happens
#     to contain the word "evolution" scores True whether or not it meant
#     evolution.md.
#   - check that the cited source SUPPORTS the claim. That is faithfulness's
#     job, and it needs a judge, not string overlap.
#   - recognise a correct answer that cites in prose ("the design docs say")
#     rather than by filename. Those score False.
#
# It is reported so that a COLLAPSE in it is visible -- the signal that a
# prompt stopped asking for citations at all -- not so that any particular
# value is believed.

_STOPWORDS = frozenset(
    {
        "index", "docs", "doc", "overview", "intro", "common", "config",
        "configuration", "content", "the", "and", "with", "from", "into",
    }
)
_MIN_TOKEN = 4


def _citation_tokens(retrieved: Iterable[Retrieved]) -> set[str]:
    tokens: set[str] = set()
    for r in retrieved:
        basename = r.source_path.rsplit("/", 1)[-1]
        stem = basename.rsplit(".", 1)[0]
        for candidate in (basename, stem, *stem.replace("-", "_").split("_")):
            if len(candidate) >= _MIN_TOKEN and candidate.lower() not in _STOPWORDS:
                tokens.add(candidate.lower())
        if r.section_path:
            leaf = r.section_path.split(" > ")[-1].strip()
            if len(leaf) >= _MIN_TOKEN and leaf.lower() not in _STOPWORDS:
                tokens.add(leaf.lower())
    return tokens


def citation_ok(answer: str, retrieved: Iterable[Retrieved]) -> bool:
    haystack = " ".join(answer.lower().split())
    return any(token in haystack for token in _citation_tokens(retrieved))


# ---------------------------------------------------------------------------
# Aggregation and the length-control correlation
# ---------------------------------------------------------------------------


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Pearson r, or None when it is undefined.

    None rather than 0.0 for a constant series: a judge that gave every answer
    a 5 has NO measurable relationship with length, and reporting that as
    "r = 0.0, no length bias" would be a claim the data cannot support.
    """
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x, mean_y = sum(xs) / len(xs), sum(ys) / len(ys)
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]
    denom = math.sqrt(sum(d * d for d in dx)) * math.sqrt(sum(d * d for d in dy))
    if denom == 0:
        return None
    return sum(a * b for a, b in zip(dx, dy, strict=True)) / denom


def mean(values: Sequence[float | None]) -> float | None:
    present = [v for v in values if v is not None]
    return sum(present) / len(present) if present else None


def rate(values: Sequence[bool | None], *, none_counts_as: bool | None = False) -> float | None:
    """Fraction of True.

    `none_counts_as` is explicit at every call site on purpose. The headline
    hit rate counts a None (the agent never searched) as a miss, because that
    is what a user experiences; `hit_at_5_given_search` excludes them, because
    that is what the retriever was actually asked. Two different questions,
    and a default would hide which one is being answered.
    """
    if none_counts_as is None:
        considered = [v for v in values if v is not None]
    else:
        considered = [none_counts_as if v is None else v for v in values]
    if not considered:
        return None
    return sum(1 for v in considered if v) / len(considered)


def approx_tokens(text: str) -> int:
    """Fallback answer length when no provider token count is available.

    len//4 measures a tokenizer nobody has, which is why the harness uses the
    gateway's own `completion_tokens` instead. This exists only for the A/B and
    label paths, which never call a model. Same rough convention already used
    in services/rag/chunk.py.
    """
    return max(1, len(re.sub(r"\s+", " ", text.strip())) // 4)
