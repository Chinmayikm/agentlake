"""Retrieval A/B: dense vs bm25 vs hybrid over the whole golden set.

**No agent, no LLM, no money.** One `retrieve()` call per (question, mode), so
the only thing that has to be running is qdrant. That is what makes this the
one measurement in ADR-008 that covers all 85 examples rather than the
25-example CI subset.

It calls `services.rag.retrieve()` directly rather than going through the MCP
server. That is not a breach of ADR-003 #1, whose rule binds `services/agent`:
the point here is to measure the RETRIEVER, and routing through the tool would
measure the model's tool-argument choices instead.

`--corpus-version` exists so the pre-fix behaviour of ADR-008 #1 is
reproducible rather than merely described: running with `unknown` rebuilds the
store the way `retrieve()` used to build it, and the report shows dense at
0.0000 with hybrid identical to bm25.
"""

from __future__ import annotations

import time
from collections.abc import Sequence
from dataclasses import dataclass, field

from eval.dataset import GoldenExample
from eval.metrics import Retrieved, matches

MODES = ("dense", "bm25", "hybrid")


@dataclass(slots=True)
class ModeResult:
    mode: str
    hits: dict[str, bool] = field(default_factory=dict)
    #: chunk_id lists per example key, so "is hybrid just bm25?" is answerable
    #: without re-running anything.
    rankings: dict[str, tuple[str, ...]] = field(default_factory=dict)
    wall_s: float = 0.0

    @property
    def hit_rate(self) -> float:
        return (sum(self.hits.values()) / len(self.hits)) if self.hits else 0.0

    def hit_rate_for(self, keys: Sequence[str]) -> float:
        subset = [self.hits[k] for k in keys if k in self.hits]
        return (sum(subset) / len(subset)) if subset else 0.0


def run_ab(
    examples: Sequence[GoldenExample],
    *,
    k: int = 5,
    modes: Sequence[str] = MODES,
    store=None,
    embedder=None,
    bm25_index=None,
) -> dict[str, ModeResult]:
    """One retrieve() per (answerable example, mode).

    The store, embedder and BM25 index are built ONCE and passed in to every
    call. Letting `retrieve()` construct its own defaults would load fastembed's
    ONNX model on every one of ~255 calls -- ADR-003 #6's observer-effect
    lesson, third instance.

    Unanswerable examples are excluded: they have no expected_sources, so every
    mode would score 0 and the only thing that would change is the denominator.
    """
    scorable = [e for e in examples if e.answerable]
    results: dict[str, ModeResult] = {}

    for mode in modes:
        result = ModeResult(mode=mode)
        started = time.perf_counter()
        for example in scorable:
            chunks = _retrieve(
                example.question, k, mode, store=store, embedder=embedder, bm25_index=bm25_index
            )
            retrieved = [
                Retrieved(source_path=c.source_path, section_path=c.section) for c in chunks
            ]
            result.hits[example.key] = matches(example.expected_sources, retrieved)
            result.rankings[example.key] = tuple(c.chunk_id for c in chunks)
        result.wall_s = time.perf_counter() - started
        results[mode] = result

    return results


def _retrieve(query, k, mode, *, store, embedder, bm25_index):
    from services.rag.retrieve import retrieve

    return retrieve(query, k, mode=mode, store=store, embedder=embedder, bm25_index=bm25_index)


def render_report(
    results: dict[str, ModeResult],
    examples: Sequence[GoldenExample],
    *,
    k: int,
    corpus_version: str,
    heading: str,
) -> str:
    """A markdown section for docs/eval/retrieval_ab.md."""
    scorable = [e for e in examples if e.answerable]
    by_project: dict[str, list[str]] = {}
    for e in scorable:
        by_project.setdefault(e.project, []).append(e.key)

    by_type: dict[str, list[str]] = {}
    for e in scorable:
        by_type.setdefault(e.type, []).append(e.key)

    unanswerable = len(examples) - len(scorable)
    lines = [
        f"### {heading}",
        "",
        f"`corpus_version = {corpus_version}`, k = {k}, {len(scorable)} answerable examples "
        f"({unanswerable} unanswerable examples excluded -- they have no expected source, so "
        f"every mode scores 0 and only the denominator moves).",
        "",
        f"| mode | hit@{k} | kafka | flink | iceberg | wall |",
        "|---|---|---|---|---|---|",
    ]
    for mode, result in results.items():
        per_project = " | ".join(
            f"{result.hit_rate_for(by_project.get(p, [])):.4f}"
            for p in ("kafka", "flink", "iceberg")
        )
        lines.append(
            f"| `{mode}` | **{result.hit_rate:.4f}** | {per_project} | {result.wall_s:.1f}s |"
        )

    # By question type, because the headline number is confounded by how the
    # questions were written. A set authored from prose sections is mostly
    # paraphrase, which is dense's home ground; `config` questions are the ones
    # that name an exact identifier, which is BM25's. Reporting the split is
    # what turns "hybrid lost" into a claim that can be checked.
    types = sorted(by_type)
    lines += [
        "",
        "| mode | " + " | ".join(f"{t} (n={len(by_type[t])})" for t in types) + " |",
        "|---|" + "---|" * len(types),
    ]
    for mode, result in results.items():
        cells = " | ".join(f"{result.hit_rate_for(by_type[t]):.4f}" for t in types)
        lines.append(f"| `{mode}` | {cells} |")

    if "hybrid" in results and "dense" in results:
        delta = results["hybrid"].hit_rate - results["dense"].hit_rate
        lines += ["", f"**hybrid - dense = {delta:+.4f}**"]
    if "hybrid" in results and "bm25" in results:
        identical = sum(
            1
            for key in results["hybrid"].rankings
            if results["hybrid"].rankings[key] == results["bm25"].rankings.get(key)
        )
        total = len(results["hybrid"].rankings)
        lines.append(
            f"hybrid's top-{k} is byte-identical to bm25's for **{identical}/{total}** examples."
        )
    return "\n".join(lines) + "\n"


def disagreements(
    results: dict[str, ModeResult], examples: Sequence[GoldenExample], *, limit: int = 12
) -> str:
    """The examples where hybrid wins and dense does not, and vice versa.

    ADR-002 #3 argued for hybrid from two hand-picked queries. This is the same
    argument over the whole set, which is what makes it evidence rather than an
    anecdote.
    """
    if not {"dense", "hybrid", "bm25"} <= set(results):
        return ""
    by_key = {e.key: e for e in examples}
    rows = []
    for key in sorted(results["hybrid"].hits):
        d, b, h = (results[m].hits[key] for m in ("dense", "bm25", "hybrid"))
        if h != d or h != b:
            rows.append((key, d, b, h))

    lines = [
        "",
        "### Where the modes disagree",
        "",
        "| example | dense | bm25 | hybrid | question |",
        "|---|---|---|---|---|",
    ]
    tick = {True: "hit", False: "-"}
    for key, d, b, h in rows[:limit]:
        question = by_key[key].question
        question = question if len(question) <= 62 else question[:59] + "..."
        lines.append(f"| `{key}` | {tick[d]} | {tick[b]} | {tick[h]} | {question} |")
    if len(rows) > limit:
        lines.append(f"| ... | | | | {len(rows) - limit} more |")
    return "\n".join(lines) + "\n"
