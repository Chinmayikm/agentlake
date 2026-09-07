"""The golden dataset: loading and validating eval/golden/*.yaml.

One file per project, mirroring services/rag/sources.yaml's shape. Per-project
rather than one big file because `project` then comes from the FILE, so it
cannot disagree with the content, and because 75 examples in one document is a
merge-conflict magnet.

Validation raises on the first problem rather than collecting every one: a
dataset that half-loads is worse than one that does not load, because the run
that used it produced a number over a set nobody chose.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

GOLDEN_DIR = Path(__file__).parent / "golden"
CORPUS_PATHS_FILE = GOLDEN_DIR / "corpus_paths.txt"

DATASET_VERSION = "v1"

#: The CI subset size. Fixed, not a fraction: the regression thresholds in
#: eval/baseline.json are derived from 1/N being the smallest movement a single
#: example can cause, so N changing silently would invalidate them.
CI_SUBSET_SIZE = 25

PROJECTS = ("kafka", "flink", "iceberg")
TYPES = ("factual", "config", "conceptual")
DIFFICULTIES = ("easy", "medium", "hard")

_KEY_RE = re.compile(r"^[a-z]+-[a-z0-9]+(?:-[a-z0-9]+)*-\d{3}$")


class DatasetError(ValueError):
    """A malformed golden file, naming the example and what is wrong with it."""


@dataclass(frozen=True, slots=True)
class GoldenExample:
    #: Stable identity. Assigned by hand and NEVER changed -- it is what lets a
    #: reworded question be an UPDATE (whose old text survives in the CDC
    #: changelog at its own LSN) rather than a new row that splits a metric's
    #: history in two.
    key: str
    project: str
    question: str
    #: The reference answer. 1-3 sentences, and required: the judge's
    #: answer_quality scale is "correct, complete, concise" measured against
    #: THIS, so an example without one cannot be scored on quality at all.
    expected_answer: str
    #: source_path PREFIXES. A hit is any retrieved chunk whose source_path
    #: starts with any of these -- OR, not AND: the question asks whether
    #: retrieval surfaced any of the right places, and requiring all of them
    #: would measure the corpus's redundancy rather than the retriever.
    #: Empty for an unanswerable example.
    expected_sources: tuple[str, ...]
    #: Which file and heading the question was WRITTEN FROM. Not used to score
    #: anything -- it exists so the authoring-bias rule is auditable: each
    #: question was written from a source file, and only then was retrieval
    #: run. Questions written from retrieval results are biased toward being
    #: retrievable and inflate hit@k by construction.
    written_from: str
    difficulty: str
    type: str
    #: In the CI regression subset.
    ci: bool = False
    #: False for a question the pinned corpus genuinely does not answer. These
    #: exist because "say plainly when the corpus does not answer the question"
    #: is an instruction v4 gives and v5 removes -- without them the degraded
    #: prompt would look merely terser. Excluded from the hit@k denominator;
    #: included in faithfulness and quality.
    answerable: bool = True
    tags: tuple[str, ...] = field(default_factory=tuple)

    @property
    def written_from_path(self) -> str:
        """`written_from` without its `#heading` part."""
        return self.written_from.split("#", 1)[0]


def _require(condition: bool, key: str, message: str) -> None:
    if not condition:
        raise DatasetError(f"{key}: {message}")


def _parse_example(raw: dict[str, Any], project: str, path: Path) -> GoldenExample:
    key = str(raw.get("key", "")).strip()
    _require(bool(key), f"<no key> in {path.name}", "every example needs a `key`")
    _require(bool(_KEY_RE.match(key)), key, f"key must match {_KEY_RE.pattern}")
    _require(
        key.startswith(f"{project}-"),
        key,
        f"key must start with the file's project ({project}-)",
    )

    question = str(raw.get("question", "")).strip()
    expected_answer = str(raw.get("expected_answer", "")).strip()
    written_from = str(raw.get("written_from", "")).strip()
    _require(bool(question), key, "`question` is required")
    _require(bool(expected_answer), key, "`expected_answer` is required")
    _require(bool(written_from), key, "`written_from` is required (the authoring-bias rule)")

    difficulty = str(raw.get("difficulty", "")).strip()
    type_ = str(raw.get("type", "")).strip()
    _require(difficulty in DIFFICULTIES, key, f"difficulty must be one of {DIFFICULTIES}")
    _require(type_ in TYPES, key, f"type must be one of {TYPES}")

    answerable = bool(raw.get("answerable", True))
    sources = tuple(str(s).strip() for s in raw.get("expected_sources") or ())
    if answerable:
        _require(bool(sources), key, "an answerable example needs at least one expected_source")
    else:
        _require(
            not sources,
            key,
            "an unanswerable example must have no expected_sources -- if the corpus "
            "does cover it, the example is answerable",
        )
        _require(
            written_from == "(not in the corpus)",
            key,
            "an unanswerable example's written_from must be literally "
            "'(not in the corpus)', since there is no source file it came from",
        )

    if answerable:
        _require(
            any(written_from.startswith(s) for s in sources),
            key,
            f"written_from {written_from!r} is not under any expected_source {sources} "
            "-- an example written from one file but scored against another is either "
            "a typo or a question that does not test what it claims to",
        )

    return GoldenExample(
        key=key,
        project=project,
        question=question,
        expected_answer=expected_answer,
        expected_sources=sources,
        written_from=written_from,
        difficulty=difficulty,
        type=type_,
        ci=bool(raw.get("ci", False)),
        answerable=answerable,
        tags=tuple(str(t) for t in raw.get("tags") or ()),
    )


def load_dataset(directory: Path = GOLDEN_DIR) -> list[GoldenExample]:
    """Every example across every project file, ordered by key.

    Deterministic order matters more than it looks: the label sheet's
    stratified sample and the judge's per-example RNG are both seeded off keys,
    so a run is only reproducible if the set is.
    """
    import yaml

    examples: list[GoldenExample] = []
    seen: dict[str, Path] = {}
    for path in sorted(directory.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        project = str(doc.get("project", "")).strip()
        _require(project in PROJECTS, path.name, f"`project` must be one of {PROJECTS}")
        _require(
            str(doc.get("dataset_version", "")) == DATASET_VERSION,
            path.name,
            f"`dataset_version` must be {DATASET_VERSION!r}",
        )
        for raw in doc.get("examples") or ():
            example = _parse_example(raw, project, path)
            if example.key in seen:
                raise DatasetError(
                    f"{example.key}: duplicate key, also in {seen[example.key].name}"
                )
            seen[example.key] = path
            examples.append(example)

    return sorted(examples, key=lambda e: e.key)


def ci_subset(examples: list[GoldenExample]) -> list[GoldenExample]:
    return [e for e in examples if e.ci]


def load_corpus_paths(path: Path = CORPUS_PATHS_FILE) -> list[str]:
    """The source_paths actually present in the corpus, generated by
    `python -m eval corpus-paths` and committed.

    Committed rather than queried, so the contract test that every
    expected_source resolves to a real document runs with no Qdrant. A typo'd
    prefix otherwise scores hit@k = 0 forever and reads as a retrieval
    regression rather than as a broken label.
    """
    if not path.is_file():
        return []
    # Each row is "<chunks>  <project>  <source_path>"; a source_path never
    # contains whitespace, so the last field is the whole path.
    return [
        line.split()[-1]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
