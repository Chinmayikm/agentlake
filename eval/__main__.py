"""`python -m eval <subcommand>` -- the eval harness CLI.

Subcommands that cost money say so, print an estimate, and refuse to start
without confirmation. Everything here is free unless its help text says
otherwise.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from eval.ab import MODES as MODES_DEFAULT


def cmd_corpus_paths(args: argparse.Namespace) -> int:
    """Regenerate eval/golden/corpus_paths.txt from the live corpus."""
    from eval.corpus import corpus_documents, render_corpus_paths
    from eval.dataset import CORPUS_PATHS_FILE
    from services.rag.fetch import load_corpus_version

    counts = corpus_documents()
    if not counts:
        print(
            "error: the corpus is empty for the configured corpus_version. "
            "Is qdrant up, and has `python -m services.rag ingest` run?",
            file=sys.stderr,
        )
        return 1

    rendered = render_corpus_paths(counts, load_corpus_version())
    out = Path(args.out) if args.out else CORPUS_PATHS_FILE
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(rendered, encoding="utf-8")
    print(f"wrote {out} -- {len(counts)} documents, {sum(counts.values())} chunks")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Load and validate the golden set, then print its shape."""
    from eval.dataset import CI_SUBSET_SIZE, ci_subset, load_corpus_paths, load_dataset

    examples = load_dataset()
    subset = ci_subset(examples)

    # A prefix that matches no document scores hit@k = 0 forever, and reads as
    # a retrieval regression rather than as the typo it is.
    corpus_paths = load_corpus_paths()
    unresolved = [
        (e.key, prefix)
        for e in examples
        for prefix in e.expected_sources
        if not any(p.startswith(prefix) for p in corpus_paths)
    ]
    unwritten = [
        (e.key, e.written_from_path)
        for e in examples
        if e.answerable and e.written_from_path not in corpus_paths
    ]
    print(f"{len(examples)} examples, {len(subset)} in the CI subset")
    print()

    for axis, get in (
        ("project", lambda e: e.project),
        ("type", lambda e: e.type),
        ("difficulty", lambda e: e.difficulty),
    ):
        counts: dict[str, tuple[int, int]] = {}
        for example in examples:
            total, ci = counts.get(get(example), (0, 0))
            counts[get(example)] = (total + 1, ci + (1 if example.ci else 0))
        rendered = "  ".join(f"{k}={t} (ci {c})" for k, (t, c) in sorted(counts.items()))
        print(f"  {axis:10s} {rendered}")

    unanswerable = [e for e in examples if not e.answerable]
    print(f"  {'answerable':10s} yes={len(examples) - len(unanswerable)}  no={len(unanswerable)}")

    failed = False
    if len(subset) != CI_SUBSET_SIZE:
        print(
            f"\nerror: the CI subset must be exactly {CI_SUBSET_SIZE} examples "
            f"(eval/baseline.json's thresholds are derived from 1/N)",
            file=sys.stderr,
        )
        failed = True
    for key, prefix in unresolved:
        print(
            f"\nerror: {key}: expected_source {prefix!r} matches no corpus document",
            file=sys.stderr,
        )
        failed = True
    for key, path in unwritten:
        print(f"\nerror: {key}: written_from {path!r} is not a corpus document", file=sys.stderr)
        failed = True

    if failed:
        return 1
    print(f"\nOK -- every expected_source resolves against {len(corpus_paths)} corpus documents")
    return 0


def cmd_ab(args: argparse.Namespace) -> int:
    """Retrieval-only A/B across modes. Needs qdrant; costs nothing."""
    from eval.ab import disagreements, render_report, run_ab
    from eval.dataset import load_dataset
    from services.rag.bm25 import BM25Index
    from services.rag.embed import FastEmbedEmbedder
    from services.rag.qdrant_store import QdrantStore, default_store

    examples = load_dataset()
    modes = tuple(args.modes.split(","))
    store = (
        QdrantStore(corpus_version=args.corpus_version)
        if args.corpus_version
        else default_store()
    )

    # Built once and threaded through every call: letting retrieve() construct
    # its own defaults would load the ONNX model ~255 times (ADR-003 #6).
    embedder, bm25_index = FastEmbedEmbedder(), BM25Index.load()

    print(f"corpus_version = {store.corpus_version}, k = {args.k}, modes = {modes}")
    print(f"{len([e for e in examples if e.answerable])} answerable examples\n")

    results = run_ab(
        examples, k=args.k, modes=modes, store=store, embedder=embedder, bm25_index=bm25_index
    )
    for mode, result in results.items():
        print(f"  {mode:>7}  hit@{args.k} = {result.hit_rate:.4f}   ({result.wall_s:.1f}s)")

    report = render_report(
        results,
        examples,
        k=args.k,
        corpus_version=store.corpus_version,
        heading=args.heading or f"corpus_version = {store.corpus_version}",
    ) + disagreements(results, examples)

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        existing = out.read_text(encoding="utf-8") if out.is_file() and args.append else ""
        out.write_text(existing + ("\n" if existing else "") + report, encoding="utf-8")
        print(f"\nwrote {out}")
    else:
        print()
        print(report)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m eval")
    sub = parser.add_subparsers(dest="command", required=True)

    corpus = sub.add_parser(
        "corpus-paths", help="regenerate eval/golden/corpus_paths.txt (needs qdrant)"
    )
    corpus.add_argument("--out", default=None)
    corpus.set_defaults(func=cmd_corpus_paths)

    validate = sub.add_parser("validate", help="load and validate the golden set (free)")
    validate.set_defaults(func=cmd_validate)

    ab = sub.add_parser(
        "ab", help="retrieval-only A/B across modes -- needs qdrant, costs nothing"
    )
    ab.add_argument("--k", type=int, default=5)
    ab.add_argument("--modes", default=",".join(MODES_DEFAULT))
    ab.add_argument("--out", default=None, help="write a markdown report here")
    ab.add_argument("--append", action="store_true", help="append to --out instead of replacing")
    ab.add_argument("--heading", default=None)
    ab.add_argument(
        "--corpus-version",
        default=None,
        help="override the store's corpus_version. `unknown` reproduces the "
        "pre-fix behaviour of ADR-008 #1 exactly, which is how the before/after "
        "table in docs/eval/retrieval_ab.md is measured rather than reconstructed",
    )
    ab.set_defaults(func=cmd_ab)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
