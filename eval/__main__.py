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
from eval.budget import DEFAULT_CAP_USD
from eval.label import DEFAULT_SAMPLE
from services.agent.loop import DEFAULT_PROMPT_VERSION


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


def cmd_load_golden(args: argparse.Namespace) -> int:
    """Publish eval/golden/*.yaml into golden_examples. Free; needs Postgres."""
    import json

    from eval.dataset import DATASET_VERSION, load_dataset
    from eval.db import UPSERT_GOLDEN
    from metadata.client import MetadataUnavailableError, connect, redacted_dsn

    examples = load_dataset()
    print(f"metadata: {redacted_dsn()}")
    print(f"{len(examples)} examples, dataset_version={DATASET_VERSION}\n")

    inserted = updated = unchanged = 0
    try:
        with connect() as conn, conn.cursor() as cur:
            for example in examples:
                cur.execute(
                    UPSERT_GOLDEN,
                    {
                        "example_key": example.key,
                        "question": example.question,
                        "expected_answer": example.expected_answer,
                        "expected_sources_json": json.dumps(list(example.expected_sources)),
                        "tags": [
                            example.project,
                            f"type:{example.type}",
                            f"difficulty:{example.difficulty}",
                            *(["ci"] if example.ci else []),
                            *(["unanswerable"] if not example.answerable else []),
                        ],
                        "dataset_version": DATASET_VERSION,
                    },
                )
                result = cur.fetchone()
                if result is None:
                    unchanged += 1
                elif result[1]:
                    inserted += 1
                else:
                    updated += 1

            # Never deletes. eval_results FK-reference these rows, and removing
            # one would rewrite history rather than correct it.
            cur.execute(
                "SELECT count(*) FROM golden_examples WHERE dataset_version = %(v)s "
                "AND example_key IS NOT NULL AND example_key <> ALL(%(keys)s)",
                {"v": DATASET_VERSION, "keys": [e.key for e in examples]},
            )
            orphaned = cur.fetchone()[0]
    except MetadataUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"{inserted} inserted, {updated} updated, {unchanged} unchanged")
    if orphaned:
        print(
            f"{orphaned} orphaned (in the database, not in the files) -- left alone. "
            f"eval_results reference them; deleting would rewrite history."
        )
    return 0


def _judge_model_id() -> str:
    """The provider model id the judge alias resolves to, from models.yaml.

    Recorded on the baseline so a later run can detect that the alias was
    re-pointed -- which is exactly what makes two "identical" runs
    incomparable, and is invisible from the alias alone.
    """
    from eval.judge import JUDGE_ALIAS
    from services.gateway.pricing import load_price_table

    return load_price_table().get(JUDGE_ALIAS).provider_model_id


def _select(args: argparse.Namespace):
    from eval.dataset import ci_subset, load_dataset

    examples = load_dataset()
    if args.subset == "ci":
        examples = ci_subset(examples)
    if args.limit:
        examples = examples[: args.limit]
    return examples


def cmd_run(args: argparse.Namespace) -> int:
    """**COSTS REAL MONEY.** Run the agent over the golden set and grade it."""
    import asyncio

    from eval.budget import BudgetExceeded
    from eval.harness import format_summary, git_sha
    from eval.runner import ExpensiveStepRefused, execute

    examples = _select(args)
    sha = git_sha()
    if args.require_clean and sha.endswith("-dirty"):
        print(
            "error: refusing to run against a dirty tree. A baseline pinned to a "
            "commit nobody can check out is not a baseline.",
            file=sys.stderr,
        )
        return 2
    if sha.endswith("-dirty"):
        print("WARNING: dirty tree; this run is not reproducible from a commit.")

    try:
        summary = asyncio.run(
            execute(
                examples,
                label=args.label,
                subset=args.subset,
                prompt_version=args.prompt_version,
                model_alias=args.model_alias,
                max_steps=args.max_steps,
                concurrency=args.concurrency,
                seed=args.seed,
                k=args.k,
                estimate_per_example=args.estimate_per_example,
                with_judge=not args.no_judge,
                yes=args.yes,
                cap_usd=args.cap,
                notes=args.notes,
                dry_run=args.dry_run,
            )
        )
    except (ExpensiveStepRefused, BudgetExceeded) as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 2
    if summary is None:
        return 0

    print(format_summary(summary, label=args.label))
    if args.gate:
        return _gate(summary, args)
    return 1 if summary.failures or summary.aborted else 0


def _gate(summary, args) -> int:
    from eval.baseline import Baseline, BaselineError, compare
    from eval.dataset import DATASET_VERSION
    from services.rag.fetch import load_corpus_version

    try:
        baseline = Baseline.load()
    except BaselineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    config = {
        "dataset_version": DATASET_VERSION,
        "corpus_version": load_corpus_version(),
        "prompt_version": args.prompt_version,
        "judge_model": baseline.config.get("judge_model"),
        "n": summary.n,
    }
    result = compare(
        baseline, summary.metrics(), config, allow_config_drift=args.allow_config_drift
    )
    print(result.render())

    # A harness failure fails on a DIFFERENT path from a quality regression:
    # "the gateway fell over" and "the answers got worse" need different
    # responses, and reporting the first as the second sends someone hunting a
    # regression that is not there.
    if summary.failures:
        print(f"HARNESS FAILURE: {summary.failures} example(s) errored", file=sys.stderr)
        return 3
    if summary.judge_parse_failures > 2:
        print(
            f"HARNESS FAILURE: {summary.judge_parse_failures} judge replies were "
            f"unparseable (limit 2)",
            file=sys.stderr,
        )
        return 3
    return 0 if result.passed else 1


def cmd_baseline(args: argparse.Namespace) -> int:
    """**COSTS REAL MONEY.** Two identical runs, then write eval/baseline.json."""
    import asyncio

    from eval.baseline import Baseline
    from eval.budget import BudgetExceeded
    from eval.dataset import DATASET_VERSION
    from eval.harness import format_summary, git_sha
    from eval.runner import ExpensiveStepRefused, execute
    from services.rag.fetch import load_corpus_version

    if args.from_runs:
        return _baseline_from_runs(args)

    sha = git_sha()
    if sha.endswith("-dirty"):
        print(
            "error: refusing to baseline a dirty tree. A baseline pinned to a "
            "commit nobody can check out is not a baseline.",
            file=sys.stderr,
        )
        return 2

    examples = _select(args)
    summaries = []
    for label in ("baseline A", "baseline B"):
        try:
            summary = asyncio.run(
                execute(
                    examples,
                    label=label,
                    subset=args.subset,
                    prompt_version=args.prompt_version,
                    model_alias=args.model_alias,
                    max_steps=args.max_steps,
                    concurrency=args.concurrency,
                    seed=args.seed,
                    k=args.k,
                    estimate_per_example=args.estimate_per_example,
                    with_judge=True,
                    yes=args.yes,
                    cap_usd=args.cap,
                    notes=f"{label} for eval/baseline.json",
                )
            )
        except (ExpensiveStepRefused, BudgetExceeded) as exc:
            print(f"\n{exc}", file=sys.stderr)
            return 2
        print(format_summary(summary, label=label))
        if summary.aborted or summary.failures:
            print(
                "error: a baseline run that aborted or had failures cannot be a "
                "baseline. Nothing was written.",
                file=sys.stderr,
            )
            return 2
        summaries.append(summary)

    baseline = Baseline.from_runs(
        summaries[0].metrics(),
        summaries[1].metrics(),
        {
            "git_sha": sha,
            "dataset_version": DATASET_VERSION,
            "corpus_version": load_corpus_version(),
            "prompt_version": args.prompt_version,
            "model_alias": args.model_alias,
            "max_steps": args.max_steps,
            "judge_model": _judge_model_id(),
            "judge_seed": args.seed,
            "n": summaries[0].n,
            "run_ids": [s.run_id for s in summaries],
        },
    )
    baseline.save()
    print("\nwrote eval/baseline.json")
    for key, value in baseline.thresholds.items():
        sigma = baseline.sigma.get(key, 0.0)
        print(f"  {key:<24} sigma={sigma:.4f}  threshold={value:.4f}")
    return 0


def _baseline_from_runs(args: argparse.Namespace) -> int:
    """Write eval/baseline.json from runs ALREADY in the metadata database.

    Free, and it is the honest way to salvage a baseline attempt that only
    half-happened. `make eval-baseline` spends ~$2.40 to produce two summaries
    in memory; those summaries are also rows in `eval_results`, so re-spending
    it to recompute numbers that are already stored would be paying twice for
    the same measurement.

    One run id gives a single-run baseline with declared thresholds; two give
    the sigma-derived ones. More than two is refused rather than averaged --
    `sigma_from_two` is exactly a two-observation estimator, and silently
    generalising it would make the file's threshold math a lie.
    """
    from eval.baseline import Baseline
    from eval.gate import RunNotFound, load_run
    from metadata.client import MetadataUnavailableError, connect, redacted_dsn

    ids = args.from_runs
    if len(ids) > 2:
        print(
            f"error: --from-runs takes 1 or 2 run ids, got {len(ids)}. The two-run "
            f"threshold math is a two-observation estimator (eval/baseline.py); "
            f"averaging more runs through it would not mean what it says.",
            file=sys.stderr,
        )
        return 2

    print(f"metadata: {redacted_dsn()}")
    try:
        with connect() as conn, conn.cursor() as cur:
            runs = [load_run(cur, run_id) for run_id in ids]
    except (MetadataUnavailableError, RunNotFound) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    for run in runs:
        print(
            f"  run {run.run_id}: n={run.n}  failures={run.failures}  "
            f"judge_parse_failures={run.judge_parse_failures}  "
            f"${run.total_cost_usd:.4f}  {'finished' if run.finished else 'UNFINISHED'}"
        )
        if run.failures or not run.finished:
            print(
                f"error: run {run.run_id} is not baselineable -- an unfinished run or "
                f"one with harness failures measures the harness, not the agent.",
                file=sys.stderr,
            )
            return 2

    # PINNED must agree across two runs, for the same reason compare() checks
    # it: a "baseline" averaged over two configurations describes neither.
    if len(runs) == 2:
        mismatched = [
            k for k in ("dataset_version", "corpus_version", "prompt_version", "judge_model", "n")
            if runs[0].config.get(k) != runs[1].config.get(k)
        ]
        if mismatched:
            print(
                f"error: runs {ids[0]} and {ids[1]} differ on {', '.join(mismatched)}. "
                f"They are not two observations of one configuration.",
                file=sys.stderr,
            )
            return 2

    config = {
        "git_sha": runs[0].config["git_sha"],
        "dataset_version": runs[0].config["dataset_version"],
        "corpus_version": runs[0].config["corpus_version"],
        "prompt_version": runs[0].config["prompt_version"],
        "model_alias": runs[0].config["model_alias"],
        "max_steps": runs[0].config["max_steps"],
        "judge_model": runs[0].config["judge_model"],
        "judge_seed": runs[0].config["judge_seed"],
        "n": runs[0].n,
        "run_ids": [r.run_id for r in runs],
    }
    if len(runs) == 1:
        baseline = Baseline.from_single(runs[0].metrics, config, notes=args.notes or "")
    else:
        baseline = Baseline.from_runs(runs[0].metrics, runs[1].metrics, config)
    baseline.save()

    print("\nwrote eval/baseline.json")
    for key, value in sorted(baseline.thresholds.items()):
        sigma = baseline.sigma.get(key)
        source = f"sigma={sigma:.4f}" if sigma is not None else "declared (no sigma)"
        print(f"  {key:<24}{baseline.metrics.get(key, 0.0) or 0.0:>9.4f}   "
              f"threshold={value:.4f}  {source}")
    if len(runs) == 1:
        print(
            "\nThis baseline rests on ONE run, so no run-to-run sigma was measured "
            "and\nthe thresholds are hand-set. `make eval-baseline` replaces it with "
            "the\nsigma-derived ones when there is budget for two runs."
        )
    return 0


def cmd_gate(args: argparse.Namespace) -> int:
    """Gate a run already in the metadata database against eval/baseline.json.

    Free -- it reads rows, it does not produce them. Exit 0 pass, 1 quality
    regression or config drift, 2 could not compare, 3 harness failure.
    """
    from eval.baseline import Baseline, BaselineError
    from eval.gate import RunNotFound, check, load_run
    from metadata.client import MetadataUnavailableError, connect

    try:
        baseline = Baseline.load()
    except BaselineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    try:
        with connect() as conn, conn.cursor() as cur:
            run = load_run(cur, args.run)
    except (MetadataUnavailableError, RunNotFound) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    result = check(baseline, run, allow_config_drift=args.allow_config_drift)
    print(result.render())

    # Same split as _gate(): "the harness fell over" and "the answers got
    # worse" need different responses, and reporting the first as the second
    # sends someone hunting a regression that is not there.
    if run.failures:
        print(f"HARNESS FAILURE: {run.failures} example(s) errored", file=sys.stderr)
        return 3
    if run.judge_parse_failures > 2:
        print(
            f"HARNESS FAILURE: {run.judge_parse_failures} judge replies were "
            f"unparseable (limit 2)",
            file=sys.stderr,
        )
        return 3
    return 0 if result.passed else 1


def cmd_label(args: argparse.Namespace) -> int:
    """Emit a stratified hand-labelling sheet from a completed run. Free."""
    import json

    from eval.db import LATEST_RUN, SELECT_RESULTS_FOR_RUN
    from eval.label import LabelRow, stratified_sample, write_sheet
    from metadata.client import MetadataUnavailableError, connect

    try:
        with connect() as conn, conn.cursor() as cur:
            if args.run == "latest":
                cur.execute(LATEST_RUN)
                row = cur.fetchone()
                if row is None:
                    print("error: no completed eval run to label", file=sys.stderr)
                    return 1
                run_id = row[0]
            else:
                run_id = int(args.run)

            cur.execute(SELECT_RESULTS_FOR_RUN, {"eval_run_id": run_id})
            fetched = cur.fetchall()
    except MetadataUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not fetched:
        print(f"error: run {run_id} has no results", file=sys.stderr)
        return 1

    rows = [
        LabelRow(
            eval_result_id=r[0],
            example_key=r[1],
            question=r[2],
            answer=r[3] or "",
            retrieved_sources=", ".join(json.loads(r[5] or "[]")),
        )
        for r in fetched
        # A row with no answer has nothing to label; including it would put a
        # blank in front of the labeller and invite a score for an absence.
        if r[3]
    ]
    # Stratify on (project, type), read off the key and the golden set.
    from eval.dataset import load_dataset

    by_key = {e.key: (e.project, e.type) for e in load_dataset()}
    strata = {r.eval_result_id: by_key.get(r.example_key, ()) for r in rows}

    sample = stratified_sample(rows, strata, args.n, run_id)
    csv_path, manifest_path = write_sheet(
        sample, run_id, {"source_rows": len(rows), "requested": args.n}
    )
    print(f"wrote {csv_path}  ({len(sample)} rows)")
    print(f"wrote {manifest_path}")
    print(
        "\nFill in the human_faithfulness column (1-5, using eval/rubric/faithfulness.md),\n"
        "leave rows you are unsure about blank, then run:\n"
        f"  make eval-agreement SHEET={csv_path}\n\n"
        "The judge's own score is deliberately NOT in this sheet -- seeing it would\n"
        "anchor the labelling and make the agreement number partly self-fulfilling."
    )
    return 0


def cmd_agreement(args: argparse.Namespace) -> int:
    """Judge-vs-human agreement from a filled sheet. Free."""
    import json

    from eval.db import SELECT_RESULTS_FOR_RUN
    from eval.label import (
        LabelRow,
        LabelSheetError,
        agreement,
        read_sheet,
        render_agreement,
        row_digest,
    )
    from metadata.client import MetadataUnavailableError, connect

    sheet_path = Path(args.sheet)
    manifest_path = sheet_path.with_suffix("").with_suffix(".manifest.json")
    if not manifest_path.is_file():
        manifest_path = sheet_path.parent / f"{sheet_path.stem}.manifest.json"
    if not manifest_path.is_file():
        print(f"error: no manifest beside {sheet_path}", file=sys.stderr)
        return 1
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_id = manifest["eval_run_id"]

    try:
        labels = read_sheet(sheet_path)
        with connect() as conn, conn.cursor() as cur:
            cur.execute(SELECT_RESULTS_FOR_RUN, {"eval_run_id": run_id})
            fetched = cur.fetchall()
    except (MetadataUnavailableError, LabelSheetError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    by_id = {r[0]: r for r in fetched}
    sampled = [
        LabelRow(eval_result_id=i, example_key=by_id[i][1], question=by_id[i][2],
                 answer=by_id[i][3] or "")
        for i in sorted(set(labels) | set(_manifest_ids(sheet_path)))
        if i in by_id
    ]
    # The stale-sheet guard, and it is exact rather than advisory.
    if row_digest(sampled) != manifest["row_digest"]:
        print(
            f"error: this sheet does not match run {run_id} as it now stands. The run "
            f"was re-run, or the rows were edited, or the sheet came from elsewhere.\n"
            f"Scoring a judge against answers it never produced would be worse than "
            f"not measuring at all.",
            file=sys.stderr,
        )
        return 1

    pairs, skipped = [], 0
    for result_id, human in sorted(labels.items()):
        judge_score = by_id.get(result_id, (None,) * 5)[4]
        if judge_score is None:
            skipped += 1  # an unparseable judge reply is not a disagreement
            continue
        pairs.append((int(judge_score), human))

    if not pairs:
        print("error: no labelled rows with a judge score to compare", file=sys.stderr)
        return 1

    result = agreement(pairs, skipped=skipped, unlabelled=len(sampled) - len(labels))
    report = render_agreement(
        result, {"eval_run_id": run_id, "sheet": str(sheet_path), **manifest}
    )
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")

    print(f"n = {result.n}")
    print(f"exact agreement    {result.exact:.1%}")
    print(f"within-1 agreement {result.within_one:.1%}")
    print(f"mean signed diff   {result.mean_signed_difference:+.2f}  (judge minus human)")
    print(f"kappa              {result.kappa}")
    print(f"weighted kappa     {result.weighted_kappa}")
    print(f"\nwrote {out}")
    return 0


def _manifest_ids(sheet_path: Path) -> list[int]:
    import csv

    with sheet_path.open(encoding="utf-8") as handle:
        return [int(row["eval_result_id"]) for row in csv.DictReader(handle)]


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

    load_golden = sub.add_parser(
        "load-golden", help="publish the golden set into golden_examples (free; needs Postgres)"
    )
    load_golden.set_defaults(func=cmd_load_golden)

    def add_run_flags(p: argparse.ArgumentParser) -> None:
        p.add_argument("--subset", choices=("ci", "all"), default="ci")
        p.add_argument("--limit", type=int, default=0, help="first N examples (for a pilot)")
        p.add_argument("--prompt-version", default=DEFAULT_PROMPT_VERSION)
        p.add_argument("--model-alias", default="fast")
        p.add_argument("--max-steps", type=int, default=8)
        p.add_argument("--concurrency", type=int, default=1)
        p.add_argument("--seed", type=int, default=7)
        p.add_argument("--k", type=int, default=5)
        p.add_argument(
            "--estimate-per-example",
            type=float,
            default=0.075,
            help="projected USD per example, used ONLY to refuse before spending "
            "(default: %(default)s, from ADR-007's measured turns plus a judge pair)",
        )
        p.add_argument("--cap", type=float, default=DEFAULT_CAP_USD)
        p.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
        p.add_argument("--notes", default="")

    run = sub.add_parser("run", help="**COSTS MONEY** run the agent over the set and grade it")
    add_run_flags(run)
    run.add_argument("--no-judge", action="store_true", help="mechanical metrics only (cheaper)")
    run.add_argument("--gate", action="store_true", help="compare against eval/baseline.json")
    run.add_argument("--allow-config-drift", action="store_true")
    run.add_argument("--require-clean", action="store_true", help="refuse a dirty git tree")
    run.add_argument("--dry-run", action="store_true", help="run every check, spend nothing")
    run.add_argument("--label", default="eval run")
    run.set_defaults(func=cmd_run)

    baseline = sub.add_parser(
        "baseline", help="**COSTS MONEY** two identical runs -> eval/baseline.json"
    )
    add_run_flags(baseline)
    baseline.add_argument(
        "--from-runs",
        nargs="+",
        type=int,
        default=[],
        metavar="RUN_ID",
        help="FREE. Build eval/baseline.json from runs already in the metadata "
        "database instead of executing new ones. One id gives a single-run "
        "baseline with declared thresholds; two give the sigma-derived ones.",
    )
    baseline.set_defaults(func=cmd_baseline)

    gate = sub.add_parser(
        "gate", help="gate a stored run against eval/baseline.json (free)"
    )
    gate.add_argument("--run", default="latest", help="run id, or 'latest'")
    gate.add_argument("--allow-config-drift", action="store_true")
    gate.set_defaults(func=cmd_gate)

    label = sub.add_parser("label", help="emit a hand-labelling sheet from a run (free)")
    label.add_argument("--run", default="latest", help="run id, or 'latest'")
    label.add_argument("--n", type=int, default=DEFAULT_SAMPLE)
    label.set_defaults(func=cmd_label)

    agreement = sub.add_parser(
        "agreement", help="judge-vs-human agreement from a filled sheet (free)"
    )
    agreement.add_argument("--sheet", required=True)
    agreement.add_argument("--out", default="docs/eval/agreement.md")
    agreement.set_defaults(func=cmd_agreement)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
