#!/usr/bin/env python3
"""Publish services/agent/prompts/*.md into the metadata database.

    python scripts/load_prompts.py [--dry-run] [--version v4]

**One direction only.** The files are the source of truth and this pushes them
into `prompt_versions`; nothing ever reads `template_text` back into the agent
(ADR-007 #6 -- an agent that refused to run because a metadata row was missing
would make the telemetry a dependency of the thing it observes). Whether the
database has caught up shows up downstream as
`lake.analytics.fct_cost_by_prompt.prompt_attribution='unknown'`, which is a
warn and not a gate for exactly this reason.

Why the row has to exist at all: `prompt_versions` is the dimension the trace
facts join to, so a prompt nobody published is a turn whose cost cannot be
attributed to the prompt that caused it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from metadata.client import MetadataUnavailableError, connect, redacted_dsn  # noqa: E402
from services.agent.prompts import (  # noqa: E402
    PROMPTS_DIR,
    available_versions,
    load_params,
    load_prompt,
)

PROMPT_NAME = "agent-system"

# ON CONFLICT DO UPDATE ... WHERE, and the WHERE is load-bearing.
#
# metadata-init and this script both run on every bring-up. An unconditional
# DO UPDATE writes a WAL record whether or not anything changed, so Debezium
# emits an `op='u'` for a row nobody touched -- and the changelog stops being a
# record of change. With the WHERE, re-running is a genuine no-op: no WAL, no
# CDC record, no spurious "the prompt changed" in the dimension's history.
# `params_json || EXCLUDED.params_json` MERGES rather than replaces: jsonb
# concatenation is right-biased, so this script's provenance keys win and every
# other key survives. The loader owns `source` and `sha256`; it does not own
# 07_seed.sql's descriptive keys (style, cite_sources, tool_hint,
# refuse_unsourced) and deleting them would be this writer discarding another
# writer's data because it happened to be in the same column. Same instinct as
# scripts/cdc_land.py ignoring an unknown key rather than crashing on it, and
# stg_prompt_versions flagging a deleted row rather than filtering it.
UPSERT_PROMPT = """
INSERT INTO prompt_versions (name, version, template_text, params_json)
VALUES (%(name)s, %(version)s, %(template_text)s, %(params_json)s)
ON CONFLICT (version) DO UPDATE
   SET name          = EXCLUDED.name,
       template_text = EXCLUDED.template_text,
       params_json   = prompt_versions.params_json || EXCLUDED.params_json
 WHERE prompt_versions.template_text IS DISTINCT FROM EXCLUDED.template_text
    OR prompt_versions.name          IS DISTINCT FROM EXCLUDED.name
    OR prompt_versions.params_json
       IS DISTINCT FROM prompt_versions.params_json || EXCLUDED.params_json
RETURNING id, (xmax = 0) AS inserted
"""

SELECT_EXISTING = "SELECT version, template_text FROM prompt_versions WHERE version = %(version)s"


def params_for(version: str, text: str) -> dict[str, object]:
    """Intent from params.yaml, plus provenance this script computes.

    `source` and `sha256` let a reader of the DIMENSION -- who has no access to
    this checkout -- tell whether the row still matches the file that produced
    it. Without them, a prompt edited but never loaded is indistinguishable
    from one that was. They are written last so a hand-edited params.yaml can
    never claim a digest it did not compute.
    """
    return {
        **load_params().get(version, {}),
        "source": f"services/agent/prompts/{version}.md",
        "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
    }


def load(versions: list[str], *, dry_run: bool = False) -> int:
    print(f"metadata: {redacted_dsn()}")
    print(f"prompts : {PROMPTS_DIR}")
    print()

    rows = [(v, load_prompt(v)) for v in versions]
    if dry_run:
        for version, text in rows:
            digest = params_for(version, text)["sha256"]
            print(f"  would upsert {version}  sha256={digest[:12]}…  {len(text)} chars")
        print(f"\ndry run: {len(rows)} prompt(s), nothing written")
        return 0

    inserted = updated = unchanged = 0
    rewritten: list[str] = []
    with connect() as conn, conn.cursor() as cur:
        for version, text in rows:
            # Read the old text BEFORE writing, so the two kinds of update can
            # be told apart. A params-only change is bookkeeping; a change to
            # template_text means previously recorded eval scores were measured
            # against words the dimension no longer holds, and conflating them
            # would make the loud warning below cry wolf on every metadata tweak
            # until nobody read it.
            cur.execute(SELECT_EXISTING, {"version": version})
            existing = cur.fetchone()
            text_changed = existing is not None and existing[1] != text

            cur.execute(
                UPSERT_PROMPT,
                {
                    "name": PROMPT_NAME,
                    "version": version,
                    "template_text": text,
                    "params_json": json.dumps(params_for(version, text)),
                },
            )
            result = cur.fetchone()
            if result is None:
                # The WHERE suppressed the update: the row already matches.
                unchanged += 1
                verdict = "unchanged"
            elif result[1]:
                inserted += 1
                verdict = "inserted"
            else:
                updated += 1
                verdict = "text changed" if text_changed else "params only"
                if text_changed:
                    rewritten.append(version)
            print(f"  {verdict:>12}  {version}  ({len(text)} chars)")

    print()
    print(f"{inserted} inserted, {updated} updated, {unchanged} unchanged")
    if rewritten:
        print(
            f"WARNING: template_text changed for {', '.join(rewritten)}. Any eval_results "
            "already attributed to those versions were scored against text the dimension "
            "no longer holds. The CDC changelog keeps the old text at its own LSN, so the "
            "old score is still traceable -- but a version is supposed to be immutable, and "
            "a real prompt change wants a NEW version, not an edit to an old one."
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python scripts/load_prompts.py")
    parser.add_argument(
        "--version",
        action="append",
        dest="versions",
        help="publish only this version (repeatable); default is every prompt file",
    )
    parser.add_argument("--dry-run", action="store_true", help="print what would be written")
    args = parser.parse_args(argv)

    versions = args.versions or available_versions()
    unknown = sorted(set(versions) - set(available_versions()))
    if unknown:
        print(f"error: no prompt file for {unknown}; have {available_versions()}", file=sys.stderr)
        return 2

    try:
        return load(versions, dry_run=args.dry_run)
    except MetadataUnavailableError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
