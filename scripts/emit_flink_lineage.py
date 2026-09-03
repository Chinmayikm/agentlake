#!/usr/bin/env python3
"""Declare the Kafka -> Iceberg edge of the lineage graph to Marquez.

    python scripts/emit_flink_lineage.py

Why this exists
---------------
``dbt-ol`` is a POST-processor: it runs dbt, then reads ``target/manifest.json``
and ``target/run_results.json`` and turns them into OpenLineage events. It can
therefore only ever describe dbt nodes. The lineage it produces starts at
``lake.raw.trace_events`` -- which is where dbt's world starts, and is one layer
short of the truth, because that table is written by a Flink job reading
``traces.events.v1``.

This script emits the missing edge.

Declared, not captured -- and the distinction matters
-----------------------------------------------------
Everything ``dbt-ol`` sends is **observed**: it describes a run that happened,
with the run's own timings and status. What this script sends is **declared**:
it asserts the shape of the Flink job -- this topic feeds these two tables --
without watching a run. The Marquez graph does not distinguish them, so this
file does, and so does ADR-006 #7.

Two things keep the declaration honest rather than decorative:

1. Every name is PARSED OUT of ``stream/flink/jobs/*.sql`` -- the topic, the
   bootstrap servers, the target table and the job name all come from the file
   Flink actually executes. Renaming the topic in the SQL renames it here.
   ``tests/test_analytics_project.py`` asserts the parse still finds all four.
2. The events carry ``eventType: COMPLETE`` with no run duration and a job
   description saying they are declared, so nothing reads a wall-clock number
   this script did not measure.

Capturing it for real means the ``openlineage-flink`` jar and a job
configuration change, and a change to a job's SQL is a state reset (ADR-004
#11): Flink derives operator IDs from the plan, so an edited query cannot resume
from its retained checkpoint. That is a real cost for a graph edge, and it is
the reason this is a declaration. Recorded as the gap rather than glossed.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
JOBS_DIR = REPO_ROOT / "stream" / "flink" / "jobs"

DEFAULT_MARQUEZ = os.environ.get("OPENLINEAGE_URL", "http://localhost:5000")
NAMESPACE = os.environ.get("OPENLINEAGE_NAMESPACE", "agentlake")

PRODUCER = "https://github.com/chinmayikm/agentlake/blob/main/scripts/emit_flink_lineage.py"
SCHEMA_URL = "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent"

#: The dataset namespace dbt-ol uses for Trino tables, which this MUST match or
#: the two halves of the graph land on different nodes with the same name and
#: never join up. dbt-ol derives it from the adapter's host and port, so it is
#: the container-internal address, not the host-side 8085.
TRINO_NAMESPACE = os.environ.get("AGENTLAKE_OL_TRINO_NAMESPACE", "trino://trino:8080")


def parse_job(path: Path) -> dict[str, str]:
    """Pull the job name, topic, brokers and target table out of one SQL file.

    Deliberately regex over the committed SQL rather than a hand-maintained
    table of names. A duplicated fact drifts; a parsed one cannot.
    """
    sql = path.read_text(encoding="utf-8")

    def one(pattern: str, what: str) -> str:
        match = re.search(pattern, sql)
        if not match:
            raise SystemExit(f"{path.name}: could not find {what}")
        return match.group(1)

    return {
        "job": one(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", "pipeline.name"),
        "topic": one(r"'topic'\s*=\s*'([^']+)'", "the Kafka topic"),
        "brokers": one(
            r"'properties\.bootstrap\.servers'\s*=\s*'([^']+)'", "bootstrap.servers"
        ),
        # Backticks because `raw` is a reserved word in Flink SQL (ADR-004 #7),
        # so the committed statement reads INSERT INTO lake.`raw`.trace_events.
        "table": one(r"INSERT\s+INTO\s+([`\w.]+)", "the INSERT target").replace("`", ""),
        "file": path.name,
    }


def parse_jobs() -> list[dict[str, str]]:
    files = sorted(JOBS_DIR.glob("*.sql"))
    if not files:
        raise SystemExit(f"no SQL jobs found under {JOBS_DIR}")
    return [parse_job(path) for path in files]


def build_event(job: dict[str, str], event_time: str) -> dict[str, Any]:
    return {
        "eventType": "COMPLETE",
        "eventTime": event_time,
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": str(uuid.uuid4())},
        "job": {
            "namespace": NAMESPACE,
            "name": job["job"],
            "facets": {
                "documentation": {
                    "_producer": PRODUCER,
                    "_schemaURL": (
                        "https://openlineage.io/spec/facets/1-0-0/"
                        "DocumentationJobFacet.json#/$defs/DocumentationJobFacet"
                    ),
                    "description": (
                        f"Flink SQL job stream/flink/jobs/{job['file']} "
                        f"(ADR-004). DECLARED lineage, not captured from a run: "
                        f"emitted by scripts/emit_flink_lineage.py from the job "
                        f"SQL itself. See ADR-006 #7."
                    ),
                },
                "sourceCode": {
                    "_producer": PRODUCER,
                    "_schemaURL": (
                        "https://openlineage.io/spec/facets/1-0-0/"
                        "SourceCodeJobFacet.json#/$defs/SourceCodeJobFacet"
                    ),
                    "language": "sql",
                    "sourceCode": f"stream/flink/jobs/{job['file']}",
                },
            },
        },
        "inputs": [{"namespace": f"kafka://{job['brokers']}", "name": job["topic"]}],
        "outputs": [{"namespace": TRINO_NAMESPACE, "name": job["table"]}],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default=DEFAULT_MARQUEZ, help=f"default: {DEFAULT_MARQUEZ}")
    parser.add_argument(
        "--dry-run", action="store_true", help="print the events instead of sending them"
    )
    args = parser.parse_args(argv)

    jobs = parse_jobs()
    event_time = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    events = [build_event(job, event_time) for job in jobs]

    if args.dry_run:
        print(json.dumps(events, indent=2))
        return 0

    endpoint = args.url.rstrip("/") + "/api/v1/lineage"
    with httpx.Client(timeout=30.0) as client:
        for job, event in zip(jobs, events, strict=True):
            response = client.post(endpoint, json=event)
            if response.is_error:
                raise SystemExit(
                    f"{job['job']}: HTTP {response.status_code}\n{response.text.strip()}"
                )
            print(
                f"declared  {job['job']:24} "
                f"kafka://{job['brokers']}/{job['topic']}  ->  {job['table']}"
            )

    print(f"\n{len(events)} lineage events sent to {endpoint} (namespace {NAMESPACE})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
