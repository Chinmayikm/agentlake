#!/usr/bin/env python3
"""Verify the hot path against the running stack, with real numbers.

    python3 scripts/hot_path_verify.py counts       # rows vs distinct spans vs topic
    python3 scripts/hot_path_verify.py freshness    # emit -> queryable latency
    python3 scripts/hot_path_verify.py panels       # time every dashboard query
    python3 scripts/hot_path_verify.py sample       # a few rows, all 13 fields

Everything here talks to ClickHouse over the HTTP interface on 8123, so it needs
no driver beyond httpx (already a dependency, see ADR-005 #1). It needs the
hotpath profile up; `counts` and `freshness` additionally need Kafka.

The numbers these subcommands print are what goes into ADR-005's verification
log, in the same shape as ADR-004's.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from services.sdk import flush, session, span, warmup  # noqa: E402

DEFAULT_URL = os.environ.get("AGENTLAKE_CLICKHOUSE", "http://localhost:8123")
DASHBOARD_DIR = REPO_ROOT / "dashboards" / "json"
TABLE = "agentlake.trace_events_rt"
TOPIC = "traces.events.v1"

# NFR-2 and NFR-5, the two numbers this script exists to check.
FRESHNESS_P95_TARGET_S = 5.0
PANEL_BUDGET_S = 1.0


# ---------------------------------------------------------------------------
# ClickHouse over HTTP
# ---------------------------------------------------------------------------


def ch_tsv(client: httpx.Client, sql: str) -> list[list[str]]:
    response = client.post("/", content=f"{sql} FORMAT TSV".encode())
    response.raise_for_status()
    text = response.text.strip()
    return [line.split("\t") for line in text.splitlines()] if text else []


def ch_json(client: httpx.Client, sql: str) -> dict[str, Any]:
    """FORMAT JSON, which carries a `statistics` block -- elapsed, rows_read,
    bytes_read -- measured by the server itself. That is the honest number for
    the panel budget: it excludes this script's own HTTP and JSON overhead,
    which Grafana would not pay either.
    """
    response = client.post("/", content=f"{sql} FORMAT JSON".encode())
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# counts -- verification step 1 and 4
# ---------------------------------------------------------------------------


def topic_offsets() -> int | None:
    """Total messages on traces.events.v1, straight from the broker. None if
    Kafka is not reachable -- the row counts are still worth printing.
    """
    try:
        out = subprocess.run(
            [
                "docker", "exec", "agentlake-kafka",
                "/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server", "kafka:19092",
                "--topic", TOPIC,
            ],
            capture_output=True, text=True, timeout=60, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    total = 0
    for line in out.stdout.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return total or None


def cmd_counts(client: httpx.Client) -> int:
    rows, spans, dups, earliest, latest = ch_tsv(
        client,
        f"SELECT count(), uniqExact(span_id), count() - uniqExact(span_id), "
        f"min(ts), max(ts) FROM {TABLE}",
    )[0]
    dlq = ch_tsv(client, "SELECT count() FROM agentlake.trace_events_dlq")[0][0]
    parts = ch_tsv(
        client,
        "SELECT count(), sum(rows) FROM system.parts "
        "WHERE database = 'agentlake' AND table = 'trace_events_rt' AND active",
    )[0]

    print(f"rows              {rows}")
    print(f"distinct span_ids {spans}")
    print(f"duplicates        {dups}", end="")
    print("   <- transient; collapses on the next merge" if int(dups) else "")
    print(f"dead letters      {dlq}")
    print(f"earliest          {earliest}")
    print(f"latest            {latest}")
    print(f"active parts      {parts[0]} holding {parts[1]} rows")

    offsets = topic_offsets()
    if offsets is None:
        print("\ntopic offsets     (kafka not reachable; skipped)")
        return 0
    print(f"\ntopic offsets     {offsets}")
    if int(spans) == offsets:
        print(f"MATCH             {spans} distinct spans == {offsets} messages on the topic")
        return 0
    print(f"MISMATCH          {spans} distinct spans != {offsets} messages on the topic")
    return 1


# ---------------------------------------------------------------------------
# freshness -- verification step 2 (NFR-2)
# ---------------------------------------------------------------------------


def cmd_freshness(client: httpx.Client, probes: int, timeout_s: float) -> int:
    """Emit a marked span through the real SDK, poll ClickHouse until it is
    visible, report the distribution.

    The marker is a per-run uuid plus a sequence number in the span's attributes
    map, which is a public part of the contract -- so this measures the same
    path a real span takes (SDK -> Avro -> Kafka -> ClickHouse Kafka engine ->
    materialized view -> MergeTree) with nothing stubbed or short-circuited.
    """
    run = uuid.uuid4().hex[:8]
    # Pay the producer/registry build once, up front, so probe 1 is not
    # measuring it. Same reason ADR-000 #3 gave warmup() its existence.
    warmup()

    latencies: list[float] = []
    timeouts = 0
    print(f"run {run}: {probes} probes, {timeout_s:.0f}s timeout each\n")

    with session(f"freshness-{run}"):
        for n in range(probes):
            marker = f"{run}-{n}"
            with span("AGENT_STEP", "freshness_probe", probe=marker):
                pass
            flush(timeout=10.0)
            started = time.perf_counter()

            found = False
            while time.perf_counter() - started < timeout_s:
                hit = ch_tsv(
                    client,
                    f"SELECT count() FROM {TABLE} "
                    f"WHERE attributes['probe'] = '{marker}'",
                )
                if hit and hit[0][0] != "0":
                    found = True
                    break
                time.sleep(0.05)

            elapsed = time.perf_counter() - started
            if found:
                latencies.append(elapsed)
                print(f"  probe {n + 1:>3}/{probes}  {elapsed * 1000:8.0f} ms", flush=True)
            else:
                timeouts += 1
                print(f"  probe {n + 1:>3}/{probes}  TIMEOUT after {timeout_s:.0f}s", flush=True)

    if not latencies:
        print("\nno probe became visible; is the materialized view attached?")
        return 1

    ordered = sorted(latencies)
    p50 = statistics.median(ordered)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    print(f"\nprobes    {len(latencies)} visible, {timeouts} timed out")
    print(f"min       {min(ordered) * 1000:.0f} ms")
    print(f"p50       {p50 * 1000:.0f} ms")
    print(f"p95       {p95 * 1000:.0f} ms")
    print(f"max       {max(ordered) * 1000:.0f} ms")
    ok = timeouts == 0 and p95 <= FRESHNESS_P95_TARGET_S
    verdict = "PASS" if ok else "FAIL"
    print(f"\n{verdict}      NFR-2 target is p95 <= {FRESHNESS_P95_TARGET_S:.0f}s")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# panels -- verification step 3 (NFR-5)
# ---------------------------------------------------------------------------

# Grafana's ClickHouse plugin substitutes these before the query is sent, so to
# time what a panel actually runs they have to be expanded the same way here.
# Deliberately not a general macro engine: an unrecognised macro should fail
# loudly rather than be timed as something it is not.
_MACRO_RE = re.compile(r"\$__(\w+)(?:\(([^()]*)\))?")


def expand_macros(sql: str, from_ms: int, to_ms: int, interval_s: int) -> str:
    def repl(match: re.Match[str]) -> str:
        name, arg = match.group(1), (match.group(2) or "").strip()
        if name == "timeFilter_ms":
            return (
                f"{arg} >= fromUnixTimestamp64Milli({from_ms}) "
                f"AND {arg} <= fromUnixTimestamp64Milli({to_ms})"
            )
        if name == "timeInterval_ms":
            return f"toStartOfInterval({arg}, INTERVAL {interval_s * 1000} millisecond)"
        if name == "fromTime_ms":
            return f"fromUnixTimestamp64Milli({from_ms})"
        if name == "toTime_ms":
            return f"fromUnixTimestamp64Milli({to_ms})"
        if name == "interval_s":
            return str(interval_s)
        raise SystemExit(f"unhandled Grafana macro $__{name} in:\n  {sql}")

    return _MACRO_RE.sub(repl, sql)


def dashboard_targets() -> list[tuple[str, str, str]]:
    """(dashboard, panel title, rawSql) for every SQL target on disk.

    Read from the dashboard files rather than from a copied list, so these
    timings cannot drift into measuring queries the dashboards do not run.
    """
    found: list[tuple[str, str, str]] = []
    for path in sorted(DASHBOARD_DIR.glob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        for panel in doc.get("panels", []):
            for target in panel.get("targets", []):
                sql = target.get("rawSql")
                if sql:
                    found.append((path.stem, panel.get("title", "?"), sql))
    return found


def cmd_panels(client: httpx.Client, window_hours: int, interval_s: int) -> int:
    targets = dashboard_targets()
    if not targets:
        print(f"no SQL targets found under {DASHBOARD_DIR}")
        return 1

    to_ms = int(time.time() * 1000)
    from_ms = to_ms - window_hours * 3600 * 1000
    print(
        f"{len(targets)} panel queries, {window_hours}h window, "
        f"{interval_s}s bucket, budget {PANEL_BUDGET_S * 1000:.0f} ms\n"
    )

    worst = 0.0
    failures = 0
    for dashboard, title, sql in targets:
        expanded = expand_macros(sql, from_ms, to_ms, interval_s)
        try:
            result = ch_json(client, expanded)
        except httpx.HTTPStatusError as exc:
            failures += 1
            print(f"  FAIL  {dashboard}/{title}\n        {exc.response.text.strip()[:300]}")
            continue
        elapsed = float(result["statistics"]["elapsed"])
        rows_read = result["statistics"]["rows_read"]
        worst = max(worst, elapsed)
        over = elapsed > PANEL_BUDGET_S
        failures += over
        flag = "OVER" if over else "ok  "
        print(
            f"  {flag}  {elapsed * 1000:7.1f} ms  {result['rows']:>4} rows out  "
            f"{rows_read:>7} read   {dashboard}/{title}"
        )

    print(f"\nslowest   {worst * 1000:.1f} ms")
    verdict = "PASS" if failures == 0 else "FAIL"
    print(f"{verdict}      NFR-5 target is every panel < {PANEL_BUDGET_S * 1000:.0f} ms")
    return 0 if failures == 0 else 1


# ---------------------------------------------------------------------------
# sample
# ---------------------------------------------------------------------------


def cmd_sample(client: httpx.Client, limit: int) -> int:
    result = ch_json(
        client,
        f"SELECT * FROM {TABLE} ORDER BY ts DESC LIMIT {limit}",
    )
    for row in result["data"]:
        print(json.dumps(row, indent=2, default=str))
    print(f"\n{result['rows']} rows, all {len(result['meta'])} columns")
    return 0


# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 scripts/hot_path_verify.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--url", default=DEFAULT_URL, help=f"ClickHouse HTTP (default: {DEFAULT_URL})"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("counts", help="rows vs distinct span_ids vs topic offsets")

    freshness = sub.add_parser("freshness", help="emit -> queryable latency (NFR-2)")
    freshness.add_argument("--probes", type=int, default=50)
    freshness.add_argument("--timeout", type=float, default=30.0, dest="timeout_s")

    panels = sub.add_parser("panels", help="time every dashboard query (NFR-5)")
    panels.add_argument("--window-hours", type=int, default=24)
    panels.add_argument("--interval-s", type=int, default=60)

    sample = sub.add_parser("sample", help="print a few whole rows")
    sample.add_argument("--limit", type=int, default=3)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    with httpx.Client(base_url=args.url.rstrip("/"), timeout=120.0) as client:
        if args.command == "counts":
            return cmd_counts(client)
        if args.command == "freshness":
            return cmd_freshness(client, args.probes, args.timeout_s)
        if args.command == "panels":
            return cmd_panels(client, args.window_hours, args.interval_s)
        if args.command == "sample":
            return cmd_sample(client, args.limit)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
