#!/usr/bin/env python3
"""Verify the analytics layer against the data it claims to summarise.

    python scripts/analytics_verify.py wait        # block until Trino is warm
    python scripts/analytics_verify.py counts      # marts vs staging vs raw
    python scripts/analytics_verify.py session     # hand-check one fct_sessions row
    python scripts/analytics_verify.py crosscheck  # Trino p95 vs ClickHouse p95

Runs on the host, in the repo's 3.14 venv, using httpx only -- see
analytics/trino_client.py for why it does not use trino-python-client. The
crosscheck subcommand additionally reuses services/mcp_server/clickhouse.py, so
the ClickHouse number it prints is produced by exactly the client the MCP tools
use rather than a second implementation that could disagree.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from analytics.trino_client import TrinoClient, TrinoError  # noqa: E402

CATALOG = "lake"


def _table(columns: list[str], rows: list[list[Any]]) -> str:
    """Render rows as a fixed-width table. No new dependency for a grid."""
    cells = [[("NULL" if v is None else str(v)) for v in row] for row in rows]
    widths = [
        max(len(str(columns[i])), *(len(row[i]) for row in cells)) if cells else len(columns[i])
        for i in range(len(columns))
    ]
    lines = ["  ".join(str(c).ljust(w) for c, w in zip(columns, widths, strict=True))]
    lines.append("  ".join("-" * w for w in widths))
    lines += ["  ".join(c.ljust(w) for c, w in zip(row, widths, strict=True)) for row in cells]
    return "\n".join(lines)


def _show(client: TrinoClient, sql: str, title: str = "") -> list[list[Any]]:
    result = client.execute(sql)
    if title:
        print(f"\n{title}")
    print(_table(result.columns, result.rows))
    return result.rows


# ---------------------------------------------------------------------------
# wait
# ---------------------------------------------------------------------------


def cmd_wait(client: TrinoClient, args: argparse.Namespace) -> int:
    """Block until Trino is ACTIVE *and* the Iceberg catalog is warm.

    Two separate facts, and only the first is what /v1/info reports. Trino
    accepts queries ~30s after the container starts, but the first query that
    touches the Iceberg catalog additionally pays plugin initialisation, the S3
    client build and a catalog round trip -- measured at 25.6s against a 0.4s
    steady state. That cold cost lands on whoever queries first, and dbt-trino
    exposes no request-timeout setting (the trino client's fixed 30s applies),
    so an unwarmed `dbt build` intermittently fails its first model with a read
    timeout that looks like a configuration error.

    Same finding, and the same fix, as ADR-000 #3 / ADR-003 #6 / ADR-005 #5:
    pay initialisation up front, deliberately, instead of charging it to the
    first real operation.
    """
    client.wait_until_ready(seconds=args.timeout)
    print(f"trino {client.url} ACTIVE")
    warm = client.execute(f"SELECT count(*) FROM {CATALOG}.raw.trace_events")
    print(f"catalog warm: {warm.rows[0][0]} rows in {CATALOG}.raw.trace_events "
          f"({warm.elapsed_ms:.0f} ms)")
    return 0


# ---------------------------------------------------------------------------
# counts
# ---------------------------------------------------------------------------


def cmd_counts(client: TrinoClient, args: argparse.Namespace) -> int:
    """Reconcile every mart against staging and against the raw Iceberg table.

    The question this answers is the only one that matters about a mart: does it
    account for exactly the rows it claims to? dbt's
    assert_marts_reconcile_with_staging test asserts the same thing and blocks
    the build; this prints the numbers so a human can see them.
    """
    raw = client.execute(
        f"SELECT count(*), count(DISTINCT span_id) FROM {CATALOG}.raw.trace_events"
    )
    raw_rows, raw_spans = raw.rows[0]

    print(f"{CATALOG}.raw.trace_events   {raw_rows} rows, {raw_spans} distinct span_ids, "
          f"{raw_rows - raw_spans} duplicates")

    _show(
        client,
        f"""
        SELECT event_type, count(*) AS spans
        FROM {CATALOG}.analytics.stg_trace_events
        GROUP BY event_type ORDER BY spans DESC
        """,
        "staging by event_type",
    )

    _show(
        client,
        f"""
        SELECT 'fct_sessions' AS mart, count(*) AS rows_,
               sum(span_count) AS spans_accounted,
               (SELECT count(*) FROM {CATALOG}.analytics.stg_trace_events) AS staging_spans
        FROM {CATALOG}.analytics.fct_sessions
        UNION ALL
        SELECT 'fct_model_costs', count(*), sum(calls),
               (SELECT count(*) FROM {CATALOG}.analytics.stg_trace_events
                WHERE event_type = 'LLM_CALL')
        FROM {CATALOG}.analytics.fct_model_costs
        UNION ALL
        SELECT 'fct_tool_reliability', count(*), sum(calls),
               (SELECT count(*) FROM {CATALOG}.analytics.stg_trace_events
                WHERE event_type = 'TOOL_CALL')
        FROM {CATALOG}.analytics.fct_tool_reliability
        """,
        "marts vs staging",
    )

    total = client.execute(
        f"SELECT sum(span_count) FROM {CATALOG}.analytics.fct_sessions"
    ).scalar()
    verdict = "MATCH" if total == raw_rows else "MISMATCH"
    print(f"\n{verdict}  fct_sessions accounts for {total} spans; "
          f"{CATALOG}.raw.trace_events holds {raw_rows}")
    return 0 if total == raw_rows else 1


# ---------------------------------------------------------------------------
# session
# ---------------------------------------------------------------------------


def cmd_session(client: TrinoClient, args: argparse.Namespace) -> int:
    """Print one fct_sessions row beside the raw spans it aggregates.

    A reconciliation query proves the totals agree with themselves. This proves
    one row agrees with the events a human can read -- which is the check that
    catches a mart that is consistently wrong.
    """
    session_id = args.session_id
    if not session_id:
        session_id = client.execute(
            f"""
            SELECT session_id FROM {CATALOG}.analytics.fct_sessions
            ORDER BY tool_call_count DESC, span_count DESC LIMIT 1
            """
        ).scalar()
        print(f"no --session-id given; using the busiest session {session_id}")

    print(f"\n=== fct_sessions row for {session_id} ===")
    _show(
        client,
        f"""
        SELECT turn_count, span_count, tool_call_count, llm_call_count, error_count,
               prompt_tokens, completion_tokens, total_tokens,
               round(total_cost_usd, 6) AS total_cost_usd, duration_ms
        FROM {CATALOG}.analytics.fct_sessions
        WHERE session_id = '{session_id}'
        """,
    )

    print("\n=== recomputed straight from the raw Iceberg table ===")
    _show(
        client,
        f"""
        SELECT count(DISTINCT trace_id) AS turn_count,
               count(*) AS span_count,
               count(*) FILTER (WHERE event_type = 'TOOL_CALL') AS tool_call_count,
               count(*) FILTER (WHERE event_type = 'LLM_CALL') AS llm_call_count,
               count(*) FILTER (WHERE status <> 'ok') AS error_count,
               coalesce(sum(prompt_tokens), 0) AS prompt_tokens,
               coalesce(sum(completion_tokens), 0) AS completion_tokens,
               coalesce(sum(prompt_tokens), 0) + coalesce(sum(completion_tokens), 0)
                   AS total_tokens,
               round(coalesce(sum(cost_usd), 0), 6) AS total_cost_usd,
               date_diff('millisecond', min(ts_epoch_ms), max(ts_epoch_ms)) AS duration_ms
        FROM {CATALOG}.raw.trace_events
        WHERE session_id = '{session_id}'
        """,
    )

    print("\n=== the spans themselves, by event type ===")
    _show(
        client,
        f"""
        SELECT event_type,
               count(*) AS spans,
               count(DISTINCT trace_id) AS traces,
               count(*) FILTER (WHERE status <> 'ok') AS errors,
               round(coalesce(sum(cost_usd), 0), 6) AS cost_usd
        FROM {CATALOG}.raw.trace_events
        WHERE session_id = '{session_id}'
        GROUP BY event_type ORDER BY spans DESC
        """,
    )
    return 0


# ---------------------------------------------------------------------------
# crosscheck
# ---------------------------------------------------------------------------


def cmd_crosscheck(client: TrinoClient, args: argparse.Namespace) -> int:
    """Trino's EXACT p95 against ClickHouse's approximate quantile(0.95).

    This is the measurement that makes ADR-004 #6's deferral concrete. The cold
    path could not compute a percentile at all; the hot path answered it with a
    sampling quantile; the batch layer answers it exactly, from every value. The
    two numbers should be close, and the direction and size of the gap is the
    price of ClickHouse's approximation -- which is worth knowing rather than
    assuming.

    Needs the hot path running as well as the analytics slice. Start ONLY
    clickhouse for it (`docker compose up -d clickhouse`), not the whole hotpath
    profile -- grafana is 140 MiB this does not need.
    """
    from services.mcp_server.clickhouse import ClickHouseClient, ClickHouseUnavailable

    trino_rows = client.execute(
        f"""
        SELECT cast(event_day AS varchar) AS event_day, model, calls,
               latency_p95_ms, latency_avg_ms, latency_max_ms
        FROM {CATALOG}.analytics.fct_model_costs
        ORDER BY model
        """
    ).dicts()

    if not trino_rows:
        print("fct_model_costs is empty -- run `make dbt-build` first")
        return 1

    clickhouse = ClickHouseClient()
    try:
        if not clickhouse.ping():
            raise ClickHouseUnavailable("ping failed")
    except ClickHouseUnavailable as exc:
        print(f"ClickHouse unreachable at {clickhouse.url}: {exc}")
        print("Start it with:  docker compose up -d clickhouse")
        return 1

    header = [
        "event_day", "model", "calls", "trino_p95_exact", "ch_p95_approx", "delta_ms", "delta_pct",
    ]
    comparable: list[list[Any]] = []
    skipped: list[list[Any]] = []
    worst_pct = 0.0

    for row in trino_rows:
        event_day = row["event_day"]
        model = row["model"]
        # Bound as HTTP parameters, never interpolated -- the rule
        # services/mcp_server/clickhouse.py sets and ADR-005 #5 explains.
        ch = clickhouse.query(
            """
            SELECT uniqExact(span_id) AS calls,
                   quantile(0.95)(latency_ms) AS p95
            FROM agentlake.trace_events_rt
            WHERE event_type = 'LLM_CALL'
              AND toDate(ts) = toDate({event_day:String})
              AND model = {model:String}
            """,
            {"event_day": event_day, "model": model or ""},
        )
        ch_calls = int(ch[0]["calls"]) if ch else 0
        trino_calls = int(row["calls"])
        trino_p95 = float(row["latency_p95_ms"])

        # Only a group both stores hold in FULL is a like-for-like comparison.
        # Iceberg is the archive and ClickHouse expires at 7 days (ADR-005 #3),
        # so a day present in one and partial in the other says nothing about
        # either engine's percentile -- reporting it as a delta would be
        # measuring the retention difference and calling it an accuracy result.
        if ch_calls != trino_calls or ch_calls == 0:
            skipped.append([
                event_day, model, f"trino {trino_calls} / clickhouse {ch_calls}",
                "different ingest history -- not comparable",
            ])
            continue

        ch_p95 = float(ch[0]["p95"])
        delta = ch_p95 - trino_p95
        pct = (delta / trino_p95 * 100.0) if trino_p95 else 0.0
        worst_pct = max(worst_pct, abs(pct))
        comparable.append([
            event_day, model, trino_calls,
            f"{trino_p95:.3f}", f"{ch_p95:.3f}", f"{delta:+.3f}", f"{pct:+.2f}%",
        ])

    print("\nTrino  lake.analytics.fct_model_costs  -- EXACT nearest-rank p95, from every value")
    print("vs")
    print("ClickHouse  agentlake.trace_events_rt   -- quantile(0.95), APPROXIMATE (sampling)\n")

    if comparable:
        print(_table(header, comparable))
        print(
            f"\nworst divergence {worst_pct:.2f}% across {len(comparable)} "
            f"group(s) where both stores hold the identical span set."
        )
    else:
        print("No group is held identically by both stores, so nothing is comparable.")
        print("Generate traffic with the hot path AND the cold path both consuming, then")
        print("rebuild the marts -- see the runbook in CLAUDE.md.")

    if skipped:
        print("\nnot compared:")
        print(_table(["event_day", "model", "calls", "why"], skipped))

    return 0 if comparable else 1


# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default=None, help="Trino base URL (default: $AGENTLAKE_TRINO)")
    sub = parser.add_subparsers(dest="command", required=True)

    wait = sub.add_parser("wait", help="block until Trino is ready and the catalog is warm")
    wait.add_argument("--timeout", type=float, default=180.0)
    wait.set_defaults(func=cmd_wait)

    counts = sub.add_parser("counts", help="marts vs staging vs raw")
    counts.set_defaults(func=cmd_counts)

    session = sub.add_parser("session", help="hand-check one fct_sessions row")
    session.add_argument("--session-id", default=None)
    session.set_defaults(func=cmd_session)

    crosscheck = sub.add_parser("crosscheck", help="Trino exact p95 vs ClickHouse quantile")
    crosscheck.set_defaults(func=cmd_crosscheck)

    args = parser.parse_args(argv)
    client = TrinoClient(url=args.url) if args.url else TrinoClient()

    try:
        return int(args.func(client, args))
    except TrinoError as exc:
        print(f"trino: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
