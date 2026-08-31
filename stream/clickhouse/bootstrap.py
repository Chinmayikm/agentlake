"""Apply the hot path's ClickHouse schema.

Run from the repo root with the hotpath profile up::

    python -m stream.clickhouse.bootstrap

Idempotent: every statement in ``stream/clickhouse/sql/`` is ``CREATE ... IF NOT
EXISTS``, so re-running after a partial failure is safe and re-running after a
success is a no-op.

Why order matters
-----------------
The files are numbered because apply order is load-bearing, not cosmetic. A
Kafka engine table starts consuming when a materialized view attaches to it, and
``CREATE MATERIALIZED VIEW ... TO`` does not create its target -- so the target
tables must exist, then the queue, then the views. Applying them in the wrong
order fails; applying them in filename order is correct by construction, which
is what this script guarantees.

Why raw HTTP and not clickhouse-connect
---------------------------------------
``httpx`` is already a dependency. ClickHouse speaks SQL over ``POST /`` on 8123
and returns JSON on request, which is the whole of what a DDL apply and a few
SELECTs need; clickhouse-connect would be a new dependency (plus optional
pyarrow) to do the same thing with a typed columnar result nothing here wants.
Same argument ADR-004 #2 made against pyiceberg, and the same one
``services/mcp_server/clickhouse.py`` follows.

Why there is no resume guard
----------------------------
``stream/flink/submit.sh`` refuses a plain submit while a resume point exists,
because Flink keeps its Kafka offsets in checkpointed state and a fresh start
would replay from earliest into a populated table (ADR-004 #11). This consumer's
offsets live in the broker under group ``clickhouse-hotpath``, so a restart
resumes by itself and the footgun does not exist. ``--recreate`` is still
destructive -- it drops the data -- but it cannot silently duplicate anything.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import httpx

DEFAULT_URL = os.environ.get("AGENTLAKE_CLICKHOUSE", "http://localhost:8123")

REPO_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent / "sql"
CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"

DATABASE = "agentlake"

# What each SQL file creates, so the script can report created/exists rather
# than printing "ok" for a no-op -- CREATE ... IF NOT EXISTS returns 200 either
# way, so existence has to be checked separately.
#
# Also the drop list for --recreate, consumed in reverse: views first (which
# stops consumption), then the queue, then the tables. Dropping a target while
# a view still points at it leaves the view broken.
OBJECTS: list[tuple[str, str]] = [
    ("01_database.sql", ""),  # the database itself; checked against system.databases
    ("02_trace_events_rt.sql", "trace_events_rt"),
    ("03_trace_events_dlq.sql", "trace_events_dlq"),
    ("04_trace_events_kafka.sql", "trace_events_kafka"),
    ("05_trace_events_mv.sql", "trace_events_mv"),
    ("06_trace_events_dlq_mv.sql", "trace_events_dlq_mv"),
]


def _execute(client: httpx.Client, sql: str) -> str:
    """POST one statement. Returns the response body; raises on any error.

    The traceback is the error report, same as stream/flink/create_tables.py --
    ClickHouse puts a genuinely useful exception (code, message, position) in
    the response body, and swallowing it to print something tidier would lose
    the only diagnostic there is.
    """
    response = client.post("/", content=sql.encode("utf-8"))
    if response.is_error:
        raise RuntimeError(f"HTTP {response.status_code}\n{response.text.strip()}\n\n{sql}")
    return response.text


def _exists(client: httpx.Client, table: str) -> bool:
    """True if `table` exists in the agentlake database (or, for "", the
    database itself). Materialized views appear in system.tables too.
    """
    if not table:
        sql = f"SELECT count() FROM system.databases WHERE name = '{DATABASE}'"
    else:
        sql = (
            f"SELECT count() FROM system.tables "
            f"WHERE database = '{DATABASE}' AND name = '{table}'"
        )
    return _execute(client, sql).strip() != "0"


def _drop(client: httpx.Client, table: str) -> None:
    what = f"{DATABASE}.{table}" if table else f"database {DATABASE}"
    if not _exists(client, table):
        print(f"absent   {what}")
        return
    if table:
        _execute(client, f"DROP TABLE IF EXISTS {DATABASE}.{table} SYNC")
    else:
        _execute(client, f"DROP DATABASE IF EXISTS {DATABASE} SYNC")
    print(f"dropped  {what}")


def _statement(filename: str) -> str:
    """One file, one statement. The HTTP interface executes a single statement
    per request, and one-per-file keeps that honest while making the numeric
    prefixes the apply order.
    """
    sql = (SQL_DIR / filename).read_text(encoding="utf-8")
    # Only the trailing semicolon would be a problem; there are none in the
    # files, and this asserts that rather than assuming it.
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    ).strip()
    if ";" in body:
        raise RuntimeError(f"{filename}: expected exactly one statement, found a ';'")
    return sql


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=f"ClickHouse HTTP interface (default: {DEFAULT_URL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the statements instead of sending them",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="DROP every object (and its data) before creating it -- destructive, "
        "and only for resetting a dev instance before a verification run. The "
        "Kafka consumer group keeps its committed offsets, so ingest resumes "
        "where it left off rather than replaying the topic.",
    )
    args = parser.parse_args(argv)

    statements = [(filename, _statement(filename)) for filename, _ in OBJECTS]

    if args.dry_run:
        for filename, sql in statements:
            print(f"--- {filename} ---")
            print(sql)
        return 0

    with httpx.Client(base_url=args.url.rstrip("/"), timeout=30.0) as client:
        if args.recreate:
            # Reverse order: views (which stops consumption), then the queue,
            # then the tables. Dropping the database last is redundant but
            # makes the reset total.
            for _, table in reversed(OBJECTS):
                _drop(client, table)

        for (_, table), (_, sql) in zip(OBJECTS, statements, strict=True):
            what = f"{DATABASE}.{table}" if table else f"database {DATABASE}"
            already = _exists(client, table)
            _execute(client, sql)
            print(f"{'exists  ' if already else 'created '} {what}")

    print(
        f"\nclickhouse {args.url} ready: {DATABASE}.trace_events_rt (+ _dlq), "
        f"consuming traces.events.v1 as group 'clickhouse-hotpath'"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
