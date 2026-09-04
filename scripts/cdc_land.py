#!/usr/bin/env python3
"""Land the Debezium changelog for prompt_versions into Iceberg.

Run from the repo root. `create-table` and `land` both need Trino up; `land`
also needs Kafka (but NOT Postgres or Connect -- the topic is the source)::

    python scripts/cdc_land.py create-table
    python scripts/cdc_land.py land
    python scripts/cdc_land.py seed          # synthetic changelog, for CI

Why a batch pull and not a Flink SQL job (ADR-007 #3)
-----------------------------------------------------
Two reasons, neither of them taste. **Mechanically**, a Debezium changelog is an
*updating* stream and ADR-004's Iceberg sink is append-only -- landing it through
Flink means Iceberg upsert mode with v2 equality deletes, a mechanism nothing
else in this repo uses, to maintain a table of tens of rows. **And the box**: the
`streaming` profile measures 1,790 MiB, `cdc` adds ~560, and dbt needs Trino for
another 853. That is over the 3.9 GB cap before anything runs. This path needs
kafka + `cdc` + `analytics` and never starts Flink at all.

It is also the honest shape for the data. Prompt versions change on a human
timescale, not per span, so a streaming job holding one of two task slots
forever to move a handful of rows a week is paying a streaming price for a batch
question. The trade, stated rather than hidden: freshness is "whenever you run
`make cdc-land`", not 30 seconds.

At-least-once, and why that is safe here
----------------------------------------
Offsets are committed to the consumer group **after** the Trino INSERT returns.
A crash in between replays the batch, so the changelog can hold the same
(partition, offset) twice. That is deliberate: the other order -- commit, then
insert -- is at-most-once, i.e. silent data loss, which no amount of dedup can
repair.

Duplicates are harmless by construction. `stg_prompt_versions` takes the latest
record per primary key, and two rows from one offset are byte-identical in every
payload column, so which one the window function picks is not observable. The
one column that differs is `ingest_ts`, and it is the last tiebreak precisely so
that the choice is deterministic without being meaningful.

What is dropped, counted rather than silently
---------------------------------------------
Three record kinds carry no row image and cannot become a row:

- **tombstones** (`value is None`). Debezium emits one after every delete, for
  Kafka log compaction. It carries nothing the preceding ``op='d'`` record does
  not -- and that record carries the full `before` image, because
  ``prompt_versions`` runs ``REPLICA IDENTITY FULL``. So the delete propagates;
  the tombstone is a duplicate of it.
- **``op='t'``** -- TRUNCATE. Note this is why a tombstone is NOT landed as
  ``op='t'``: Debezium already uses that letter.
- **``op='m'``** -- a logical decoding message.

Each is counted and printed. "Never silently dropped" is the requirement, and a
number on stdout is what makes it checkable rather than asserted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import httpx  # noqa: E402

from analytics.trino_client import TrinoClient  # noqa: E402
from stream.flink.create_tables import (  # noqa: E402
    WRITE_PROPERTIES,
    create_if_absent,
    create_table_request,
)

DEFAULT_CATALOG_URI = os.environ.get("AGENTLAKE_ICEBERG_REST", "http://localhost:8181")
DEFAULT_KAFKA = os.environ.get("AGENTLAKE_KAFKA", "localhost:9092")

#: The Debezium topic this lands. One table, deliberately: ADR-007 lands
#: prompt_versions because that is the one a mart joins to. The connector
#: captures all four metadata tables, so the other three are already on Kafka
#: and landing them is a copy of this function, not a design.
TOPIC = "cdc.metadata.prompt_versions"

#: Its own namespace, not lake.raw. ADR-006 #1's rule is one writer per table,
#: and its ownership table reads "lake.raw -- written by Flink". A third
#: namespace keeps that sentence true rather than adding a footnote to it:
#: lake.cdc is written by this script and by nothing else.
NAMESPACE = "cdc"
TABLE_NAME = "prompt_versions"
DEFAULT_TABLE = f"lake.{NAMESPACE}.{TABLE_NAME}"

#: Where the resume point lives. Offsets in the broker, not in a file: a
#: consumer group is the mechanism Kafka already has for "where did I get to",
#: and unlike the Flink jobs' retained checkpoints (ADR-004 #11) it needs no
#: guard against a replay, because a replay here is idempotent.
CONSUMER_GROUP = "agentlake-cdc-land"

#: Operations that carry a row image and become rows.
ROW_OPS = frozenset({"c", "u", "d", "r"})

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
#
# The changelog verbatim: one Iceberg row per Kafka record, NOT one row per
# prompt version. State is derived downstream by
# dbt/models/staging/stg_prompt_versions.sql. Landing the log rather than the
# state is what makes the resolution auditable, what makes an at-least-once
# puller safe, and what lets ADR-007 #4 show an UPDATE and a DELETE as two more
# rows rather than as a mutation nobody can see afterwards.
#
# Ids 1-9 are the Debezium envelope plus this script's own provenance; 10-15 are
# the row image; 16 is the whole envelope as text.

CDC_SCHEMA: dict[str, Any] = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        # --- envelope: what makes the resolution decidable and totally ordered
        {"id": 1, "name": "op", "required": True, "type": "string"},
        # Postgres' own total order over WAL records -- the only key that orders
        # a change the way the DATABASE committed it, so it survives a topic
        # recreate or an offset reset, both of which renumber kafka_offset.
        {"id": 2, "name": "source_lsn", "required": False, "type": "long"},
        {"id": 3, "name": "source_tx_id", "required": False, "type": "long"},
        # A STRING, not a boolean: Debezium emits "true"/"first"/"last"/
        # "incremental" and omits the field entirely on streaming records.
        {"id": 4, "name": "source_snapshot", "required": False, "type": "string"},
        # When the database committed it, vs when Debezium processed it. Both,
        # because the gap between them IS the connector's lag, and ADR-007 #5
        # measures exactly that across a restart.
        {"id": 5, "name": "source_ts", "required": False, "type": "timestamp"},
        {"id": 6, "name": "event_ts", "required": False, "type": "timestamp"},
        # --- this script's provenance: identifies a replayed batch
        {"id": 7, "name": "ingest_ts", "required": True, "type": "timestamp"},
        {"id": 8, "name": "kafka_partition", "required": True, "type": "int"},
        {"id": 9, "name": "kafka_offset", "required": True, "type": "long"},
        # --- row image: `after` for c/u/r, `before` for d
        {"id": 10, "name": "id", "required": True, "type": "long"},
        {"id": 11, "name": "name", "required": False, "type": "string"},
        {"id": 12, "name": "version", "required": False, "type": "string"},
        {"id": 13, "name": "template_text", "required": False, "type": "string"},
        # jsonb arrives as a JSON *string*. Landed as text; json_extract_scalar
        # reads it in Trino if anything ever needs to.
        {"id": 14, "name": "params_json", "required": False, "type": "string"},
        {"id": 15, "name": "created_at", "required": False, "type": "timestamp"},
        # --- verbatim
        # Iceberg outlives Kafka's retention, so anything not landed here is
        # gone permanently. A column added to prompt_versions six months from
        # now can be back-filled out of this instead of forcing a re-snapshot.
        {"id": 16, "name": "envelope_json", "required": False, "type": "string"},
    ],
}

#: No partition spec, and that is a decision rather than an omission. The only
#: consumer is a latest-record resolution, which is a full scan by definition --
#: ROW_NUMBER() OVER (PARTITION BY id) cannot prune by day. Partitioning would
#: buy zero pruning and cost one small file per partition per commit, on a table
#: whose entire lifetime content is kilobytes. Iceberg supports partition
#: evolution, so day(ingest_ts) can be added later without rewriting a file.
CDC_PARTITION_SPEC: dict[str, Any] = {"spec-id": 0, "fields": []}


# ---------------------------------------------------------------------------
# Envelope -> row
# ---------------------------------------------------------------------------


class SkippedRecord(Exception):
    """Raised for a record that carries no row image. Carries its reason so the
    caller can count categories rather than print one opaque total."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _sql_str(value: str) -> str:
    """Single-quote a SQL string literal, doubling embedded quotes.

    Unlike scripts/seed_iceberg.py's identical helper, the values reaching this
    one are NOT generated by this script: template_text and params_json come
    out of an application table, so this is load-bearing rather than belt and
    braces. Trino string literals have no backslash escape, so doubling the
    quote is both sufficient and complete.
    """
    return "'" + value.replace("'", "''") + "'"


def _sql_opt_str(value: object) -> str:
    if value is None:
        return "CAST(NULL AS VARCHAR)"
    return _sql_str(value if isinstance(value, str) else json.dumps(value))


def _sql_opt_long(value: object) -> str:
    if value is None:
        return "CAST(NULL AS BIGINT)"
    return str(int(value))


def _sql_ts(dt: datetime | None) -> str:
    if dt is None:
        return "CAST(NULL AS TIMESTAMP)"
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"


def _epoch_ms_to_dt(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000.0, UTC).replace(tzinfo=None)


def parse_timestamp(value: object) -> datetime | None:
    """Debezium's rendering of a temporal column, in either of its two shapes.

    ``time.precision.mode=connect`` makes every *non*-zoned temporal type a
    millisecond integer. It does NOT cover ``timestamptz``: Postgres' zoned type
    maps to Debezium's ``ZonedTimestamp`` semantic type, which is always an
    ISO-8601 string regardless of the precision mode. ``created_at`` is a
    ``timestamptz``, so it arrives as ``"2026-09-04T16:07:56.402020Z"`` -- while
    a plain ``timestamp`` column added to this table later would arrive as
    ``1788538076402``.

    Both are handled, because guessing wrong is not an error -- it is a column
    of nonsense timestamps that nothing raises about. Everything is normalised
    to naive UTC, which is what Iceberg's ``timestamp`` (without zone) holds and
    what ``stream/flink/create_tables.py`` already stores ``ts_epoch_ms`` as.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return _epoch_ms_to_dt(value)
    if isinstance(value, str):
        # fromisoformat handles the trailing Z from Python 3.11 on.
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed
    raise ValueError(f"unrecognised timestamp rendering: {value!r}")


def envelope_to_values(
    envelope: dict[str, Any] | None,
    key: dict[str, Any] | None,
    partition: int,
    offset: int,
    ingest_ts: datetime,
) -> dict[str, Any]:
    """One Debezium record -> the typed values of one Iceberg row.

    Pure, and separated from both Kafka and Trino so tests can exercise every
    envelope shape -- snapshot, insert, update, delete, tombstone, truncate --
    without a broker or a warehouse (tests/test_cdc.py).

    Raises SkippedRecord for anything that carries no row image.
    """
    if envelope is None:
        # A tombstone. The key still identifies the row, which is what makes
        # "counted, not silently dropped" reportable rather than a claim.
        deleted_id = (key or {}).get("id")
        raise SkippedRecord(f"tombstone (id={deleted_id})")

    op = envelope.get("op")
    if op not in ROW_OPS:
        raise SkippedRecord(f"op={op!r}")

    # `after` for create/update/read, `before` for delete. The delete's before
    # image is complete only because prompt_versions runs REPLICA IDENTITY FULL
    # -- under the default identity every column but the key would be NULL here,
    # and a deleted row would land with no `version` to resolve against.
    image = envelope.get("after") if op != "d" else envelope.get("before")
    if not isinstance(image, dict) or image.get("id") is None:
        raise SkippedRecord(f"op={op!r} with no usable row image")

    source = envelope.get("source") or {}
    return {
        "op": op,
        "source_lsn": source.get("lsn"),
        "source_tx_id": source.get("txId"),
        "source_snapshot": source.get("snapshot"),
        "source_ts": _epoch_ms_to_dt(source.get("ts_ms")),
        "event_ts": _epoch_ms_to_dt(envelope.get("ts_ms")),
        "ingest_ts": ingest_ts,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "id": int(image["id"]),
        "name": image.get("name"),
        "version": image.get("version"),
        "template_text": image.get("template_text"),
        "params_json": image.get("params_json"),
        "created_at": parse_timestamp(image.get("created_at")),
        "envelope_json": json.dumps(envelope, separators=(",", ":"), sort_keys=True),
    }


def values_to_sql(values: dict[str, Any]) -> str:
    """One VALUES tuple, in CDC_SCHEMA's field order."""
    return (
        "("
        f"{_sql_str(values['op'])}, "
        f"{_sql_opt_long(values['source_lsn'])}, "
        f"{_sql_opt_long(values['source_tx_id'])}, "
        f"{_sql_opt_str(values['source_snapshot'])}, "
        f"{_sql_ts(values['source_ts'])}, "
        f"{_sql_ts(values['event_ts'])}, "
        f"{_sql_ts(values['ingest_ts'])}, "
        f"{int(values['kafka_partition'])}, "
        f"{int(values['kafka_offset'])}, "
        f"{int(values['id'])}, "
        f"{_sql_opt_str(values['name'])}, "
        f"{_sql_opt_str(values['version'])}, "
        f"{_sql_opt_str(values['template_text'])}, "
        f"{_sql_opt_str(values['params_json'])}, "
        f"{_sql_ts(values['created_at'])}, "
        f"{_sql_opt_str(values['envelope_json'])}"
        ")"
    )


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def cmd_create_table(args: argparse.Namespace) -> int:
    """Create lake.cdc.prompt_versions through the Iceberg REST catalog.

    Not in stream/flink/create_tables.py's TABLES list, even though it reuses
    that module's request builder: that script is documented as creating the
    tables the FLINK JOBS write into, and this is not one. Same catalog, same
    two POSTs, different owner.
    """
    request = create_table_request(TABLE_NAME, CDC_SCHEMA, CDC_PARTITION_SPEC)
    if args.dry_run:
        print(json.dumps(request, indent=2))
        return 0

    with httpx.Client(base_url=args.uri.rstrip("/"), timeout=30.0) as client:
        if args.recreate:
            dropped = client.delete(
                f"/v1/namespaces/{NAMESPACE}/tables/{TABLE_NAME}",
                params={"purgeRequested": "true"},
            )
            if dropped.status_code == httpx.codes.NOT_FOUND:
                print(f"absent   {DEFAULT_TABLE}")
            elif dropped.is_error:
                raise RuntimeError(
                    f"drop {DEFAULT_TABLE}: HTTP {dropped.status_code}\n{dropped.text}"
                )
            else:
                print(f"dropped  {DEFAULT_TABLE}")
        create_if_absent(
            client, "/v1/namespaces", {"namespace": [NAMESPACE], "properties": {}},
            f"namespace {NAMESPACE}",
        )
        create_if_absent(
            client, f"/v1/namespaces/{NAMESPACE}/tables", request, f"table {DEFAULT_TABLE}"
        )

    print(f"\ncatalog {args.uri} ready: {DEFAULT_TABLE}")
    print(f"write properties: {', '.join(sorted(WRITE_PROPERTIES))}")
    return 0


def _committed_offsets(consumer: Any, partitions: list[Any]) -> dict[int, int]:
    """The group's committed offset per partition, -1 where it has none."""
    from confluent_kafka import OFFSET_INVALID

    committed = consumer.committed(partitions, timeout=10.0)
    return {
        tp.partition: (-1 if tp.offset in (OFFSET_INVALID, None) or tp.offset < 0 else tp.offset)
        for tp in committed
    }


def cmd_land(args: argparse.Namespace) -> int:
    """Drain the topic into Iceberg, then commit offsets."""
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    client = TrinoClient()
    existing = client.execute(f"SELECT count(*) FROM {args.table}").scalar()

    consumer = Consumer(
        {
            "bootstrap.servers": args.kafka,
            "group.id": CONSUMER_GROUP,
            # Where a group with NO committed offset starts. The whole changelog
            # is the point -- starting at 'latest' would silently skip the
            # initial snapshot, which is the only record of rows that existed
            # before the lander did.
            "auto.offset.reset": "earliest",
            # Offsets are committed explicitly, after the insert. See the module
            # docstring: the other order is silent data loss.
            "enable.auto.commit": False,
        }
    )

    try:
        metadata = consumer.list_topics(args.topic, timeout=10.0)
        if args.topic not in metadata.topics or metadata.topics[args.topic].error:
            print(
                f"REFUSED: topic {args.topic} does not exist on {args.kafka}.\n\n"
                "Debezium creates it on the first record it captures, so an absent\n"
                "topic means the connector has not run. Start the cdc profile and\n"
                "register it:\n\n"
                "    make cdc-up && make cdc-connector\n"
            )
            return 1
        parts = [
            TopicPartition(args.topic, p) for p in metadata.topics[args.topic].partitions
        ]
        committed = _committed_offsets(consumer, parts)

        # The guard. The Iceberg table and the consumer group are two
        # independent pieces of state and nothing ties them together, so
        # "table empty, offsets committed" means someone recreated the table
        # while the group remembers having already read the changelog -- and
        # landing from here would silently produce a truncated history that
        # every count downstream would then treat as complete. Same instinct as
        # submit.sh's resume guard (ADR-004 #11): make the safe path default and
        # the destructive one explicit.
        if not existing and any(offset > 0 for offset in committed.values()):
            print(
                f"REFUSED: {args.table} is empty but the consumer group\n"
                f"'{CONSUMER_GROUP}' has already committed offsets "
                f"{dict(sorted(committed.items()))}.\n\n"
                "That means the table was recreated while the group kept its place, so\n"
                "landing now would skip every record before those offsets and leave a\n"
                "silently truncated changelog. Reset the group to re-read it all:\n\n"
                "    docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \\\n"
                f"      --bootstrap-server kafka:19092 --group {CONSUMER_GROUP} \\\n"
                f"      --topic {args.topic} --reset-offsets --to-earliest --execute\n\n"
                "or re-run with --force to accept the gap.\n"
            )
            return 1

        consumer.assign(parts)
        ingest_ts = datetime.now(UTC).replace(tzinfo=None)

        rows: list[str] = []
        by_op: dict[str, int] = {}
        skipped: dict[str, int] = {}
        last_offsets: dict[int, int] = {}

        while True:
            message = consumer.poll(timeout=args.poll_timeout)
            if message is None:
                break  # the topic is drained
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"kafka: {message.error()}")

            raw_value = message.value()
            raw_key = message.key()
            envelope = json.loads(raw_value) if raw_value else None
            key = json.loads(raw_key) if raw_key else None
            last_offsets[message.partition()] = message.offset()

            try:
                values = envelope_to_values(
                    envelope, key, message.partition(), message.offset(), ingest_ts
                )
            except SkippedRecord as skip:
                # Counted by category, never silently. A tombstone is a
                # duplicate of the op='d' record before it; a truncate or a
                # logical-decoding message has no row image at all.
                bucket = skip.reason.split(" (")[0]
                skipped[bucket] = skipped.get(bucket, 0) + 1
                continue

            rows.append(values_to_sql(values))
            by_op[values["op"]] = by_op.get(values["op"], 0) + 1

            if len(rows) >= args.batch:
                client.execute(f"INSERT INTO {args.table} VALUES\n" + ",\n".join(rows))
                print(f"  inserted {len(rows)}")
                rows = []
                consumer.commit(asynchronous=False)

        if rows:
            client.execute(f"INSERT INTO {args.table} VALUES\n" + ",\n".join(rows))
            print(f"  inserted {len(rows)}")

        # Commit last, and only if nothing raised: everything read so far is
        # durably in Iceberg by this point.
        if by_op or skipped:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    total_rows = sum(by_op.values())
    total_skipped = sum(skipped.values())
    print(f"\nlanded {total_rows} row(s) into {args.table}")
    for op in sorted(by_op):
        print(f"  op={op}  {by_op[op]}")
    if skipped:
        print(f"skipped {total_skipped} record(s), by kind:")
        for reason in sorted(skipped):
            print(f"  {reason}  {skipped[reason]}")
    else:
        print("skipped 0 records")
    if last_offsets:
        print(f"committed through offset {dict(sorted(last_offsets.items()))}")

    after = client.execute(f"SELECT count(*) FROM {args.table}").scalar()
    print(f"{args.table} now holds {after} row(s) (was {existing})")
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    """Synthetic changelog rows, for a warehouse with no Kafka in front of it.

    CI has no broker and no Postgres (ADR-006 #10 is why the analytics job
    fits), so without this the CDC source table is empty, stg_prompt_versions
    has no rows, and fct_cost_by_prompt's join -- the entire point of the mart
    -- is never executed even though every test passes. Exactly the role
    scripts/seed_iceberg.py plays for the trace facts, and fenced the same way.

    The shapes are the ones the real connector produces: three snapshot reads,
    one update, one delete-with-before-image.
    """
    client = TrinoClient()
    existing = client.execute(f"SELECT count(*) FROM {args.table}").scalar()
    if existing and not args.force:
        print(
            f"REFUSED: {args.table} already holds {existing} rows.\n\n"
            "These are synthetic changelog records. Mixed into rows Debezium\n"
            "produced they cannot be told apart afterwards, and the latest-record\n"
            "resolution downstream would silently prefer whichever has the higher\n"
            "offset. Re-run with --force, or reset the table:\n\n"
            "    python scripts/cdc_land.py create-table --recreate\n"
        )
        return 1

    base = datetime(2026, 9, 1, 12, 0, 0)
    ingest_ts = datetime.now(UTC).replace(tzinfo=None)
    seeds = [
        # Three snapshot reads, sharing one LSN exactly as a real initial
        # snapshot does -- which is what makes the op-rank tiebreak in
        # stg_prompt_versions worth having.
        ("r", 1, "v1", "You are a helpful assistant.", 1000, "first", 0),
        ("r", 2, "v2", "You are a helpful assistant with tools.", 1000, "true", 1),
        ("r", 3, "v3", "You are agentlake's documentation assistant.", 1000, "last", 2),
        # An update to v2, streaming (no snapshot marker, higher LSN).
        ("u", 2, "v2", "You are a helpful assistant with tools. Cite sources.", 2000, None, 3),
        # A delete of v1, carrying its full before image -- which is what
        # REPLICA IDENTITY FULL buys and what the resolution needs to say WHICH
        # version was retired.
        ("d", 1, "v1", "You are a helpful assistant.", 3000, None, 4),
    ]

    rows = []
    for op, row_id, version, template, lsn, snapshot, offset in seeds:
        rows.append(
            values_to_sql(
                {
                    "op": op,
                    "source_lsn": lsn,
                    "source_tx_id": 700 + offset,
                    "source_snapshot": snapshot,
                    "source_ts": base,
                    "event_ts": base,
                    "ingest_ts": ingest_ts,
                    "kafka_partition": 0,
                    "kafka_offset": offset,
                    "id": row_id,
                    "name": "agent-system",
                    "version": version,
                    "template_text": template,
                    "params_json": '{"style": "cited"}',
                    "created_at": base,
                    "envelope_json": None,
                }
            )
        )

    client.execute(f"INSERT INTO {args.table} VALUES\n" + ",\n".join(rows))
    total = client.execute(f"SELECT count(*) FROM {args.table}").scalar()
    print(f"seeded {len(rows)} changelog record(s); {args.table} now holds {total}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--table", default=DEFAULT_TABLE, help=f"target table (default: {DEFAULT_TABLE})"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-table", help="create lake.cdc.prompt_versions")
    create.add_argument("--uri", default=DEFAULT_CATALOG_URI)
    create.add_argument("--dry-run", action="store_true")
    create.add_argument(
        "--recreate",
        action="store_true",
        help="DROP the table (purging its data files) first. Destructive, and "
        "worse here than for the Flink tables: the changelog cannot be "
        "regenerated by re-running traffic. Getting it back means deleting "
        "the connector and re-snapshotting.",
    )
    create.set_defaults(func=cmd_create_table)

    land = sub.add_parser("land", help="drain the CDC topic into Iceberg")
    land.add_argument("--kafka", default=DEFAULT_KAFKA)
    land.add_argument("--topic", default=TOPIC)
    land.add_argument("--batch", type=int, default=200)
    land.add_argument(
        "--poll-timeout",
        type=float,
        default=5.0,
        help="seconds to wait for a record before calling the topic drained "
        "(default: 5). This runs to completion and exits -- there is no "
        "--follow, deliberately: a long-running lander committing to the "
        "SQLite-backed catalog while dbt drops and creates seven tables is "
        "exactly the race ADR-006 #8 spent an afternoon on.",
    )
    land.add_argument("--force", action="store_true", help="ignore the empty-table guard")
    land.set_defaults(func=cmd_land)

    seed = sub.add_parser("seed", help="synthetic changelog rows (CI)")
    seed.add_argument("--force", action="store_true")
    seed.set_defaults(func=cmd_seed)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
