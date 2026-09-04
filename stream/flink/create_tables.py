"""Create the Iceberg tables the Flink jobs write into.

Run once, from the repo root, with the streaming profile up::

    python -m stream.flink.create_tables

Why this exists at all -- i.e. why the DDL is not in the SQL jobs
---------------------------------------------------------------
``lake.raw.trace_events`` is meant to be partitioned by ``day(ts_epoch_ms)``,
which is Iceberg *hidden* partitioning: the partition is derived from a column
by a transform, is not a column itself, and queries prune on it without ever
naming it. Flink SQL cannot express that. From the Iceberg 1.10 Flink DDL docs,
verbatim:

    Iceberg supports hidden partitioning but Flink doesn't support partitioning
    by a function on columns. There is no way to support hidden partitions in
    the Flink DDL.

``PARTITIONED BY`` in Flink accepts identity columns only. So the tables are
created out of band, here, by POSTing an Iceberg ``CreateTableRequest`` straight
at the REST catalog -- the same catalog Flink then opens -- and the Flink jobs
only ever run ``INSERT INTO``. Flink reads the partition spec back from the
catalog and honours it when writing. See ADR-004 #2.

This is metadata, not data: no file in the warehouse is written by anything but
Flink.

Why raw HTTP and not pyiceberg
------------------------------
``httpx`` is already a dependency (services/rag/fetch.py). pyiceberg would be a
new one that declares support only through Python 3.13 -- this repo runs 3.14 --
and drags in pyarrow to do something that is one POST of a JSON document. The
Iceberg REST spec is the contract here, and it is small enough to write down.

Idempotent: an existing namespace or table (HTTP 409) counts as success, so
re-running after a partial failure is safe.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx

DEFAULT_CATALOG_URI = os.environ.get("AGENTLAKE_ICEBERG_REST", "http://localhost:8181")

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
#
# Iceberg field ids are assigned explicitly and are permanent: they, not names,
# are what Iceberg tracks a column by across renames. Ids 1-13 are the thirteen
# contract fields in .avsc order (tests/test_cold_path_contract.py asserts that
# order still matches contracts/trace_event_v1.avsc); 14/15 are the attributes map's
# key and value, which need ids of their own because nested fields share the
# table's id space. Partition field ids start at 1000 by Iceberg convention.
#
# Type mapping from the Avro contract:
#   ["null", X]           -> required: false     (an Avro union with null)
#   enum                  -> string              (Flink's Avro reader gives VARCHAR)
#   long/timestamp-millis -> timestamp           (the logical type IS a timestamp;
#                                                 the field keeps its contract name)
#   map<string,string>    -> map, value-required (Avro map values are non-null)

TS_FIELD_ID = 12  # ts_epoch_ms -- the source column for the day() partition

RAW_SCHEMA: dict[str, Any] = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "trace_id", "required": True, "type": "string"},
        {"id": 2, "name": "span_id", "required": True, "type": "string"},
        {"id": 3, "name": "parent_span_id", "required": False, "type": "string"},
        {"id": 4, "name": "session_id", "required": True, "type": "string"},
        {"id": 5, "name": "event_type", "required": True, "type": "string"},
        {"id": 6, "name": "model", "required": False, "type": "string"},
        {"id": 7, "name": "prompt_tokens", "required": False, "type": "long"},
        {"id": 8, "name": "completion_tokens", "required": False, "type": "long"},
        {"id": 9, "name": "latency_ms", "required": True, "type": "double"},
        {"id": 10, "name": "cost_usd", "required": False, "type": "double"},
        {"id": 11, "name": "status", "required": True, "type": "string"},
        {"id": TS_FIELD_ID, "name": "ts_epoch_ms", "required": True, "type": "timestamp"},
        {
            "id": 13,
            "name": "attributes",
            "required": True,
            "type": {
                "type": "map",
                "key-id": 14,
                "key": "string",
                "value-id": 15,
                "value": "string",
                "value-required": True,
            },
        },
    ],
}

RAW_PARTITION_SPEC: dict[str, Any] = {
    "spec-id": 0,
    "fields": [
        # Hidden partitioning: ts_day is derived from ts_epoch_ms, is not a
        # column, and is what Flink SQL could not have declared.
        {"source-id": TS_FIELD_ID, "field-id": 1000, "name": "ts_day", "transform": "day"},
    ],
}

# Only window_start/window_end are required. Every aggregate is optional
# because Flink types SUM() as nullable regardless of its input's nullability,
# and writing a nullable expression into a required Iceberg column is a planner
# error at submit time, not a runtime one -- there is nothing to gain by making
# the job harder to submit for a NOT NULL that nothing reads.
AGG_SCHEMA: dict[str, Any] = {
    "type": "struct",
    "schema-id": 0,
    "fields": [
        {"id": 1, "name": "window_start", "required": True, "type": "timestamp"},
        {"id": 2, "name": "window_end", "required": True, "type": "timestamp"},
        {"id": 3, "name": "event_type", "required": False, "type": "string"},
        {"id": 4, "name": "model", "required": False, "type": "string"},
        {"id": 5, "name": "event_count", "required": False, "type": "long"},
        {"id": 6, "name": "error_count", "required": False, "type": "long"},
        {"id": 7, "name": "prompt_tokens_sum", "required": False, "type": "long"},
        {"id": 8, "name": "completion_tokens_sum", "required": False, "type": "long"},
        {"id": 9, "name": "cost_usd_sum", "required": False, "type": "double"},
        {"id": 10, "name": "latency_sum_ms", "required": False, "type": "double"},
        {"id": 11, "name": "latency_max_ms", "required": False, "type": "double"},
    ],
}

AGG_PARTITION_SPEC: dict[str, Any] = {
    "spec-id": 0,
    "fields": [
        {"source-id": 1, "field-id": 1000, "name": "window_day", "transform": "day"},
    ],
}

# Sized for a 1 GB TaskManager, not for a cluster. Parquet's default 128 MB row
# group is buffered in the writer's heap before it is flushed, which on this box
# is most of the task heap for a single open partition -- 8 MB row groups and 32
# MB target files trade a little query efficiency for a writer that fits. See
# ADR-004 #8.
WRITE_PROPERTIES: dict[str, str] = {
    "write.format.default": "parquet",
    "write.parquet.row-group-size-bytes": str(8 * 1024 * 1024),
    "write.target-file-size-bytes": str(32 * 1024 * 1024),
    # Flink's Iceberg sink commits once per checkpoint (30s), so a long-running
    # job accumulates snapshots fast. Keep the metadata log bounded.
    "write.metadata.previous-versions-max": "20",
    "write.metadata.delete-after-commit.enabled": "true",
}

TABLES: list[tuple[str, str, dict[str, Any], dict[str, Any]]] = [
    ("raw", "trace_events", RAW_SCHEMA, RAW_PARTITION_SPEC),
    ("curated", "agg_model_5m", AGG_SCHEMA, AGG_PARTITION_SPEC),
]


def create_table_request(
    name: str, schema: dict[str, Any], partition_spec: dict[str, Any]
) -> dict[str, Any]:
    """The Iceberg REST ``CreateTableRequest`` body for one table."""
    return {
        "name": name,
        "schema": schema,
        "partition-spec": partition_spec,
        "write-order": None,
        "stage-create": False,
        "properties": dict(WRITE_PROPERTIES),
    }


def create_if_absent(client: httpx.Client, path: str, body: dict[str, Any], what: str) -> bool:
    """POST `body` to `path`. Returns True if it created something, False if it
    already existed. Anything else raises.

    Public, and imported by scripts/cdc_land.py, which creates
    ``lake.cdc.prompt_versions`` through the same REST catalog (ADR-007 #3).
    That table is not in ``TABLES`` below because this module is what creates
    the tables the *Flink jobs* write into, and the CDC landing table is written
    by the CDC lander -- but the two POSTs are identical, so they are written
    once.
    """
    response = client.post(path, json=body)
    if response.status_code == httpx.codes.CONFLICT:
        print(f"exists   {what}")
        return False
    if response.is_error:
        raise RuntimeError(f"{what}: HTTP {response.status_code}\n{response.text}")
    print(f"created  {what}")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--uri",
        default=DEFAULT_CATALOG_URI,
        help=f"Iceberg REST catalog base URL (default: {DEFAULT_CATALOG_URI})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the request bodies instead of sending them",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="DROP each table (purging its data files) before creating it -- "
        "destructive, and only for resetting a dev warehouse before a "
        "verification run. Stop the Flink jobs first.",
    )
    args = parser.parse_args(argv)

    if args.dry_run:
        for namespace, table, schema, spec in TABLES:
            print(f"--- {namespace}.{table} ---")
            print(json.dumps(create_table_request(table, schema, spec), indent=2))
        return 0

    with httpx.Client(base_url=args.uri.rstrip("/"), timeout=30.0) as client:
        for namespace, table, schema, spec in TABLES:
            if args.recreate:
                dropped = client.delete(
                    f"/v1/namespaces/{namespace}/tables/{table}",
                    params={"purgeRequested": "true"},
                )
                if dropped.status_code == httpx.codes.NOT_FOUND:
                    print(f"absent   {namespace}.{table}")
                elif dropped.is_error:
                    raise RuntimeError(
                        f"drop {namespace}.{table}: HTTP {dropped.status_code}\n{dropped.text}"
                    )
                else:
                    print(f"dropped  {namespace}.{table}")
            create_if_absent(client, "/v1/namespaces", {"namespace": [namespace], "properties": {}},
                 f"namespace {namespace}")
            create_if_absent(client, f"/v1/namespaces/{namespace}/tables",
                 create_table_request(table, schema, spec), f"table {namespace}.{table}")

    print(f"\ncatalog {args.uri} ready: lake.raw.trace_events, lake.curated.agg_model_5m")
    return 0


if __name__ == "__main__":
    sys.exit(main())
