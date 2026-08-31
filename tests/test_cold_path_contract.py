"""The cold path restates contracts/trace_event_v1.avsc in three places, and
every one of them is a silent-drift risk. These tests make the drift loud.

1. ``stream/flink/create_tables.py``'s RAW_SCHEMA -- the Iceberg table's columns,
   which Flink writes into positionally.
2. the ``avro-confluent.schema`` literal in each ``stream/flink/jobs/*.sql`` --
   the Avro reader schema, which has to be spelled out because Flink cannot
   derive an enum from a SQL STRING column (see the comment in the SQL).
3. the ``CREATE TEMPORARY TABLE`` column list in each job.

A field added to the .avsc without touching these gets silently dropped on the
way to Iceberg; a field reordered gets silently written into the wrong column.
Neither failure announces itself at runtime, which is exactly why they are
tested here rather than left to the live pipeline.

No Kafka, no Flink, no Docker: these read files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from stream.flink.create_tables import RAW_PARTITION_SPEC, RAW_SCHEMA, TS_FIELD_ID

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"
JOBS_DIR = REPO_ROOT / "stream" / "flink" / "jobs"

JOB_FILES = sorted(JOBS_DIR.glob("*.sql"))

# Avro type -> Iceberg primitive, for the types this contract actually uses.
# Deliberately not a general converter: an unmapped type should fail the test
# and force a decision, not be guessed at.
AVRO_TO_ICEBERG: dict[str, str] = {
    "string": "string",
    "long": "long",
    "double": "double",
    "boolean": "boolean",
}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _iceberg_type(avro_type: Any) -> tuple[str | dict[str, Any], bool]:
    """(iceberg type, required) for one Avro field type."""
    if isinstance(avro_type, list):  # union
        non_null = [t for t in avro_type if t != "null"]
        assert len(non_null) == 1, f"unsupported union: {avro_type}"
        inner, _ = _iceberg_type(non_null[0])
        return inner, False
    if isinstance(avro_type, str):
        return AVRO_TO_ICEBERG[avro_type], True
    if isinstance(avro_type, dict):
        kind = avro_type["type"]
        if kind == "enum":
            return "string", True
        if kind == "map":
            assert avro_type["values"] == "string"
            return "map", True
        if kind == "long" and avro_type.get("logicalType") == "timestamp-millis":
            return "timestamp", True
        raise AssertionError(f"unsupported avro type: {avro_type}")
    raise AssertionError(f"unsupported avro type: {avro_type!r}")


def test_raw_table_has_every_contract_field_in_order(contract: dict[str, Any]) -> None:
    """Order matters as much as membership: Flink's INSERT is positional."""
    contract_names = [f["name"] for f in contract["fields"]]
    table_names = [f["name"] for f in RAW_SCHEMA["fields"]]
    assert table_names == contract_names


def test_raw_table_types_and_nullability_match_the_contract(contract: dict[str, Any]) -> None:
    for avro_field, iceberg_field in zip(contract["fields"], RAW_SCHEMA["fields"], strict=True):
        expected_type, expected_required = _iceberg_type(avro_field["type"])
        actual_type = iceberg_field["type"]
        if isinstance(actual_type, dict):
            actual_type = actual_type["type"]
        assert actual_type == expected_type, avro_field["name"]
        assert iceberg_field["required"] is expected_required, avro_field["name"]


def test_iceberg_field_ids_are_unique_including_nested() -> None:
    """Nested fields share the table's id space, so the map's key-id/value-id
    can collide with a column id. Iceberg would accept it and later resolve the
    wrong column."""
    ids: list[int] = []
    for field in RAW_SCHEMA["fields"]:
        ids.append(field["id"])
        if isinstance(field["type"], dict) and field["type"]["type"] == "map":
            ids.extend([field["type"]["key-id"], field["type"]["value-id"]])
    assert len(ids) == len(set(ids)), f"duplicate iceberg field ids: {ids}"


def test_partition_spec_points_at_ts_epoch_ms() -> None:
    """day(ts_epoch_ms) is the whole reason the table is created outside Flink.
    source-id is an id, not a name, so a reordered schema would silently
    repartition the table by some other column."""
    ts_field = next(f for f in RAW_SCHEMA["fields"] if f["name"] == "ts_epoch_ms")
    assert ts_field["id"] == TS_FIELD_ID

    (spec_field,) = RAW_PARTITION_SPEC["fields"]
    assert spec_field["source-id"] == ts_field["id"]
    assert spec_field["transform"] == "day"


@pytest.mark.parametrize("job", JOB_FILES, ids=lambda p: p.name)
def test_job_sql_embeds_the_exact_contract(job: Path, contract: dict[str, Any]) -> None:
    """The avro-confluent reader schema must be the contract, byte for byte in
    meaning. Flink compares it against the registry's writer schema, so any
    divergence fails every record at runtime -- after deploy, not before."""
    sql = job.read_text(encoding="utf-8")
    match = re.search(r"'avro-confluent\.schema'\s*=\s*'(\{.*?\})'\s*\n", sql, re.DOTALL)
    assert match, f"{job.name} declares no avro-confluent.schema"
    assert json.loads(match.group(1)) == contract


@pytest.mark.parametrize("job", JOB_FILES, ids=lambda p: p.name)
def test_job_sql_source_columns_match_the_contract(job: Path, contract: dict[str, Any]) -> None:
    sql = job.read_text(encoding="utf-8")
    body = re.search(
        r"CREATE TEMPORARY TABLE kafka_trace_events \((.*?)\n\) WITH", sql, re.DOTALL
    )
    assert body, f"{job.name} has no kafka_trace_events DDL"

    names: list[str] = []
    for line in body.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line.startswith("WATERMARK"):
            continue
        # First token is the column name; backticks quote Flink's reserved words.
        names.append(line.split()[0].strip("`"))
    assert names == [f["name"] for f in contract["fields"]]


def test_there_is_at_least_one_job_to_check() -> None:
    """A glob that silently matches nothing turns every parametrized test above
    into a no-op that still reports green."""
    assert JOB_FILES
