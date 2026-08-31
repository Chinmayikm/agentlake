"""The hot path restates contracts/trace_event_v1.avsc twice, and both are a
silent-drift risk. These tests make the drift loud.

1. ``stream/clickhouse/sql/04_trace_events_kafka.sql`` -- the Kafka engine
   table's columns. ClickHouse binds Avro fields to columns by NAME, so a
   renamed field is silently skipped and a missing one is an error at ingest
   time, not at deploy time.
2. ``stream/clickhouse/sql/02_trace_events_rt.sql`` -- the MergeTree every
   dashboard, both MCP tools and scripts/hot_path_verify.py read.

Plus the materialized view in ``05_trace_events_mv.sql``, which is the seam
between them and the one place a column may legitimately change its name
(ts_epoch_ms -> ts, ADR-005 #8).

A field added to the .avsc without touching these gets silently dropped on the
way to ClickHouse; a field whose type is wrong fails every record. Neither
announces itself at deploy time, which is why they are tested here rather than
left to the live pipeline. Same premise as tests/test_cold_path_contract.py.

No ClickHouse, no Kafka, no Docker: these read files.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"
SQL_DIR = REPO_ROOT / "stream" / "clickhouse" / "sql"

# Column body of a `CREATE TABLE ... ( ... ) ENGINE` statement.
_CREATE_TABLE_BODY = r"CREATE TABLE IF NOT EXISTS \w+\.\w+\s*\n\((.*?)\n\)\s*\nENGINE"

RT_SQL = SQL_DIR / "02_trace_events_rt.sql"
KAFKA_SQL = SQL_DIR / "04_trace_events_kafka.sql"
MV_SQL = SQL_DIR / "05_trace_events_mv.sql"

# The one column that legitimately changes name, and the boundary it changes at.
# The Kafka table must use the contract name (ClickHouse matches Avro fields by
# name); the materialized view is this path's only transformation step, so the
# rename rides along with the projection there. See ADR-005 #8.
RENAMES: dict[str, str] = {"ts_epoch_ms": "ts"}

# Avro type -> ClickHouse type, for the types this contract actually uses.
# Deliberately not a general converter: an unmapped type should fail the test
# and force a decision, not be guessed at. Mirrors AVRO_TO_ICEBERG in
# tests/test_cold_path_contract.py.
AVRO_TO_CLICKHOUSE: dict[str, str] = {
    "string": "String",
    "long": "Int64",
    "double": "Float64",
    "boolean": "Bool",
}


@pytest.fixture(scope="module")
def contract() -> dict[str, Any]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _clickhouse_type(avro_type: Any) -> str:
    """The ClickHouse column type for one Avro field type.

    Handles the three shapes this contract uses: a ["null", X] union (Nullable),
    a bare type name, and a dict (enum, map, or a logical type).
    """
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        assert len(non_null) == 1, f"unsupported union: {avro_type}"
        assert len(avro_type) == 2, f"unsupported union: {avro_type}"
        return f"Nullable({_clickhouse_type(non_null[0])})"
    if isinstance(avro_type, str):
        assert avro_type in AVRO_TO_CLICKHOUSE, f"unsupported avro type: {avro_type}"
        return AVRO_TO_CLICKHOUSE[avro_type]
    if isinstance(avro_type, dict):
        kind = avro_type.get("type")
        if kind == "enum":
            # String, not Enum8: adding a symbol to the Avro enum in a later
            # contract version must not require a ClickHouse migration.
            return "String"
        if kind == "map":
            assert avro_type["values"] == "string", f"unsupported map: {avro_type}"
            return "Map(String, String)"
        if kind == "long" and avro_type.get("logicalType") == "timestamp-millis":
            return "DateTime64(3)"
        raise AssertionError(f"unsupported avro type: {avro_type}")
    raise AssertionError(f"unsupported avro type: {avro_type}")


def _columns(path: Path) -> list[tuple[str, str]]:
    """(name, type) for each column in a file's CREATE TABLE body, in order."""
    sql = path.read_text(encoding="utf-8")
    body = re.search(_CREATE_TABLE_BODY, sql, re.DOTALL)
    assert body, f"{path.name} has no CREATE TABLE ... ENGINE block"

    columns: list[tuple[str, str]] = []
    for line in body.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        name, _, column_type = line.partition(" ")
        columns.append((name, column_type.strip()))
    return columns


def _clause(path: Path, keyword: str) -> str:
    """One trailing clause (ORDER BY / PARTITION BY / TTL) as written, comments
    and continuation lines stripped."""
    lines = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("--")
    ]
    for line in lines:
        if line.startswith(keyword):
            return line
    raise AssertionError(f"{path.name} has no {keyword} clause")


# ---------------------------------------------------------------------------
# The Kafka engine table carries the contract, verbatim
# ---------------------------------------------------------------------------


def test_kafka_table_has_every_contract_field_in_order(contract: dict[str, Any]) -> None:
    """Names must match the Avro field names exactly: ClickHouse binds by name,
    so a mismatch is silently skipped rather than rejected."""
    assert [name for name, _ in _columns(KAFKA_SQL)] == [f["name"] for f in contract["fields"]]


def test_kafka_table_types_match_the_contract(contract: dict[str, Any]) -> None:
    for avro_field, (name, column_type) in zip(
        contract["fields"], _columns(KAFKA_SQL), strict=True
    ):
        assert column_type == _clickhouse_type(avro_field["type"]), name


# ---------------------------------------------------------------------------
# The hot table carries the contract, with exactly one rename
# ---------------------------------------------------------------------------


def test_hot_table_has_every_contract_field_with_only_the_documented_rename(
    contract: dict[str, Any],
) -> None:
    expected = [RENAMES.get(f["name"], f["name"]) for f in contract["fields"]]
    assert [name for name, _ in _columns(RT_SQL)] == expected


def test_hot_table_types_match_the_contract(contract: dict[str, Any]) -> None:
    for avro_field, (name, column_type) in zip(contract["fields"], _columns(RT_SQL), strict=True):
        assert column_type == _clickhouse_type(avro_field["type"]), name


def test_the_rename_happens_only_at_the_materialized_view() -> None:
    """The rename is a projection-time change, so it must appear in the MV and
    nowhere else. See ADR-005 #8."""
    mv = MV_SQL.read_text(encoding="utf-8")
    for contract_name, hot_name in RENAMES.items():
        assert f"{contract_name} AS {hot_name}" in mv
        # The source side keeps the contract name; the target side does not.
        assert contract_name in dict(_columns(KAFKA_SQL))
        assert contract_name not in dict(_columns(RT_SQL))
        assert hot_name in dict(_columns(RT_SQL))


def test_materialized_view_selects_every_contract_field(contract: dict[str, Any]) -> None:
    """A field missing from the MV's SELECT is a column that silently stays
    NULL/zero in the hot table while ingest reports success."""
    mv = MV_SQL.read_text(encoding="utf-8")
    body = re.search(r"AS SELECT\n(.*?)\nFROM ", mv, re.DOTALL)
    assert body, "05_trace_events_mv.sql has no AS SELECT ... FROM block"

    selected: list[str] = []
    for line in body.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue
        # "ts_epoch_ms AS ts" projects to its alias; a bare column to itself.
        selected.append(line.split(" AS ")[-1].strip() if " AS " in line else line)
    assert selected == [RENAMES.get(f["name"], f["name"]) for f in contract["fields"]]


# ---------------------------------------------------------------------------
# Engine choices the dedup posture depends on (ADR-005 #2)
# ---------------------------------------------------------------------------


def test_hot_table_sorting_key_ends_in_span_id() -> None:
    """ReplacingMergeTree deduplicates on the sorting key and nothing else, so
    span_id being in it is what makes a replayed batch collapse rather than
    accumulate. Losing this line would silently turn dedup off."""
    order_by = _clause(RT_SQL, "ORDER BY")
    assert order_by.startswith("ORDER BY (event_type, model, ts, span_id)"), order_by


def test_hot_table_is_a_replacing_mergetree() -> None:
    assert "ENGINE = ReplacingMergeTree" in RT_SQL.read_text(encoding="utf-8")


def test_hot_table_partitions_and_expires_on_the_timestamp() -> None:
    """A duplicate row is byte-identical including ts, so partitioning by ts is
    what keeps every copy of a row in one partition and makes partition-local
    merging sufficient."""
    assert _clause(RT_SQL, "PARTITION BY") == "PARTITION BY toDate(ts)"
    assert _clause(RT_SQL, "TTL") == "TTL toDateTime(ts) + INTERVAL 7 DAY"


def test_hot_table_allows_a_nullable_sorting_key() -> None:
    """`model` is Nullable and is in the ORDER BY, so without this the CREATE
    TABLE is rejected outright."""
    assert "allow_nullable_key = 1" in RT_SQL.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Kafka wiring the runbook and ADR-005 claim
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("setting", "value"),
    [
        # The INTERNAL listener. 9092 is EXTERNAL and resolves to the wrong host
        # from inside a container -- the same trap stream/flink/jobs/*.sql notes.
        ("kafka_broker_list", "'kafka:19092'"),
        ("kafka_topic_list", "'traces.events.v1'"),
        ("kafka_group_name", "'clickhouse-hotpath'"),
        ("kafka_format", "'AvroConfluent'"),
        ("format_avro_schema_registry_url", "'http://schema-registry:8081'"),
        # What makes the _error/_raw_message virtual columns exist, and so what
        # the dead-letter view in 06_*.sql depends on.
        ("kafka_handle_error_mode", "'stream'"),
        # The freshness knob: NFR-2's p95 <= 5s is unreachable at the 7500ms
        # default.
        ("kafka_flush_interval_ms", "1000"),
    ],
)
def test_kafka_engine_settings(setting: str, value: str) -> None:
    assert f"{setting} = {value}" in KAFKA_SQL.read_text(encoding="utf-8")


def test_the_contract_is_not_duplicated_into_the_hot_path_sql(contract: dict[str, Any]) -> None:
    """The inverse of what tests/test_cold_path_contract.py has to check.

    ADR-004 #7 had to spell the whole .avsc into an `avro-confluent.schema`
    literal in both Flink jobs, because Flink derives a reader schema from the
    column types and Avro will not resolve an enum to a string. ClickHouse's
    AvroConfluent reader does that itself, so the contract lives in exactly one
    file on this path. This test is what keeps someone from "fixing" a future
    problem by pasting it in and quietly reintroducing the drift surface.
    """
    marker = f'"name":"{contract["name"]}"'
    for path in sorted(SQL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8").replace(" ", "")
        assert marker not in text, f"{path.name} embeds a copy of the Avro contract"


def test_there_is_sql_to_check() -> None:
    """A glob or a regex that silently matches nothing turns every test above
    into a no-op that still reports green."""
    assert sorted(p.name for p in SQL_DIR.glob("*.sql"))
    assert _columns(RT_SQL)
    assert _columns(KAFKA_SQL)
