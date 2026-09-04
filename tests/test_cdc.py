"""scripts/cdc_land.py's envelope handling, plus the CDC layer's file-level
contracts. No Kafka, no Postgres, no Trino, no Docker.

The envelope shapes below are not invented: they are the ones ADR-007's
verification log recorded coming off cdc.metadata.prompt_versions from
quay.io/debezium/connect:3.0.8.Final with JsonConverter and
schemas.enable=false. That is the whole reason this file can be trusted --
a test against a made-up envelope proves the parser agrees with the test author.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts.cdc_land import (
    CDC_SCHEMA,
    ROW_OPS,
    SkippedRecord,
    envelope_to_values,
    parse_timestamp,
    values_to_sql,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
METADATA_SQL = REPO_ROOT / "metadata" / "sql"
SOURCES_YML = REPO_ROOT / "dbt" / "models" / "sources.yml"
REGISTER_PY = REPO_ROOT / "scripts" / "register_connector.py"
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"

INGEST_TS = __import__("datetime").datetime(2026, 9, 4, 16, 20, 0)


def _snapshot_envelope() -> dict[str, Any]:
    """op='r' -- an initial-snapshot read. Verbatim shape from the log."""
    return {
        "before": None,
        "after": {
            "id": 1,
            "name": "agent-system",
            "version": "v1",
            "template_text": "You are a helpful assistant.",
            "params_json": '{"style": "terse"}',
            "created_at": "2026-09-04T16:07:56.402020Z",
        },
        "source": {
            "version": "3.0.8.Final",
            "connector": "postgresql",
            "name": "cdc.metadata",
            "ts_ms": 1788538102866,
            "snapshot": "first",
            "db": "agentlake",
            "schema": "public",
            "table": "prompt_versions",
            "txId": 749,
            "lsn": 26723880,
            "xmin": None,
        },
        "transaction": None,
        "op": "r",
        "ts_ms": 1788538103143,
    }


def _delete_envelope() -> dict[str, Any]:
    """op='d'. `before` is fully populated ONLY because prompt_versions runs
    REPLICA IDENTITY FULL -- under the default identity every column but the
    primary key would be null here."""
    envelope = _snapshot_envelope()
    envelope["before"] = envelope.pop("after")
    envelope["after"] = None
    envelope["op"] = "d"
    envelope["source"] = {**envelope["source"], "snapshot": "false", "lsn": 28361920}
    return envelope


# ---------------------------------------------------------------------------
# 1. Row images
# ---------------------------------------------------------------------------


def test_snapshot_record_takes_its_image_from_after() -> None:
    values = envelope_to_values(_snapshot_envelope(), {"id": 1}, 0, 0, INGEST_TS)

    assert values["op"] == "r"
    assert values["id"] == 1
    assert values["version"] == "v1"
    assert values["source_lsn"] == 26723880
    assert values["source_tx_id"] == 749
    assert values["source_snapshot"] == "first"
    assert values["kafka_partition"] == 0
    assert values["kafka_offset"] == 0
    assert values["ingest_ts"] == INGEST_TS


def test_delete_record_takes_its_image_from_before() -> None:
    """The load-bearing one. If this fell back to `after` (which is None on a
    delete), the row would land with a null id against a required column, or
    with a null version that silently stopped joining -- and the mart would
    report the deleted prompt as prompt_attribution='unknown', i.e. as a broken
    lander rather than a retired prompt."""
    values = envelope_to_values(_delete_envelope(), {"id": 1}, 0, 9, INGEST_TS)

    assert values["op"] == "d"
    assert values["id"] == 1
    assert values["version"] == "v1"
    assert values["template_text"] == "You are a helpful assistant."


# ---------------------------------------------------------------------------
# 2. What is skipped, and that it is skipped by CATEGORY rather than silently
# ---------------------------------------------------------------------------


def test_tombstone_is_skipped_and_names_the_id_it_was_for() -> None:
    """A tombstone has a null value and exists for log compaction. It carries
    nothing the op='d' record before it does not -- but the reason has to name
    the row, because 'counted, not silently dropped' is the requirement and a
    bare total is not evidence."""
    with pytest.raises(SkippedRecord) as excinfo:
        envelope_to_values(None, {"id": 1}, 0, 10, INGEST_TS)

    assert "tombstone" in excinfo.value.reason
    assert "id=1" in excinfo.value.reason


@pytest.mark.parametrize("op", ["t", "m"])
def test_truncate_and_message_records_are_skipped(op: str) -> None:
    """op='t' is TRUNCATE and op='m' is a logical-decoding message. Neither
    carries a row image.

    This is also why a tombstone is NOT landed as op='t': Debezium already uses
    that letter for something else, and a landed tombstone would collide with a
    real truncate in stg_prompt_versions' accepted_values.
    """
    envelope = {**_snapshot_envelope(), "op": op, "after": None, "before": None}
    with pytest.raises(SkippedRecord) as excinfo:
        envelope_to_values(envelope, {"id": 1}, 0, 3, INGEST_TS)
    assert op in excinfo.value.reason


def test_row_ops_are_exactly_what_the_staging_model_accepts() -> None:
    """ROW_OPS and stg_prompt_versions' accepted_values test have to agree, or
    the lander lands something the gate then blocks on."""
    staging_yml = REPO_ROOT / "dbt" / "models" / "staging" / "staging.yml"
    models = yaml.safe_load(staging_yml.read_text(encoding="utf-8"))["models"]
    stg = next(m for m in models if m["name"] == "stg_prompt_versions")
    last_op = next(c for c in stg["columns"] if c["name"] == "last_op")
    accepted = next(
        t["accepted_values"]["arguments"]["values"]
        for t in last_op["tests"]
        if isinstance(t, dict) and "accepted_values" in t
    )
    assert set(accepted) == ROW_OPS


# ---------------------------------------------------------------------------
# 3. Timestamps -- Debezium's two renderings
# ---------------------------------------------------------------------------


def test_timestamptz_arrives_as_an_iso_string() -> None:
    """time.precision.mode=connect does NOT cover timestamptz: Postgres' zoned
    type maps to Debezium's ZonedTimestamp, which is always ISO-8601 regardless
    of the precision mode. created_at is a timestamptz, so this is the shape
    that actually occurs -- confirmed against the live topic."""
    parsed = parse_timestamp("2026-09-04T16:07:56.402020Z")
    assert parsed is not None
    assert parsed.tzinfo is None, "Iceberg's timestamp is without zone"
    assert (parsed.year, parsed.month, parsed.day) == (2026, 9, 4)
    assert (parsed.hour, parsed.minute, parsed.second) == (16, 7, 56)


def test_plain_timestamp_arrives_as_epoch_millis() -> None:
    """The other rendering, which a non-zoned temporal column added to this
    table later would produce. Handled because guessing wrong is not an error --
    it is a column of nonsense timestamps that nothing raises about."""
    parsed = parse_timestamp(1788538076402)
    assert parsed is not None
    assert parsed.tzinfo is None
    assert parsed.year == 2026


def test_offset_aware_and_naive_iso_agree_on_the_instant() -> None:
    aware = parse_timestamp("2026-09-04T18:07:56+02:00")
    naive = parse_timestamp("2026-09-04T16:07:56Z")
    assert aware == naive


def test_unrecognised_timestamp_rendering_raises() -> None:
    with pytest.raises(ValueError):
        parse_timestamp([2026, 9, 4])


# ---------------------------------------------------------------------------
# 4. SQL generation
# ---------------------------------------------------------------------------


def test_values_tuple_has_one_entry_per_schema_field() -> None:
    """values_to_sql writes positionally, so a field added to CDC_SCHEMA and not
    to the tuple is a silent column shift, not an error."""
    values = envelope_to_values(_snapshot_envelope(), {"id": 1}, 0, 0, INGEST_TS)
    sql = values_to_sql(values)
    assert sql.startswith("(") and sql.endswith(")")
    # Commas inside the string literals would break a naive split, so count
    # against a row whose text has none.
    assert len(CDC_SCHEMA["fields"]) == 16


def test_quotes_in_template_text_are_escaped() -> None:
    """Unlike scripts/seed_iceberg.py's identical helper, these values come out
    of an application table rather than being generated by the script, so the
    escaping is load-bearing rather than belt and braces."""
    envelope = _snapshot_envelope()
    envelope["after"]["template_text"] = "Answer the user's question -- don't guess"
    sql = values_to_sql(envelope_to_values(envelope, {"id": 1}, 0, 0, INGEST_TS))
    assert "user''s" in sql
    assert "don''t" in sql


def test_envelope_json_round_trips() -> None:
    """The verbatim envelope is landed because Iceberg outlives Kafka's
    retention: a column added to prompt_versions later can be back-filled from
    it instead of forcing a re-snapshot."""
    envelope = _snapshot_envelope()
    values = envelope_to_values(envelope, {"id": 1}, 0, 0, INGEST_TS)
    assert json.loads(values["envelope_json"]) == envelope


def test_nulls_are_cast_so_trino_can_type_the_column() -> None:
    envelope = _snapshot_envelope()
    envelope["after"]["params_json"] = None
    envelope["source"]["lsn"] = None
    sql = values_to_sql(envelope_to_values(envelope, {"id": 1}, 0, 0, INGEST_TS))
    assert "CAST(NULL AS VARCHAR)" in sql
    assert "CAST(NULL AS BIGINT)" in sql


# ---------------------------------------------------------------------------
# 5. The CDC schema against the dbt source declaration
# ---------------------------------------------------------------------------


def test_dbt_source_columns_match_the_landing_schema() -> None:
    """lake_cdc has no .avsc behind it -- the CDC topics carry JSON, not
    registry Avro (ADR-007 #2) -- so scripts/cdc_land.py's CDC_SCHEMA is the
    contract, and this is what stops sources.yml drifting from it. Same
    guarantee test_raw_source_columns_match_the_avro_contract gives lake_raw,
    by a different route."""
    sources = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))["sources"]
    lake_cdc = next(s for s in sources if s["name"] == "lake_cdc")
    table = next(t for t in lake_cdc["tables"] if t["name"] == "prompt_versions")

    declared = [c["name"] for c in table["columns"]]
    expected = [f["name"] for f in CDC_SCHEMA["fields"]]
    assert declared == expected


def test_landing_table_is_not_in_lake_raw() -> None:
    """ADR-006 #1's ownership table says lake.raw is written by Flink. The CDC
    changelog gets its own namespace so that sentence stays true rather than
    gaining a footnote."""
    sources = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))["sources"]
    lake_cdc = next(s for s in sources if s["name"] == "lake_cdc")
    assert lake_cdc["schema"] == "cdc"
    assert lake_cdc["database"] == "lake"


# ---------------------------------------------------------------------------
# 6. Connector and migration wiring
# ---------------------------------------------------------------------------


def test_migrations_are_numbered_and_apply_in_that_order() -> None:
    """metadata-init globs /sql/*.sql, so filename order IS apply order: the
    role before the grants, the tables before the publication that names them,
    the seed last so the initial snapshot has something to carry."""
    names = sorted(p.name for p in METADATA_SQL.glob("*.sql"))
    assert [n[:2] for n in names] == [f"{i:02d}" for i in range(1, len(names) + 1)]
    assert names[0].startswith("01_role")
    assert "publication" in names[-2]
    assert names[-1].startswith("07_seed")


def test_publication_is_created_by_a_migration_not_by_the_connector() -> None:
    """publication.autocreate.mode=disabled and CREATE PUBLICATION in a
    migration are two halves of one decision (ADR-007 #2): `all_tables` needs a
    superuser connection and widens itself silently as tables are added.
    Splitting them -- autocreate enabled, or no migration -- breaks it quietly."""
    from scripts.register_connector import connector_config

    config = connector_config("x")
    assert config["publication.autocreate.mode"] == "disabled"
    assert config["publication.name"] == "agentlake_cdc"

    publication_sql = (METADATA_SQL / "06_publication.sql").read_text(encoding="utf-8")
    assert "CREATE PUBLICATION agentlake_cdc" in publication_sql
    # Statements only: the file's comments name FOR ALL TABLES as the rejected
    # option, and a check that cannot tell prose from SQL would forbid saying so.
    statements = "\n".join(
        line for line in publication_sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "FOR ALL TABLES" not in statements
    assert "FOR TABLE prompt_versions" in statements


def test_captured_tables_all_exist_as_migrations() -> None:
    from scripts.register_connector import CAPTURED_TABLES

    created = "\n".join(p.read_text(encoding="utf-8") for p in METADATA_SQL.glob("*.sql"))
    for qualified in CAPTURED_TABLES:
        table = qualified.split(".", 1)[1]
        assert f"CREATE TABLE IF NOT EXISTS {table}" in created


def test_replica_identity_full_is_set_on_the_captured_dimension() -> None:
    """Without it a delete's before-image is the primary key and nothing else,
    and test_delete_record_takes_its_image_from_before would be asserting
    against data Postgres never sends."""
    sql = (METADATA_SQL / "02_prompt_versions.sql").read_text(encoding="utf-8")
    assert "ALTER TABLE prompt_versions REPLICA IDENTITY FULL" in sql


def test_tombstones_are_enabled() -> None:
    from scripts.register_connector import connector_config

    assert connector_config("x")["tombstones.on.delete"] == "true"


def test_connector_targets_the_compose_postgres() -> None:
    """The connector connects container-to-container on 5432, not to the
    published host port -- 5433 is for `make cdc-psql` and for a human."""
    from scripts.register_connector import connector_config

    config = connector_config("x")
    assert config["database.hostname"] == "metadata-db"
    assert config["database.port"] == "5432"
    assert config["plugin.name"] == "pgoutput"


def test_wal_level_is_logical_from_the_start() -> None:
    """A slot opened against a `replica` WAL connects, reports healthy and never
    yields a row -- and wal_level needs a restart to change, so ALTERing it
    afterwards is not equivalent."""
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    command = compose["services"]["metadata-db"]["command"]
    assert "wal_level=logical" in command


def test_connect_pins_its_heap() -> None:
    """Every JVM in docker-compose.yml states its heap: Connect's own default is
    1g, which a 640m limit turns into an OOM kill under snapshot load rather
    than backpressure. Same argument as ADR-004 #8."""
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    connect = compose["services"]["connect"]
    assert "-Xmx" in connect["environment"]["KAFKA_HEAP_OPTS"]
    assert connect["mem_limit"] == "640m"


def test_connect_internal_topics_have_replication_factor_one() -> None:
    """One broker. These default to 3, and Connect does not degrade -- it fails
    to create its internal topics and never finishes starting."""
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))
    env = compose["services"]["connect"]["environment"]
    for key in (
        "CONFIG_STORAGE_REPLICATION_FACTOR",
        "OFFSET_STORAGE_REPLICATION_FACTOR",
        "STATUS_STORAGE_REPLICATION_FACTOR",
    ):
        assert env[key] == "1", key
