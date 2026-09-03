"""The analytics layer, checked as files.

No Trino, no dbt, no Docker: these read the repo. They exist for the same reason
tests/test_dashboards.py does -- the failures they catch are silent ones. A dbt
source whose column list has drifted from the contract does not error, it
compiles and quietly stops selecting a column. A Trino catalog pointed at a
different warehouse than Flink writes to does not error either: both engines
carry on working, against different data, and "one catalog, two engines" becomes
false while every test still passes.

Actually running the models is what CI's `quality` job does, and what
`make dbt-build` does locally.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

CONTRACT_PATH = REPO_ROOT / "contracts" / "trace_event_v1.avsc"
CREATE_TABLES = REPO_ROOT / "stream" / "flink" / "create_tables.py"
FLINK_JOBS = sorted((REPO_ROOT / "stream" / "flink" / "jobs").glob("*.sql"))

DBT_DIR = REPO_ROOT / "dbt"
SOURCES_YML = DBT_DIR / "models" / "sources.yml"
STAGING_YML = DBT_DIR / "models" / "staging" / "staging.yml"
MARTS_YML = DBT_DIR / "models" / "marts" / "marts.yml"
DBT_PROJECT = DBT_DIR / "dbt_project.yml"
STG_TRACE_EVENTS = DBT_DIR / "models" / "staging" / "stg_trace_events.sql"

TRINO_CATALOG = REPO_ROOT / "analytics" / "trino" / "catalog" / "lake.properties"
TRINO_CONFIG = REPO_ROOT / "analytics" / "trino" / "config.properties"
TRINO_JVM = REPO_ROOT / "analytics" / "trino" / "jvm.config"
CHECKPOINT_PY = REPO_ROOT / "quality" / "checkpoint.py"
LINEAGE_PY = REPO_ROOT / "scripts" / "emit_flink_lineage.py"
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"

MARTS = ("fct_sessions", "fct_model_costs", "fct_tool_reliability")


def _yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _properties(path: Path) -> dict[str, str]:
    """Parse a java .properties file: key=value, # comments, no sections."""
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def _source_tables(name: str) -> dict[str, Any]:
    for source in _yaml(SOURCES_YML)["sources"]:
        for table in source["tables"]:
            if table["name"] == name:
                return {"source": source, "table": table}
    raise AssertionError(f"no source table named {name} in {SOURCES_YML}")


# ---------------------------------------------------------------------------
# The contract reaches the analytics layer intact
# ---------------------------------------------------------------------------


def test_raw_source_columns_match_the_avro_contract() -> None:
    """dbt's source declaration must list exactly the contract's 13 fields.

    tests/test_cold_path_contract.py already ties create_tables.py's RAW_SCHEMA
    to the .avsc. This ties dbt to the same list, so a contract change that is
    not carried through here fails in CI instead of becoming a column nobody
    selects any more.
    """
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    expected = [field["name"] for field in contract["fields"]]

    declared = [column["name"] for column in _source_tables("trace_events")["table"]["columns"]]

    assert declared == expected, (
        "dbt/models/sources.yml lake_raw.trace_events must list the contract's "
        f"fields in order.\n  contract: {expected}\n  sources.yml: {declared}"
    )


def test_staging_model_selects_every_contract_field() -> None:
    """stg_trace_events must carry all 13 fields forward.

    ts_epoch_ms is the one exception: it is renamed to `ts` at this projection
    boundary, which is ADR-005 #8's rule ("rename where you already transform").
    Every other field keeps its contract name.
    """
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    sql = STG_TRACE_EVENTS.read_text(encoding="utf-8")

    for field in contract["fields"]:
        name = field["name"]
        if name == "ts_epoch_ms":
            assert "ts_epoch_ms as ts" in sql, (
                "ts_epoch_ms must be renamed to ts exactly once, here -- see ADR-005 #8"
            )
            continue
        assert re.search(rf"^\s+{re.escape(name)},", sql, re.MULTILINE), (
            f"stg_trace_events does not select the contract field {name!r}"
        )


def test_curated_source_columns_match_create_tables() -> None:
    """The agg source must match the Iceberg schema Flink's job writes."""
    text = CREATE_TABLES.read_text(encoding="utf-8")
    body = text[text.index("AGG_SCHEMA"):text.index("AGG_PARTITION_SPEC")]
    expected = re.findall(r'"name":\s*"(\w+)"', body)

    declared = [column["name"] for column in _source_tables("agg_model_5m")["table"]["columns"]]

    assert declared == expected, (
        "dbt/models/sources.yml lake_curated.agg_model_5m must match "
        f"create_tables.py's AGG_SCHEMA.\n  schema: {expected}\n  sources.yml: {declared}"
    )


# ---------------------------------------------------------------------------
# One catalog, two engines
# ---------------------------------------------------------------------------


def test_trino_catalog_points_at_the_same_rest_catalog_as_flink() -> None:
    """ADR-006 #1's central claim, made structural.

    If these drift, nothing breaks loudly: Flink keeps writing to its warehouse,
    Trino keeps reading a different one, and every mart is built from data that
    is not the data the cold path produced.
    """
    catalog = _properties(TRINO_CATALOG)
    flink_sql = "\n".join(path.read_text(encoding="utf-8") for path in FLINK_JOBS)

    flink_uri = re.search(r"'uri'\s*=\s*'([^']+)'", flink_sql)
    flink_warehouse = re.search(r"'warehouse'\s*=\s*'([^']+)'", flink_sql)
    flink_s3 = re.search(r"'s3\.endpoint'\s*=\s*'([^']+)'", flink_sql)
    assert flink_uri and flink_warehouse and flink_s3, "could not parse the Flink CREATE CATALOG"

    assert catalog["iceberg.rest-catalog.uri"] == flink_uri.group(1)
    assert catalog["iceberg.rest-catalog.warehouse"] == flink_warehouse.group(1)
    assert catalog["s3.endpoint"] == flink_s3.group(1)
    assert catalog["connector.name"] == "iceberg"
    assert catalog["iceberg.catalog.type"] == "rest"


def test_trino_catalog_file_is_named_for_the_catalog_flink_uses() -> None:
    """The filename IS the Trino catalog name, and it must be `lake` so that
    lake.raw.trace_events is the same fully-qualified name in both engines."""
    assert TRINO_CATALOG.stem == "lake"
    flink_sql = FLINK_JOBS[0].read_text(encoding="utf-8")
    assert re.search(r"CREATE CATALOG\s+lake\b", flink_sql)


def test_trino_credentials_are_not_committed() -> None:
    """Keys reach Trino through ${ENV:...}, the way ADR-004 #9 keeps them out of
    the Flink job SQL."""
    catalog = _properties(TRINO_CATALOG)
    assert catalog["s3.aws-access-key"].startswith("${ENV:")
    assert catalog["s3.aws-secret-key"].startswith("${ENV:")


def test_trino_query_memory_fits_inside_the_heap() -> None:
    """query.max-memory-per-node + heap-headroom must fit in the heap the JVM
    derives, or Trino refuses to start. The arithmetic is easy to break by
    editing one file and not the other."""
    config = _properties(TRINO_CONFIG)
    jvm = TRINO_JVM.read_text(encoding="utf-8")

    percent = re.search(r"-XX:MaxRAMPercentage=(\d+)", jvm)
    assert percent, "jvm.config must pin MaxRAMPercentage"

    compose = _yaml(COMPOSE_PATH)
    mem_limit = compose["services"]["trino"]["mem_limit"]
    limit_mb = int(str(mem_limit).rstrip("m"))
    heap_mb = limit_mb * int(percent.group(1)) / 100

    def mb(value: str) -> int:
        return int(value.upper().removesuffix("MB").removesuffix("GB"))

    reserved = mb(config["query.max-memory-per-node"]) + mb(
        config["memory.heap-headroom-per-node"]
    )
    assert reserved < heap_mb, (
        f"query.max-memory-per-node + heap-headroom = {reserved}MB does not fit "
        f"in the {heap_mb:.0f}MB heap derived from {mem_limit} at "
        f"{percent.group(1)}% -- Trino will refuse to start"
    )


# ---------------------------------------------------------------------------
# The gates exist and cover what they claim to
# ---------------------------------------------------------------------------


def _tests_declared_for(path: Path) -> dict[str, int]:
    """Count declared tests per model in a dbt schema yaml."""
    counts: dict[str, int] = {}
    for model in _yaml(path)["models"]:
        total = len(model.get("tests") or [])
        for column in model.get("columns") or []:
            total += len(column.get("tests") or [])
        counts[model["name"]] = total
    return counts


@pytest.mark.parametrize("mart", MARTS)
def test_every_mart_declares_tests(mart: str) -> None:
    counts = _tests_declared_for(MARTS_YML)
    assert mart in counts, f"{mart} has no entry in marts.yml"
    assert counts[mart] >= 5, f"{mart} declares only {counts[mart]} tests"


def test_blocking_test_count_meets_the_bar() -> None:
    """>= 15 blocking checks across dbt and Great Expectations combined.

    Counted rather than asserted in prose, so removing tests until the gate is
    decorative fails here.
    """
    declared = _tests_declared_for(STAGING_YML) | _tests_declared_for(MARTS_YML)
    singular = len(list((DBT_DIR / "tests").glob("*.sql")))

    # Two dbt tests are severity: warn on purpose -- accepted_values on the
    # free-form `status` string, and not_null on the contract-nullable `model`.
    warn = 2
    dbt_blocking = sum(declared.values()) + singular - warn

    checkpoint = CHECKPOINT_PY.read_text(encoding="utf-8")
    ge_blocking = checkpoint.count("meta=BLOCK")

    assert dbt_blocking + ge_blocking >= 15, (
        f"only {dbt_blocking} dbt + {ge_blocking} GE blocking checks"
    )


def test_checkpoint_covers_exactly_the_three_marts() -> None:
    checkpoint = CHECKPOINT_PY.read_text(encoding="utf-8")
    for mart in MARTS:
        assert f'table="{mart}"' in checkpoint, f"quality/checkpoint.py does not cover {mart}"


def test_checkpoint_marks_every_expectation_with_a_severity() -> None:
    """An expectation with no severity silently defaults to blocking. That is
    the safe default, but an unmarked one is more likely an oversight -- so the
    marking is required rather than inferred."""
    checkpoint = CHECKPOINT_PY.read_text(encoding="utf-8")
    expectations = re.findall(r"gxe\.Expect\w+\((.*?)\),\n", checkpoint, re.DOTALL)
    assert expectations, "no expectations found in quality/checkpoint.py"
    unmarked = [e for e in expectations if "meta=BLOCK" not in e and "meta=WARN" not in e]
    assert not unmarked, f"{len(unmarked)} expectation(s) declare no severity"


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


def test_flink_lineage_is_parsed_from_the_job_sql() -> None:
    """The declared Kafka -> Iceberg edge must be derived from the SQL Flink
    runs, not from names typed into the script -- otherwise renaming the topic
    leaves a lineage graph that is confidently wrong."""
    import sys

    sys.path.insert(0, str(REPO_ROOT))
    from scripts.emit_flink_lineage import parse_jobs

    jobs = parse_jobs()
    assert len(jobs) == len(FLINK_JOBS)

    by_name = {job["job"]: job for job in jobs}
    assert "agentlake-raw-sink" in by_name
    assert "agentlake-agg-model-5m" in by_name

    assert by_name["agentlake-raw-sink"]["table"] == "lake.raw.trace_events"
    assert by_name["agentlake-agg-model-5m"]["table"] == "lake.curated.agg_model_5m"

    for job in jobs:
        assert job["topic"] == "traces.events.v1"
        assert job["brokers"] == "kafka:19092"


def test_lineage_topic_matches_the_sdk() -> None:
    """The topic in the lineage graph is the topic the SDK produces to."""
    import sys

    sys.path.insert(0, str(REPO_ROOT))
    from scripts.emit_flink_lineage import parse_jobs
    from services.sdk import TOPIC

    assert {job["topic"] for job in parse_jobs()} == {TOPIC}


# ---------------------------------------------------------------------------
# Compose wiring
# ---------------------------------------------------------------------------


def test_analytics_profile_shares_the_storage_services_with_streaming() -> None:
    """Trino depends on iceberg-rest, which lives in the streaming profile. If
    the storage services are not ALSO in the analytics profile, compose refuses
    the whole file with "depends on undefined service"."""
    services = _yaml(COMPOSE_PATH)["services"]
    for name in ("minio", "minio-init", "iceberg-rest"):
        profiles = services[name].get("profiles", [])
        assert "analytics" in profiles and "streaming" in profiles, (
            f"{name} must belong to both the streaming and analytics profiles, got {profiles}"
        )


def test_trino_config_is_mounted_file_by_file() -> None:
    """A directory mount at /etc/trino hides node.properties and log.properties,
    which ship in the image. Same trap as ClickHouse's config.d (ADR-005 #9) and
    Flink's /opt/flink/lib."""
    volumes = _yaml(COMPOSE_PATH)["services"]["trino"]["volumes"]
    targets = [str(v).split(":")[1] for v in volumes]
    assert "/etc/trino" not in targets, "never mount a directory over /etc/trino"
    assert "/etc/trino/config.properties" in targets
    assert "/etc/trino/jvm.config" in targets
    assert "/etc/trino/catalog/lake.properties" in targets


def test_iceberg_catalog_uses_a_single_jdbc_connection() -> None:
    """The REST fixture is a JdbcCatalog on SQLite, which permits one writer.
    Iceberg's pool defaults to 2 connections, and two writers racing one SQLite
    file is SQLITE_BUSY -- which surfaced as dbt failing to drop a table. See
    ADR-006 #8."""
    environment = _yaml(COMPOSE_PATH)["services"]["iceberg-rest"]["environment"]
    assert str(environment.get("CATALOG_CLIENTS")) == "1"


def test_dbt_does_not_use_create_or_replace() -> None:
    """`replace` fails deterministically against this catalog -- a replace-table
    commit is an UPDATE the JdbcCatalog does not carry out. See ADR-006 #8."""
    project = _yaml(DBT_PROJECT)
    assert project["models"]["agentlake"]["+on_table_exists"] == "drop"
