"""The provisioned Grafana dashboards, checked as files.

A broken dashboard does not fail loudly: Grafana provisions it, the page loads,
and individual panels report "datasource not found" or an empty result inside
their own frame. Nothing exits non-zero and nothing appears in a log anyone
reads. So the parts that can silently disagree are asserted here instead:

1. every panel's datasource uid against the provisioned datasource's uid,
2. every column and table named in a panel query against the ClickHouse DDL,
3. the dashboard provider's options.path against the docker-compose mount.

No Grafana, no ClickHouse, no Docker: these read files. Panel *timings* are a
different question and live in scripts/hot_path_verify.py, which runs the same
queries against the real store. See ADR-005.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = REPO_ROOT / "dashboards" / "json"
DATASOURCE_YAML = REPO_ROOT / "dashboards" / "provisioning" / "datasources" / "clickhouse.yaml"
PROVIDER_YAML = REPO_ROOT / "dashboards" / "provisioning" / "dashboards" / "agentlake.yaml"
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
RT_SQL = REPO_ROOT / "stream" / "clickhouse" / "sql" / "02_trace_events_rt.sql"

# Column body of a `CREATE TABLE ... ( ... ) ENGINE` statement.
_CREATE_TABLE_BODY = r"CREATE TABLE IF NOT EXISTS \w+\.\w+\s*\n\((.*?)\n\)\s*\nENGINE"

DASHBOARDS = sorted(DASHBOARD_DIR.glob("*.json"))
DATASOURCE_TYPE = "grafana-clickhouse-datasource"


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _panels(doc: dict[str, Any]) -> list[dict[str, Any]]:
    return doc.get("panels", [])


def _targets() -> list[tuple[str, str, dict[str, Any]]]:
    """(dashboard, panel title, target) for every SQL target across all files."""
    found = []
    for path in DASHBOARDS:
        for panel in _panels(_load(path)):
            for target in panel.get("targets", []):
                found.append((path.stem, panel.get("title", "?"), target))
    return found


@pytest.fixture(scope="module")
def datasource_uid() -> str:
    return _yaml(DATASOURCE_YAML)["datasources"][0]["uid"]


@pytest.fixture(scope="module")
def hot_table_columns() -> set[str]:
    """Column names straight from the DDL, so a renamed column breaks the panels
    that reference it here rather than in a browser."""
    sql = RT_SQL.read_text(encoding="utf-8")
    body = re.search(_CREATE_TABLE_BODY, sql, re.DOTALL)
    assert body
    names = set()
    for line in body.group(1).splitlines():
        line = line.strip().rstrip(",")
        if line and not line.startswith("--"):
            names.add(line.split(" ")[0])
    return names


# ---------------------------------------------------------------------------
# Datasource wiring
# ---------------------------------------------------------------------------


def test_datasource_declares_an_explicit_uid(datasource_uid: str) -> None:
    """Without a pinned uid Grafana generates a random one per install, and
    every dashboard that references it provisions fine and then renders
    "datasource not found" in each panel."""
    assert datasource_uid == "clickhouse"


def test_datasource_points_at_the_compose_service() -> None:
    json_data = _yaml(DATASOURCE_YAML)["datasources"][0]["jsonData"]
    # Compose DNS, and the native port -- which is deliberately not published on
    # the host (minio owns 9000 under --profile streaming). See ADR-005 #1.
    assert json_data["host"] == "clickhouse"
    assert json_data["port"] == 9000
    assert json_data["protocol"] == "native"
    assert json_data["defaultDatabase"] == "agentlake"


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_every_target_references_the_provisioned_datasource(
    dashboard: str, title: str, target: dict[str, Any], datasource_uid: str
) -> None:
    assert target["datasource"]["uid"] == datasource_uid, f"{dashboard}/{title}"
    assert target["datasource"]["type"] == DATASOURCE_TYPE, f"{dashboard}/{title}"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_every_sql_panel_references_the_provisioned_datasource(
    path: Path, datasource_uid: str
) -> None:
    for panel in _panels(_load(path)):
        if panel["type"] == "text":  # text panels legitimately have no datasource
            continue
        assert panel["datasource"]["uid"] == datasource_uid, panel.get("title")


# ---------------------------------------------------------------------------
# Dashboard JSON shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dashboard_id_is_null(path: Path) -> None:
    """Grafana assigns its own id on provisioning; a stale non-null one collides."""
    assert _load(path)["id"] is None


def test_dashboard_uids_and_titles_are_unique() -> None:
    docs = [_load(p) for p in DASHBOARDS]
    uids = [d["uid"] for d in docs]
    titles = [d["title"] for d in docs]
    assert len(set(uids)) == len(uids)
    assert len(set(titles)) == len(titles)


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dashboard_is_time_picker_driven(path: Path) -> None:
    """Every query must be bounded by the panel's time range, or the dashboard
    scans the whole retention window regardless of what the picker says -- and
    the NFR-5 budget stops meaning anything."""
    doc = _load(path)
    assert "time" in doc and doc["time"]["from"].startswith("now-")
    for panel in _panels(doc):
        for target in panel.get("targets", []):
            assert "$__timeFilter_ms(ts)" in target["rawSql"], panel.get("title")


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.name)
def test_dashboard_panels_have_unique_ids_and_titles(path: Path) -> None:
    panels = _panels(_load(path))
    ids = [p["id"] for p in panels]
    assert len(set(ids)) == len(ids)


# ---------------------------------------------------------------------------
# The SQL the panels actually run
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_panel_queries_only_touch_the_hot_table(
    dashboard: str, title: str, target: dict[str, Any]
) -> None:
    tables = set(re.findall(r"FROM\s+([\w.]+)", target["rawSql"]))
    assert tables == {"agentlake.trace_events_rt"}, f"{dashboard}/{title}: {tables}"


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_panel_grouping_names_a_real_column_or_a_select_alias(
    dashboard: str, title: str, target: dict[str, Any], hot_table_columns: set[str]
) -> None:
    """Catches a renamed column, which otherwise surfaces as a human noticing an
    empty panel days later.

    GROUP BY is the strongest place to check: a name there is either a column of
    the hot table or an alias the same SELECT defined, and nothing else. That
    makes it decidable from the text, unlike the SELECT list, where function
    calls and literals would need a real parser to tell apart from identifiers.
    """
    sql = target["rawSql"]
    for clause in re.findall(r"GROUP BY (.+?)(?: ORDER BY| LIMIT|$)", sql):
        for name in (part.strip() for part in clause.split(",")):
            if not name:
                continue
            assert name in hot_table_columns or f"AS {name}" in sql, (
                f"{dashboard}/{title}: GROUP BY {name} is neither a column of "
                f"trace_events_rt nor an alias defined in this query"
            )


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_panel_counts_use_distinct_span_ids(
    dashboard: str, title: str, target: dict[str, Any]
) -> None:
    """The dedup posture (ADR-005 #2) has to hold on the dashboard too: the
    Kafka engine is at-least-once, so a bare count() can transiently
    over-report where uniqExact(span_id) cannot."""
    sql = target["rawSql"]
    assert "count()" not in sql, f"{dashboard}/{title} uses count(); use uniqExact(span_id)"


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_panel_error_checks_use_status_inequality(
    dashboard: str, title: str, target: dict[str, Any]
) -> None:
    """status is a free-form contract string, so `status = 'error'` would miss
    any other non-ok value the SDK was given."""
    sql = target["rawSql"]
    assert "status = 'error'" not in sql, f"{dashboard}/{title}"


@pytest.mark.parametrize(("dashboard", "title", "target"), _targets(), ids=lambda v: str(v)[:40])
def test_panel_queries_use_no_final(dashboard: str, title: str, target: dict[str, Any]) -> None:
    """FINAL everywhere is rejected as a habit, not on cost (ADR-005 #2). This
    is where that decision would quietly erode."""
    assert not re.search(r"\bFINAL\b", target["rawSql"]), f"{dashboard}/{title}"


# ---------------------------------------------------------------------------
# Provisioning paths agree with the compose mounts
# ---------------------------------------------------------------------------


def test_provider_path_matches_the_compose_mount() -> None:
    """These two live in different files and neither validates the other; a
    mismatch means Grafana starts cleanly with no dashboards at all."""
    path = _yaml(PROVIDER_YAML)["providers"][0]["options"]["path"]
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    assert f"./dashboards/json:{path}:ro" in compose


def test_datasource_provisioning_directory_is_mounted() -> None:
    compose = COMPOSE_PATH.read_text(encoding="utf-8")
    assert "./dashboards/provisioning:/etc/grafana/provisioning:ro" in compose


def test_there_are_dashboards_and_targets_to_check() -> None:
    """A glob that silently matches nothing turns every parametrized test above
    into a no-op that still reports green."""
    assert DASHBOARDS
    assert _targets()
