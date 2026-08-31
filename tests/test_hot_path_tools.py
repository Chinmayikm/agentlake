"""Tests for the two ClickHouse-backed MCP tools, against an injected fake
client -- the same seam search_docs uses for store/embedder/bm25_index, and the
same one tests/test_mcp_server.py drives through dispatch_tool(deps=...).

No ClickHouse: the fake records the SQL and parameters it was handed and replays
canned rows, so what is under test is the query construction, the whitelist, the
tree builder and the failure contract -- not ClickHouse itself. See ADR-005 #5.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.mcp_server.clickhouse import ClickHouseUnavailable
from services.mcp_server.schemas import QUERY_METRICS_SCHEMA
from services.mcp_server.server import dispatch_tool
from services.mcp_server.tools import (
    GROUP_BYS,
    METRICS,
    WINDOWS,
    build_metric_sql,
    get_trace,
    query_metrics,
)


class FakeClickHouse:
    """Records every call; replays `rows`. Set `raises` to simulate an
    unreachable store."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, raises: str | None = None):
        self.rows = rows if rows is not None else []
        self.raises = raises
        self.calls: list[tuple[str, dict[str, str] | None]] = []

    def query(self, sql: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        if self.raises:
            raise ClickHouseUnavailable(self.raises)
        return self.rows

    @property
    def sql(self) -> str:
        assert self.calls, "no query was issued"
        return self.calls[-1][0]


def _span(
    span_id: str,
    parent: str | None,
    event_type: str = "LLM_CALL",
    *,
    status: str = "ok",
    latency: float = 1.0,
    cost: float | None = None,
    prompt: int | None = None,
    completion: int | None = None,
    ts: int = 1000,
    name: str = "n",
) -> dict[str, Any]:
    return {
        "span_id": span_id,
        "parent_span_id": parent,
        "session_id": "sess",
        "event_type": event_type,
        "model": "claude-haiku-4-5" if event_type == "LLM_CALL" else None,
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "latency_ms": latency,
        "cost_usd": cost,
        "status": status,
        "ts_epoch_ms": ts,
        "attributes": {"name": name},
    }


# ---------------------------------------------------------------------------
# query_metrics: the whitelist
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("metric", sorted(METRICS))
def test_every_whitelisted_metric_builds_and_runs(metric: str) -> None:
    fake = FakeClickHouse([{"spans": 3}])
    result = query_metrics(metric, "1h", ch=fake)
    assert "error" not in result
    assert result["metric"] == metric
    assert result["rows"] == [{"spans": 3}]
    assert result["row_count"] == 1
    assert "agentlake.trace_events_rt" in fake.sql


@pytest.mark.parametrize("window", sorted(WINDOWS))
def test_every_whitelisted_window_maps_to_an_interval(window: str) -> None:
    fake = FakeClickHouse()
    query_metrics("request_rate", window, ch=fake)
    assert WINDOWS[window] in fake.sql


@pytest.mark.parametrize("group_by", sorted(GROUP_BYS))
def test_every_whitelisted_group_by_is_accepted(group_by: str) -> None:
    fake = FakeClickHouse()
    result = query_metrics("request_rate", "1h", group_by, ch=fake)
    assert "error" not in result
    if group_by != "none":
        assert f"GROUP BY {GROUP_BYS[group_by]}" in fake.sql
    else:
        assert "GROUP BY" not in fake.sql


def test_unknown_metric_is_reported_and_never_reaches_the_store() -> None:
    fake = FakeClickHouse()
    result = query_metrics("DROP TABLE trace_events_rt", "1h", ch=fake)
    assert "unknown metric" in result["error"]
    assert fake.calls == []


def test_unknown_window_is_reported_and_never_reaches_the_store() -> None:
    fake = FakeClickHouse()
    result = query_metrics("request_rate", "1 HOUR; DROP TABLE x", ch=fake)
    assert "unknown window" in result["error"]
    assert fake.calls == []


def test_unknown_group_by_is_reported_and_never_reaches_the_store() -> None:
    fake = FakeClickHouse()
    result = query_metrics("request_rate", "1h", "model, (SELECT 1)", ch=fake)
    assert "unknown group_by" in result["error"]
    assert fake.calls == []


def test_no_caller_text_can_reach_the_sql() -> None:
    """The whitelist is the whole defence: SQL is assembled from module-level
    dicts, so there is no path by which a caller's string is concatenated into
    a query. This asserts that property directly rather than trusting it."""
    hostile = "'; DROP TABLE agentlake.trace_events_rt; --"
    for args in (
        (hostile, "1h", "none"),
        ("request_rate", hostile, "none"),
        ("request_rate", "1h", hostile),
    ):
        fake = FakeClickHouse()
        result = query_metrics(*args, ch=fake)
        assert "error" in result
        assert fake.calls == []


def test_metrics_sql_is_assembled_only_from_the_whitelists() -> None:
    for metric in METRICS:
        for window in WINDOWS:
            for group_by in GROUP_BYS:
                sql = build_metric_sql(metric, window, group_by)
                assert "{" not in sql and "}" not in sql, f"unfilled placeholder: {sql}"


def test_result_echoes_the_sql_that_produced_it() -> None:
    """An agent quoting a number is asking to be believed; shipping the query
    makes the claim checkable. See ADR-005 #5."""
    fake = FakeClickHouse([{"p95_ms": 12.0}])
    result = query_metrics("p95_latency", "1h", ch=fake)
    assert result["sql"] == fake.sql
    assert "quantile(0.95)" in result["sql"]


def test_fixed_grouping_metrics_ignore_group_by() -> None:
    """cost_by_model already groups by model; a second dimension on top would be
    a different metric, not an option, so the reported group_by says so."""
    fake = FakeClickHouse()
    result = query_metrics("cost_by_model", "1h", "event_type", ch=fake)
    assert result["group_by"] == "none"
    assert "event_type" not in fake.sql


def test_counts_use_distinct_span_ids_not_row_counts() -> None:
    """The dedup posture (ADR-005 #2) lives in this one detail: the Kafka engine
    is at-least-once, so count() can transiently over-report and
    uniqExact(span_id) cannot."""
    for metric in ("request_rate", "error_rate", "tool_error_rate"):
        sql = build_metric_sql(metric, "1h", "none")
        assert "uniqExact(span_id)" in sql
        assert "count()" not in sql


def test_error_rate_tests_status_inequality_not_equality() -> None:
    """status is a free-form contract string -- services/sdk/telemetry.py
    promotes any set(status=...) value, and "degraded" is already covered in
    tests/test_telemetry.py -- so counting `status = 'error'` would undercount."""
    for metric in ("error_rate", "tool_error_rate"):
        sql = build_metric_sql(metric, "1h", "none")
        assert "status != 'ok'" in sql
        assert "status = 'error'" not in sql


def test_unreachable_store_is_reported_not_fabricated() -> None:
    fake = FakeClickHouse(raises="connection refused")
    result = query_metrics("request_rate", "1h", ch=fake)
    assert result == {"error": "metrics store unreachable: connection refused"}


def test_schema_enums_match_the_implementation_whitelists() -> None:
    """The whitelist is enforced twice -- by jsonschema before dispatch, and in
    the tool itself for non-MCP callers. They have to agree, or one path accepts
    what the other rejects."""
    properties = QUERY_METRICS_SCHEMA["properties"]
    assert set(properties["metric"]["enum"]) == set(METRICS)
    assert set(properties["window"]["enum"]) == set(WINDOWS)
    assert set(properties["group_by"]["enum"]) == set(GROUP_BYS)


# ---------------------------------------------------------------------------
# get_trace: the tree
# ---------------------------------------------------------------------------


def test_get_trace_binds_the_trace_id_as_a_parameter() -> None:
    """Bound, not formatted: a trace_id containing a quote is a trace_id that
    matches nothing, rather than a syntax error or an injection."""
    fake = FakeClickHouse([_span("a", None)])
    get_trace("'; DROP TABLE x; --", ch=fake)
    sql, params = fake.calls[-1]
    assert params == {"trace_id": "'; DROP TABLE x; --"}
    assert "{trace_id:String}" in sql
    assert "DROP TABLE" not in sql


def test_get_trace_deduplicates_its_own_result_set() -> None:
    """Point lookups dedup, aggregates tolerate (ADR-005 #2). A trace is tens of
    rows, so collapsing them outright costs nothing."""
    fake = FakeClickHouse([_span("a", None)])
    get_trace("t", ch=fake)
    assert "LIMIT 1 BY span_id" in fake.sql


def test_get_trace_builds_a_nested_tree() -> None:
    rows = [
        _span("root", None, "AGENT_STEP", ts=100),
        _span("child", "root", "LLM_CALL", ts=101, cost=0.5, prompt=10, completion=5),
        _span("grandchild", "child", "RETRIEVAL", ts=102),
    ]
    result = get_trace("t", ch=FakeClickHouse(rows))
    assert result["span_count"] == 3
    assert result["root_count"] == 1
    assert result["orphan_count"] == 0
    assert result["total_cost_usd"] == 0.5
    assert result["total_tokens"] == 15
    assert result["duration_ms"] == 2

    (root,) = result["spans"]
    assert root["span_id"] == "root"
    (child,) = root["children"]
    assert child["span_id"] == "child"
    assert [g["span_id"] for g in child["children"]] == ["grandchild"]


def test_get_trace_links_children_that_arrive_before_their_parents() -> None:
    """A span emits from its own `with` block's exit, so a child's event reaches
    Kafka BEFORE its parent's (see scripts/consume_tree.py). A tree builder that
    assumed parents came first would orphan half of every trace."""
    rows = [_span("child", "root", ts=101), _span("root", None, "AGENT_STEP", ts=100)]
    result = get_trace("t", ch=FakeClickHouse(rows))
    assert result["orphan_count"] == 0
    (root,) = result["spans"]
    assert [c["span_id"] for c in root["children"]] == ["child"]


def test_get_trace_keeps_orphans_as_flagged_roots() -> None:
    """A missing parent is real: the 7-day TTL can expire a parent while a child
    survives. Dropping the child would under-report the turn's cost while the
    result still looked complete."""
    rows = [_span("child", "expired-parent", cost=0.25)]
    result = get_trace("t", ch=FakeClickHouse(rows))
    assert result["orphan_count"] == 1
    assert result["root_count"] == 1
    assert result["spans"][0]["orphan"] is True
    assert result["total_cost_usd"] == 0.25


def test_get_trace_counts_errors_by_status_inequality() -> None:
    rows = [
        _span("a", None, "AGENT_STEP", status="error"),
        _span("b", "a", "TOOL_CALL", status="degraded"),
        _span("c", "a", status="ok"),
    ]
    result = get_trace("t", ch=FakeClickHouse(rows))
    assert result["error_count"] == 2


def test_get_trace_tolerates_null_cost_and_tokens() -> None:
    """Only LLM_CALL spans carry cost and tokens; every other event type has
    NULL there, which is real data rather than a defect."""
    rows = [_span("a", None, "RETRIEVAL"), _span("b", "a", "LLM_CALL", cost=0.1, prompt=7)]
    result = get_trace("t", ch=FakeClickHouse(rows))
    assert result["total_cost_usd"] == 0.1
    assert result["total_tokens"] == 7


def test_get_trace_not_found_is_reported_not_invented() -> None:
    """An empty tree would read as "this turn did nothing"; the error says which
    of the two it is, and where the older data went."""
    result = get_trace("missing", ch=FakeClickHouse([]))
    assert "not found" in result["error"]
    assert "lake.raw.trace_events" in result["error"]
    assert "spans" not in result


def test_get_trace_unreachable_store_is_reported_not_fabricated() -> None:
    result = get_trace("t", ch=FakeClickHouse(raises="connection refused"))
    assert result == {"error": "trace store unreachable: connection refused"}


# ---------------------------------------------------------------------------
# Through dispatch_tool, the way the MCP server actually calls them
# ---------------------------------------------------------------------------


def test_query_metrics_through_dispatch_tool_with_injected_client(events) -> None:
    fake = FakeClickHouse([{"spans": 1}])
    args = {"metric": "request_rate", "window": "1h"}
    result = dispatch_tool("query_metrics", args, {"ch": fake})
    assert result["rows"] == [{"spans": 1}]
    assert events[0]["event_type"] == "TOOL_CALL"


def test_get_trace_through_dispatch_tool_with_injected_client(events) -> None:
    fake = FakeClickHouse([_span("root", None, "AGENT_STEP")])
    result = dispatch_tool("get_trace", {"trace_id": "t"}, {"ch": fake})
    assert result["span_count"] == 1
    assert events[0]["attributes"]["tool"] == "get_trace"


def test_schema_validation_rejects_a_bad_metric_before_dispatch(events) -> None:
    """The enum in QUERY_METRICS_SCHEMA means dispatch_tool's jsonschema pass
    rejects this without the tool -- or a ClickHouse client -- being reached."""
    result = dispatch_tool("query_metrics", {"metric": "everything", "window": "1h"})
    assert "invalid arguments" in result["error"]
