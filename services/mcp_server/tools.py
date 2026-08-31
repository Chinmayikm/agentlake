"""The three tool implementations. Plain Python in, plain dict out -- no MCP
types here, so these are testable as ordinary functions (see
tests/test_mcp_server.py, tests/test_hot_path_tools.py) and framework-agnostic
if the transport ever changes.

get_trace and query_metrics were honest stubs until ADR-005 landed the hot path.
What has NOT changed is the shape ADR-003 #3 fixed: {"error": ...} is still the
right answer whenever the store is unreachable or the trace is not there. It was
written as the permanent contract, not as a placeholder, and it stayed one.
"""

from __future__ import annotations

from typing import Any

from services.mcp_server.clickhouse import ClickHouseClient, ClickHouseUnavailable
from services.rag.bm25 import BM25Index
from services.rag.embed import Embedder
from services.rag.retrieve import RetrievalMode, retrieve
from services.rag.store import Store

TABLE = "agentlake.trace_events_rt"

# A trace is one agent turn (ADR-003 #4): AGENT_STEP -> GATEWAY/TOOL_CALL ->
# LLM_CALL/RETRIEVAL, so tens of spans, not thousands. The cap is a guard
# against a pathological trace becoming a pathological tool result, since
# services/agent/loop.py json.dumps a tool result whole into the model's
# context. A truncated tree says so rather than silently shrinking.
_MAX_SPANS = 500


# ---------------------------------------------------------------------------
# search_docs
# ---------------------------------------------------------------------------


def search_docs(
    query: str,
    k: int = 5,
    mode: RetrievalMode = "hybrid",
    *,
    store: Store | None = None,
    embedder: Embedder | None = None,
    bm25_index: BM25Index | None = None,
) -> dict:
    """Wrap services.rag.retrieve() as a tool result.

    store/embedder/bm25_index are injectable purely for tests -- production
    calls (via server.dispatch_tool with no `deps`) always pass None here, so
    retrieve() falls back to its own real defaults (QdrantStore,
    FastEmbedEmbedder, BM25Index.load()), same as the CLI in services/rag/cli.py.
    """
    chunks = retrieve(query, k, mode=mode, store=store, embedder=embedder, bm25_index=bm25_index)
    return {
        "results": [
            {
                "chunk_id": chunk.chunk_id,
                "project": chunk.project,
                "source_path": chunk.source_path,
                "section_path": chunk.section,
                "text": chunk.text,
                "score": chunk.score,
            }
            for chunk in chunks
        ]
    }


# ---------------------------------------------------------------------------
# query_metrics -- a closed vocabulary, never free-form SQL
# ---------------------------------------------------------------------------
#
# Three dicts, and between them they are the ONLY source of SQL text that
# reaches ClickHouse. A caller picks a key from each; it cannot supply a
# fragment, and there is no code path that concatenates caller text into a
# query. That is worth more than injection safety alone: a closed set is a set
# whose cost and correctness can be measured, which is what makes
# scripts/hot_path_verify.py's panel budget meaningful for the agent's queries
# too. See ADR-005 #5.
#
# Counting follows the hot path's shared vocabulary (ADR-005 #2 and #3):
# uniqExact(span_id) for counts, so they are exact whatever the merge state;
# plain sum() for money and tokens, which tolerates a transient over-count;
# status != 'ok' rather than status = 'error', because status is a free-form
# contract string.

WINDOWS: dict[str, str] = {
    "5m": "INTERVAL 5 MINUTE",
    "15m": "INTERVAL 15 MINUTE",
    "1h": "INTERVAL 1 HOUR",
    "24h": "INTERVAL 24 HOUR",
}

GROUP_BYS: dict[str, str] = {
    "none": "",
    "model": "model",
    "event_type": "event_type",
    "status": "status",
}

# {where} and {group} are filled from the dicts above and from nothing else.
METRICS: dict[str, str] = {
    "request_rate": (
        "SELECT {group_select}uniqExact(span_id) AS spans, "
        "uniqExact(span_id) / {window_seconds} * 60 AS spans_per_min "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window}{extra_where}{group_by}"
    ),
    "p95_latency": (
        "SELECT {group_select}"
        "round(quantile(0.5)(latency_ms), 3) AS p50_ms, "
        "round(quantile(0.95)(latency_ms), 3) AS p95_ms, "
        "round(quantile(0.99)(latency_ms), 3) AS p99_ms, "
        "uniqExact(span_id) AS spans "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window}{extra_where}{group_by}"
    ),
    "error_rate": (
        "SELECT {group_select}"
        "uniqExact(span_id) AS spans, "
        "uniqExactIf(span_id, status != 'ok') AS errors, "
        "round(100.0 * uniqExactIf(span_id, status != 'ok') "
        "/ greatest(uniqExact(span_id), 1), 3) AS error_rate_pct "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window}{extra_where}{group_by}"
    ),
    "cost_by_model": (
        "SELECT model, round(sum(cost_usd), 6) AS cost_usd, "
        "uniqExact(span_id) AS spans, uniqExact(trace_id) AS turns "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window} AND model IS NOT NULL "
        "GROUP BY model ORDER BY cost_usd DESC"
    ),
    # The aliases must not repeat the column names. `sum(prompt_tokens) AS
    # prompt_tokens` puts the alias in scope for the rest of the SELECT, so a
    # later `sum(prompt_tokens)` resolves to the aggregate rather than to the
    # column and ClickHouse rejects the query with "Aggregate function ... is
    # found inside another aggregate function" (code 184). Suffixing the
    # aliases is the whole fix.
    "tokens_by_model": (
        "SELECT model, sum(prompt_tokens) AS prompt_tokens_sum, "
        "sum(completion_tokens) AS completion_tokens_sum, "
        "sum(prompt_tokens) + sum(completion_tokens) AS total_tokens, "
        "uniqExact(span_id) AS spans "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window} AND model IS NOT NULL "
        "GROUP BY model ORDER BY total_tokens DESC"
    ),
    "tool_error_rate": (
        "SELECT {group_select}"
        "uniqExact(span_id) AS tool_calls, "
        "uniqExactIf(span_id, status != 'ok') AS errors, "
        "round(100.0 * uniqExactIf(span_id, status != 'ok') "
        "/ greatest(uniqExact(span_id), 1), 3) AS error_rate_pct "
        f"FROM {TABLE} "
        "WHERE ts >= now() - {window} AND event_type = 'TOOL_CALL'{group_by}"
    ),
}

# cost_by_model and tokens_by_model group by model inherently; offering a second
# grouping dimension on top would be a different metric, not an option.
_FIXED_GROUPING = {"cost_by_model", "tokens_by_model"}

_WINDOW_SECONDS: dict[str, int] = {"5m": 300, "15m": 900, "1h": 3600, "24h": 86400}


def build_metric_sql(metric: str, window: str, group_by: str) -> str:
    """Assemble one whitelisted query. Every fragment comes from a module-level
    dict; nothing is interpolated from caller text.
    """
    template = METRICS[metric]
    grouping = "" if metric in _FIXED_GROUPING else GROUP_BYS[group_by]
    column = grouping if grouping else ""
    return template.format(
        window=WINDOWS[window],
        window_seconds=_WINDOW_SECONDS[window],
        group_select=f"{column}, " if column else "",
        group_by=f" GROUP BY {column} ORDER BY {column}" if column else "",
        extra_where="",
    )


def query_metrics(
    metric: str,
    window: str,
    group_by: str = "none",
    *,
    ch: ClickHouseClient | None = None,
) -> dict:
    """Run one whitelisted aggregate over the hot table and return its rows.

    The result carries the exact SQL that produced it. That is the point, not a
    debugging aid: an agent quoting a p95 to a human is asking to be believed,
    and shipping the query alongside the number makes the claim checkable
    instead of trusted. See ADR-005 #5.

    `ch` is injectable for tests, exactly as search_docs takes store/embedder;
    production calls pass None and get a real client.

    Returns {"error": ...} and never a fabricated number when ClickHouse is
    unreachable -- the contract ADR-003 #3 fixed for these tools before the
    store existed, kept now that it does.
    """
    if metric not in METRICS:
        return {"error": f"unknown metric {metric!r}; known metrics: {sorted(METRICS)}"}
    if window not in WINDOWS:
        return {"error": f"unknown window {window!r}; known windows: {sorted(WINDOWS)}"}
    if group_by not in GROUP_BYS:
        return {"error": f"unknown group_by {group_by!r}; allowed: {sorted(GROUP_BYS)}"}

    sql = build_metric_sql(metric, window, group_by)
    client = ch if ch is not None else ClickHouseClient()
    try:
        rows = client.query(sql)
    except ClickHouseUnavailable as exc:
        return {"error": f"metrics store unreachable: {exc}"}

    return {
        "metric": metric,
        "window": window,
        "group_by": "none" if metric in _FIXED_GROUPING else group_by,
        "sql": sql,
        "row_count": len(rows),
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# get_trace
# ---------------------------------------------------------------------------

_TRACE_SQL = (
    "SELECT span_id, parent_span_id, session_id, event_type, model, "
    "prompt_tokens, completion_tokens, latency_ms, cost_usd, status, "
    "toUnixTimestamp64Milli(ts) AS ts_epoch_ms, attributes "
    f"FROM {TABLE} "
    "WHERE trace_id = {trace_id:String} "
    "ORDER BY ts "
    # The point-lookup half of ADR-005 #2's posture: aggregates tolerate
    # transient duplicates, point lookups dedup. A trace is tens of rows, so
    # collapsing them outright costs nothing and the model is never handed the
    # same span twice.
    "LIMIT 1 BY span_id "
    f"LIMIT {_MAX_SPANS}"
)


def _node(row: dict[str, Any]) -> dict[str, Any]:
    attributes = row.get("attributes") or {}
    return {
        "span_id": row["span_id"],
        "name": attributes.get("name"),
        "event_type": row["event_type"],
        "status": row["status"],
        "latency_ms": row["latency_ms"],
        "model": row["model"],
        "prompt_tokens": row["prompt_tokens"],
        "completion_tokens": row["completion_tokens"],
        "cost_usd": row["cost_usd"],
        "ts_epoch_ms": row["ts_epoch_ms"],
        "attributes": attributes,
        "children": [],
    }


def _build_tree(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Link spans into a parent/child forest. Returns (roots, orphan_count).

    Two properties this must have, both learned from how the spans are actually
    produced:

    - It cannot assume a parent appears before its children. A span emits from
      its own `with` block's exit, so a child's event reaches Kafka BEFORE its
      parent's (see scripts/consume_tree.py). Every row is indexed first, then
      linked.
    - A span whose parent is missing is kept as a root and counted as an orphan,
      not dropped. Missing parents are real here: the 7-day TTL can expire a
      parent while a child survives, and _MAX_SPANS can truncate. Silently
      dropping those spans would under-report a trace's cost while looking
      complete, which is the failure mode this whole tool exists not to have.
    """
    nodes = {row["span_id"]: _node(row) for row in rows}
    roots: list[dict[str, Any]] = []
    orphans = 0
    for row in rows:
        node = nodes[row["span_id"]]
        parent_id = row.get("parent_span_id")
        if parent_id is None:
            roots.append(node)
        elif parent_id in nodes:
            nodes[parent_id]["children"].append(node)
        else:
            node["orphan"] = True
            orphans += 1
            roots.append(node)
    return roots, orphans


def get_trace(trace_id: str, *, ch: ClickHouseClient | None = None) -> dict:
    """Look up one agent turn's span tree by trace_id.

    One trace is one turn end to end across all three processes -- agent,
    gateway, MCP server -- which is exactly what ADR-003 #4 rebuilt trace
    propagation to make true, and what this tool was waiting for a store to be
    able to read back.

    Honest on both failure paths, per ADR-003 #3: an unreachable store and a
    trace that is not there each return a structured {"error": ...} that says
    which, and never an empty tree that would read as "this turn did nothing".
    """
    client = ch if ch is not None else ClickHouseClient()
    try:
        rows = client.query(_TRACE_SQL, {"trace_id": trace_id})
    except ClickHouseUnavailable as exc:
        return {"error": f"trace store unreachable: {exc}"}

    if not rows:
        return {
            "error": (
                f"trace {trace_id} not found in {TABLE}. The hot table keeps 7 days; "
                f"older traces are in the lake (lake.raw.trace_events)."
            )
        }

    roots, orphans = _build_tree(rows)
    timestamps = [row["ts_epoch_ms"] for row in rows]
    return {
        "trace_id": trace_id,
        "session_id": rows[0]["session_id"],
        "span_count": len(rows),
        "root_count": len(roots),
        "orphan_count": orphans,
        "truncated": len(rows) >= _MAX_SPANS,
        "error_count": sum(1 for row in rows if row["status"] != "ok"),
        "duration_ms": max(timestamps) - min(timestamps) if timestamps else 0,
        "total_cost_usd": round(sum(row["cost_usd"] or 0.0 for row in rows), 6),
        "total_tokens": sum(
            (row["prompt_tokens"] or 0) + (row["completion_tokens"] or 0) for row in rows
        ),
        "spans": roots,
    }
