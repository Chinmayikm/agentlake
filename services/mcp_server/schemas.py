"""JSON Schemas + descriptions for each tool. One source of truth, used both
for the MCP ``Tool.inputSchema`` advertised to clients and for the explicit
``jsonschema.validate()`` call in ``server.dispatch_tool()``.
"""

from __future__ import annotations

SEARCH_DOCS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "query": {"type": "string", "minLength": 1},
        "k": {"type": "integer", "minimum": 1, "maximum": 50, "default": 5},
        "mode": {
            "type": "string",
            "enum": ["dense", "bm25", "hybrid"],
            "default": "hybrid",
        },
    },
    "required": ["query"],
    "additionalProperties": False,
}

SEARCH_DOCS_DESCRIPTION = (
    "Search the pinned Kafka/Flink/Iceberg docs corpus. Returns ranked chunks "
    "with their source section and project, most relevant first. mode=hybrid "
    "(default) fuses dense and BM25 retrieval; use mode=dense or mode=bm25 to "
    "run either alone."
)

GET_TRACE_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "trace_id": {"type": "string", "minLength": 1},
    },
    "required": ["trace_id"],
    "additionalProperties": False,
}

GET_TRACE_DESCRIPTION = (
    "Look up one agent turn's span tree by trace_id, from the hot store "
    "(ClickHouse, last 7 days). One trace is one turn end to end across the "
    "agent, the gateway and this MCP server. Returns the spans as a nested "
    "parent/child tree with per-span event_type, status, latency_ms, model, "
    "tokens and cost_usd, plus turn totals (span_count, duration_ms, "
    "total_cost_usd, total_tokens, error_count). Returns a structured "
    "{\"error\": ...} -- never an empty or invented tree -- if the trace is not "
    "found or the store is unreachable."
)

# The enums are the whitelist, and they are enforced twice on purpose: here, so
# server.dispatch_tool()'s jsonschema.validate() rejects a bad argument before
# the tool runs, and again inside services/mcp_server/tools.py, which is the
# seam tests exercise and the one a non-MCP caller would reach. See ADR-005 #5.
QUERY_METRICS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "metric": {
            "type": "string",
            "enum": [
                "request_rate",
                "p95_latency",
                "error_rate",
                "cost_by_model",
                "tokens_by_model",
                "tool_error_rate",
            ],
        },
        "window": {"type": "string", "enum": ["5m", "15m", "1h", "24h"]},
        "group_by": {
            "type": "string",
            "enum": ["none", "model", "event_type", "status"],
            "default": "none",
        },
    },
    "required": ["metric", "window"],
    "additionalProperties": False,
}

QUERY_METRICS_DESCRIPTION = (
    "Query one pre-defined aggregate over the hot store (ClickHouse, last 7 "
    "days of trace events). There is no free-form SQL: pick a metric from "
    "request_rate, p95_latency (returns p50/p95/p99), error_rate, "
    "cost_by_model, tokens_by_model, tool_error_rate; a window from 5m, 15m, "
    "1h, 24h; and optionally group_by one of model, event_type, status "
    "(cost_by_model and tokens_by_model already group by model). The result "
    "includes the exact SQL that produced it, so any number you quote from it "
    "can be checked. Returns a structured {\"error\": ...} rather than a "
    "plausible number if the store is unreachable."
)
