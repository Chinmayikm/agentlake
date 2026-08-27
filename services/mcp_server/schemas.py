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
    "Look up one agent turn's span tree by trace_id. HONEST STUB: the trace "
    "store (ClickHouse) is not wired up yet -- this always returns a "
    "structured {\"error\": ...} instead of fabricated data. Lands Day 3."
)

QUERY_METRICS_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "metric": {"type": "string", "minLength": 1},
        "window": {"type": "string", "minLength": 1},
    },
    "required": ["metric", "window"],
    "additionalProperties": False,
}

QUERY_METRICS_DESCRIPTION = (
    "Query an aggregate metric (e.g. latency_p50, cost_usd_total) over a time "
    "window (e.g. '1h', '7d'). HONEST STUB: the metrics store (ClickHouse) is "
    "not wired up yet -- this always returns a structured {\"error\": ...} "
    "instead of fabricated numbers. Lands Day 3."
)
