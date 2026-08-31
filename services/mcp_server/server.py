"""Server("agentlake-mcp") -- tool registry, dispatch, and the stdio entrypoint.

dispatch_tool() is the seam tests exercise directly (see tests/test_mcp_server.py),
independent of the MCP protocol machinery: it strips the trace-context sidecar
(see below), validates the remaining arguments against each tool's JSON Schema,
calls the tool function, and emits one TOOL_CALL span, mirroring how
tests/test_rag_retrieve.py tests retrieve() itself rather than the CLI wrapper.

@server.call_tool() is registered with validate_input=False: the MCP low-level
Server's own automatic argument validation (mcp.server.lowlevel, on by default)
would reject every call carrying a _trace_context sidecar, since every tool
schema sets additionalProperties: False. dispatch_tool()'s own
jsonschema.validate() -- run on the arguments *after* _trace_context is
stripped -- is the real, tested validation path; the framework's was only ever
defense in depth, and now it would actively conflict with cross-process trace
propagation (ADR-003 #4), so it's off.

Trace propagation: contextvars don't cross the stdio process boundary, so a
caller (services/agent/mcp_client.py) that wants this process's spans to join
its own trace has no way to say so except inside the call itself. Arguments
may carry an optional "_trace_context": {"trace_id": ..., "parent_span_id": ...}
key -- dispatch_tool() pops it before validation/dispatch and passes it to
span() so the resulting TOOL_CALL (and anything it nests, e.g. RETRIEVAL) joins
the caller's trace instead of rooting a new one.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

import jsonschema
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from services.mcp_server.schemas import (
    GET_TRACE_DESCRIPTION,
    GET_TRACE_SCHEMA,
    QUERY_METRICS_DESCRIPTION,
    QUERY_METRICS_SCHEMA,
    SEARCH_DOCS_DESCRIPTION,
    SEARCH_DOCS_SCHEMA,
)
from services.mcp_server.tools import get_trace, query_metrics, search_docs
from services.sdk import session, span

logger = logging.getLogger("agentlake.mcp_server")

# A span attribute is a string in a map<string,string> Avro field -- truncate
# so a pathological tool call can't balloon the emitted event. Matches
# services/rag/retrieve.py's _MAX_QUERY_ATTR precedent (ADR-002 #4).
_MAX_ARGS_PREVIEW = 200


@dataclass(frozen=True, slots=True)
class ToolSpec:
    schema: dict
    description: str
    fn: Callable[..., dict]


TOOLS: dict[str, ToolSpec] = {
    "search_docs": ToolSpec(SEARCH_DOCS_SCHEMA, SEARCH_DOCS_DESCRIPTION, search_docs),
    "get_trace": ToolSpec(GET_TRACE_SCHEMA, GET_TRACE_DESCRIPTION, get_trace),
    "query_metrics": ToolSpec(QUERY_METRICS_SCHEMA, QUERY_METRICS_DESCRIPTION, query_metrics),
}


def dispatch_tool(name: str, arguments: dict[str, Any], deps: dict[str, Any] | None = None) -> dict:
    """Strip trace context, validate the remaining arguments, run the tool,
    and emit a TOOL_CALL span.

    `deps` injects test doubles (store/embedder/bm25_index) into search_docs
    without threading them through the wire-level arguments -- see
    services/mcp_server/tools.py's search_docs docstring. Production callers
    (server.py's call_tool handler) never pass deps.

    An optional "_trace_context": {"trace_id", "parent_span_id"} key in
    `arguments` is popped before validation/dispatch (it is transport
    metadata, not a tool input -- see module docstring) and used to open the
    TOOL_CALL span as a child of the caller's trace instead of rooting a new
    one. Absent, this span roots its own trace, same as before.
    """
    spec = TOOLS[name]
    arguments = dict(arguments)
    trace_context = arguments.pop("_trace_context", None) or {}

    with span(
        "TOOL_CALL",
        name,
        tool=name,
        trace_id=trace_context.get("trace_id"),
        parent_span_id=trace_context.get("parent_span_id"),
    ) as tspan:
        tspan.set(args_preview=json.dumps(arguments)[:_MAX_ARGS_PREVIEW])
        try:
            jsonschema.validate(instance=arguments, schema=spec.schema)
        except jsonschema.ValidationError as exc:
            result = {"error": f"invalid arguments for {name}: {exc.message}"}
        else:
            result = spec.fn(**arguments, **(deps or {}))
        tspan.set(result_size=len(json.dumps(result)))
    return result


def _session_ctx(session_id: str | None):
    """session(session_id) if given, else a no-op -- mirrors
    services/gateway/chat.py's session_ctx helper.
    """
    return session(session_id) if session_id else nullcontext(None)


server: Server = Server("agentlake-mcp")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(name=name, description=spec.description, inputSchema=spec.schema)
        for name, spec in TOOLS.items()
    ]


@server.call_tool(validate_input=False)
async def call_tool(name: str, arguments: dict[str, Any]) -> dict:
    # Set by services/agent when it spawns this process (see
    # services/agent/mcp_client.py) so this process's spans land in the same
    # session_id as the agent's. Trace context (trace_id/parent_span_id)
    # travels separately, per call, in arguments["_trace_context"] -- see
    # dispatch_tool() and docs/adr/ADR-003 #4.
    session_id = os.environ.get("AGENTLAKE_SESSION_ID")
    with _session_ctx(session_id):
        return dispatch_tool(name, arguments)


def warmup() -> bool:
    """Eagerly load the embedding model, touch Qdrant, and touch ClickHouse, so
    the first real tool call isn't billed those one-time costs.

    Recurrence of ADR-000 #3's finding: the first live RETRIEVAL span here
    measured 7.3s (fastembed's ONNX model load) against ~1s on every call
    after. Same fix, same shape -- move the one-time cost to process start.
    See ADR-003.

    The ClickHouse ping is the third instance of the same pattern (ADR-005 #5):
    without it the first query_metrics span would include a TCP handshake, and
    a metric describing latency would be the one thing in this repo lying about
    its own.

    Never raises: a warmup failure (Qdrant unreachable, model not cached yet,
    ClickHouse not running) just means the server starts anyway and the first
    real call pays the lazy-init cost, exactly like the pre-warmup behavior.
    """
    try:
        from services.mcp_server.clickhouse import ClickHouseClient
        from services.rag.embed import FastEmbedEmbedder
        from services.rag.qdrant_store import QdrantStore

        FastEmbedEmbedder().embed(["warmup"])
        QdrantStore().count_chunks()
        with ClickHouseClient() as clickhouse:
            clickhouse.ping()
        return True
    except Exception:
        logger.warning(
            "mcp_server warmup failed; the first tool call will pay the lazy-init cost",
            exc_info=True,
        )
        return False


async def run_stdio() -> None:
    warmup()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())
