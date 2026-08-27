"""ToolExecutor -- the agent's MCP client. The loop only depends on the
ToolExecutor protocol (see loop.py), so tests inject a fake and never spawn a
subprocess; StdioToolExecutor is the one production implementation, and the
one place in services/agent that imports mcp.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import AsyncExitStack
from typing import Any, Protocol

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from services.sdk import current_parent_span_id, current_trace_id


class ToolExecutor(Protocol):
    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict: ...


class StdioToolExecutor:
    """Spawns `python -m services.mcp_server` and talks real MCP over stdio.

    session_id, if given, is passed as AGENTLAKE_SESSION_ID so the server
    subprocess's own spans land in the same session_id as the agent's.
    Trace context (trace_id/parent_span_id) travels separately, per call --
    see call_tool() below and ADR-003 #4.
    """

    def __init__(self, session_id: str | None = None) -> None:
        self._session_id = session_id
        self._stack = AsyncExitStack()
        self._session: ClientSession | None = None

    async def __aenter__(self) -> StdioToolExecutor:
        env = dict(os.environ)
        if self._session_id:
            env["AGENTLAKE_SESSION_ID"] = self._session_id
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "services.mcp_server"], env=env
        )
        read, write = await self._stack.enter_async_context(stdio_client(params))
        self._session = await self._stack.enter_async_context(ClientSession(read, write))
        await self._session.initialize()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._stack.aclose()

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> dict:
        assert self._session is not None, "StdioToolExecutor used outside `async with`"
        call_args = dict(arguments)
        # Cross-process trace propagation (ADR-003 #4): the server subprocess
        # has no contextvars of its own, so this is the only way its
        # TOOL_CALL/RETRIEVAL spans can join our trace instead of rooting a
        # new one. Stripped server-side before validation/dispatch -- see
        # services/mcp_server/server.py's dispatch_tool().
        trace_id = current_trace_id()
        if trace_id:
            call_args["_trace_context"] = {
                "trace_id": trace_id,
                "parent_span_id": current_parent_span_id(),
            }
        result = await self._session.call_tool(name, call_args)
        if result.structuredContent is not None:
            return result.structuredContent
        first = result.content[0] if result.content else None
        text = first.text if first is not None and hasattr(first, "text") else None
        if text is not None:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return {"error": text}
        return {"error": "tool call failed" if result.isError else "tool returned no content"}
