"""The bounded agent loop: run_turn().

Anthropic-style tool use through the gateway (ADR-001's single door) --
tool_use content blocks come back from the model, tool_result blocks go back
in. Tool execution is delegated to a ToolExecutor (see mcp_client.py); a
failure there (timeout, error, an honest stub) always becomes a tool_result
observation, never a crash. Budget exhaustion forces one final untooled call
instead of returning nothing. See docs/adr/ADR-003.

No span is opened here around each tool call: the MCP server's own TOOL_CALL
span (services/mcp_server/server.py's dispatch_tool()) is the authoritative
one, joined to this trace via the _trace_context sidecar mcp_client.py sends
(ADR-003 #4). Opening a second, client-side TOOL_CALL span here would nest
one under the other instead of both under AGENT_STEP.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

from services.agent.gateway_client import GatewayClient
from services.agent.mcp_client import ToolExecutor
from services.gateway.chat import ChatRequest
from services.mcp_server.schemas import (
    GET_TRACE_DESCRIPTION,
    GET_TRACE_SCHEMA,
    QUERY_METRICS_DESCRIPTION,
    QUERY_METRICS_SCHEMA,
    SEARCH_DOCS_DESCRIPTION,
    SEARCH_DOCS_SCHEMA,
)
from services.sdk import session, span

DEFAULT_MAX_STEPS = 8
DEFAULT_TOOL_TIMEOUT = 15.0

# Same schemas/descriptions the MCP server advertises via list_tools() --
# one source of truth for what each tool does and accepts.
TOOL_DEFS: list[dict[str, Any]] = [
    {
        "name": "search_docs",
        "description": SEARCH_DOCS_DESCRIPTION,
        "input_schema": SEARCH_DOCS_SCHEMA,
    },
    {
        "name": "get_trace",
        "description": GET_TRACE_DESCRIPTION,
        "input_schema": GET_TRACE_SCHEMA,
    },
    {
        "name": "query_metrics",
        "description": QUERY_METRICS_DESCRIPTION,
        "input_schema": QUERY_METRICS_SCHEMA,
    },
]


@dataclass(slots=True)
class AgentResult:
    answer: str
    truncated: bool
    steps_used: int
    tools_called: list[str] = field(default_factory=list)
    session_id: str = ""
    trace_id: str = ""
    total_tokens: int = 0
    total_cost_usd: float = 0.0


def _text_of(content: list[dict[str, Any]]) -> str:
    return "\n".join(block["text"] for block in content if block.get("type") == "text").strip()


async def run_turn(
    question: str,
    *,
    gateway: GatewayClient,
    tool_executor: ToolExecutor,
    session_id: str | None = None,
    model_alias: str = "fast",
    max_steps: int = DEFAULT_MAX_STEPS,
    tool_timeout: float = DEFAULT_TOOL_TIMEOUT,
) -> AgentResult:
    with session(session_id) as sid, span("AGENT_STEP", "agent_turn") as step:
        messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
        tools_called: list[str] = []
        total_tokens = 0
        total_cost_usd = 0.0
        steps_used = 0
        truncated = False
        answer = ""

        for _ in range(max_steps):
            steps_used += 1
            resp = await gateway.chat(
                ChatRequest(messages=messages, model_alias=model_alias, tools=TOOL_DEFS),
                session_id=sid,
            )
            total_tokens += resp.usage.prompt_tokens + resp.usage.completion_tokens
            total_cost_usd += resp.usage.cost_usd
            messages.append({"role": "assistant", "content": resp.content})

            uses = [b for b in resp.content if b.get("type") == "tool_use"]
            if not uses:
                answer = _text_of(resp.content)
                break

            tool_results = []
            for use in uses:
                name = use["name"]
                try:
                    observation = await asyncio.wait_for(
                        tool_executor.call_tool(name, use["input"]), timeout=tool_timeout
                    )
                    content_text = json.dumps(observation)
                except Exception as exc:
                    content_text = json.dumps({"error": str(exc)})
                tools_called.append(name)
                tool_results.append(
                    {"type": "tool_result", "tool_use_id": use["id"], "content": content_text}
                )
            messages.append({"role": "user", "content": tool_results})
        else:
            # Budget exhausted without a final text answer: one forced call,
            # no tools offered, so the model can't do anything but answer.
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Tool budget exhausted -- give your best final answer now "
                        "based on what you've gathered so far, and note the truncation."
                    ),
                }
            )
            resp = await gateway.chat(
                ChatRequest(messages=messages, model_alias=model_alias, tools=None),
                session_id=sid,
            )
            total_tokens += resp.usage.prompt_tokens + resp.usage.completion_tokens
            total_cost_usd += resp.usage.cost_usd
            answer = _text_of(resp.content) or (
                "(no answer produced before the tool budget was exhausted)"
            )
            truncated = True

        step.set(steps_used=steps_used, tools_called=",".join(tools_called), truncated=truncated)
        trace_id = step.trace_id

    return AgentResult(
        answer=answer,
        truncated=truncated,
        steps_used=steps_used,
        tools_called=tools_called,
        session_id=sid,
        trace_id=trace_id,
        total_tokens=total_tokens,
        total_cost_usd=total_cost_usd,
    )
