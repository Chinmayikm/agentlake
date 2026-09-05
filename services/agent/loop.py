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
from services.agent.prompts import load_prompt
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

#: Which prompt template this agent is running. Stamped onto the AGENT_STEP span
#: here, and sent to the gateway as X-Prompt-Version so it reaches the LLM_CALL
#: span too -- which is the one that carries cost_usd and tokens, and therefore
#: the only one the "cost per turn by prompt version" dashboards can divide.
#: See ADR-007 #6.
#:
#: A constant, not a lookup: services/agent does not read the metadata database.
#: The text comes from services/agent/prompts/<version>.md and is published to
#: prompt_versions by scripts/load_prompts.py -- repo is source of truth, and
#: the check that the two agree lives downstream, in
#: lake.analytics.fct_cost_by_prompt, where a version the dimension has never
#: held shows up as prompt_attribution='unknown' rather than as a crash here.
#: That is deliberate: an agent that refused to run because a metadata row was
#: missing would make the telemetry a dependency of the thing it observes.
#:
#: v4, not v3, and the bump records a change in DELIVERY rather than wording:
#: v4's text is v3's verbatim, but until ADR-008 no `system` prompt was sent at
#: all, so v1/v2/v3 were labels on byte-identical requests. Same words actually
#: reaching the model is a different system, so it gets a different version.
DEFAULT_PROMPT_VERSION = "v4"

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


@dataclass(frozen=True, slots=True)
class RetrievedRef:
    """One chunk a `search_docs` call handed back, as the agent saw it.

    `section_path` keeps the TOOL's name for the field rather than
    `services.rag`'s `section`, because the tool result is what the agent
    actually received -- so every consumer downstream inherits one vocabulary
    and the rename stays in the single place it already lived,
    services/mcp_server/tools.py.

    `call_index` is which `search_docs` call produced it, 0-based. That is what
    makes "the first search_docs call" a computable thing, which matters
    because hit@k over the UNION of every call measures an agent that can
    brute-force the metric by searching five times, not the retriever.
    """

    chunk_id: str
    source_path: str
    section_path: str
    score: float
    call_index: int


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
    #: Every chunk every search_docs call returned, in call order. The only
    #: place chunk TEXT-adjacent identity is available in-process -- the
    #: RETRIEVAL span carries paths but the trace is a copy of this, not the
    #: other way round.
    retrieved: list[RetrievedRef] = field(default_factory=list)
    #: completion_tokens of the LAST gateway call. Exact, and free: a response
    #: containing tool_use always continues the loop, so the final response is
    #: text-only, and the forced-final path offers no tools. An eval measuring
    #: answer length wants this rather than len(text)//4, which measures a
    #: tokenizer nobody has.
    final_completion_tokens: int = 0
    #: The (k, mode) each search_docs call actually asked for. The model
    #: chooses these per call, so "what was the retriever configured with" has
    #: no single answer and this is the honest one.
    retrieval_calls: list[dict[str, Any]] = field(default_factory=list)


def _text_of(content: list[dict[str, Any]]) -> str:
    return "\n".join(block["text"] for block in content if block.get("type") == "text").strip()


def _collect_retrieval(
    tool_input: dict[str, Any],
    observation: object,
    call_index: int,
    retrieved: list[RetrievedRef],
    retrieval_calls: list[dict[str, Any]],
) -> None:
    """Record what one search_docs call asked for and got back.

    Defensive about shape on purpose: an error observation is a dict with no
    "results", and a tool failure is already serialized as {"error": ...} by
    the caller. Nothing here may raise -- a metrics-collection bug must not be
    able to fail a turn that otherwise succeeded.
    """
    retrieval_calls.append(
        {"k": tool_input.get("k"), "mode": tool_input.get("mode"), "call_index": call_index}
    )
    if not isinstance(observation, dict):
        return
    for result in observation.get("results") or []:
        if not isinstance(result, dict):
            continue
        retrieved.append(
            RetrievedRef(
                chunk_id=str(result.get("chunk_id", "")),
                source_path=str(result.get("source_path", "")),
                section_path=str(result.get("section_path", "")),
                score=float(result.get("score") or 0.0),
                call_index=call_index,
            )
        )


async def run_turn(
    question: str,
    *,
    gateway: GatewayClient,
    tool_executor: ToolExecutor,
    session_id: str | None = None,
    model_alias: str = "fast",
    max_steps: int = DEFAULT_MAX_STEPS,
    tool_timeout: float = DEFAULT_TOOL_TIMEOUT,
    prompt_version: str = DEFAULT_PROMPT_VERSION,
) -> AgentResult:
    with (
        session(session_id) as sid,
        span("AGENT_STEP", "agent_turn", prompt_version=prompt_version) as step,
    ):
        system_prompt = load_prompt(prompt_version)
        messages: list[dict[str, Any]] = [{"role": "user", "content": question}]
        tools_called: list[str] = []
        retrieved: list[RetrievedRef] = []
        retrieval_calls: list[dict[str, Any]] = []
        total_tokens = 0
        total_cost_usd = 0.0
        final_completion_tokens = 0
        steps_used = 0
        truncated = False
        answer = ""

        for _ in range(max_steps):
            steps_used += 1
            resp = await gateway.chat(
                ChatRequest(
                    messages=messages,
                    model_alias=model_alias,
                    tools=TOOL_DEFS,
                    system=system_prompt,
                ),
                session_id=sid,
                prompt_version=prompt_version,
            )
            total_tokens += resp.usage.prompt_tokens + resp.usage.completion_tokens
            total_cost_usd += resp.usage.cost_usd
            final_completion_tokens = resp.usage.completion_tokens
            messages.append({"role": "assistant", "content": resp.content})

            uses = [b for b in resp.content if b.get("type") == "tool_use"]
            if not uses:
                answer = _text_of(resp.content)
                break

            tool_results = []
            for use in uses:
                name = use["name"]
                observation: object = None
                try:
                    observation = await asyncio.wait_for(
                        tool_executor.call_tool(name, use["input"]), timeout=tool_timeout
                    )
                    content_text = json.dumps(observation)
                except Exception as exc:
                    content_text = json.dumps({"error": str(exc)})
                if name == "search_docs":
                    _collect_retrieval(
                        use.get("input") or {},
                        observation,
                        len(retrieval_calls),
                        retrieved,
                        retrieval_calls,
                    )
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
                # system, still: a truncated turn losing its system prompt
                # would silently change what is being measured, exactly at the
                # point the turn is already degraded.
                ChatRequest(
                    messages=messages,
                    model_alias=model_alias,
                    tools=None,
                    system=system_prompt,
                ),
                session_id=sid,
                prompt_version=prompt_version,
            )
            total_tokens += resp.usage.prompt_tokens + resp.usage.completion_tokens
            total_cost_usd += resp.usage.cost_usd
            final_completion_tokens = resp.usage.completion_tokens
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
        retrieved=retrieved,
        final_completion_tokens=final_completion_tokens,
        retrieval_calls=retrieval_calls,
    )
