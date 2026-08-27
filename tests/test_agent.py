"""Tests for services/agent's loop against a scripted fake gateway and a fake
ToolExecutor -- no network, no real LLM, no MCP subprocess. Assertions are
written against docs/adr/ADR-003's spec, not the implementation.
"""

from __future__ import annotations

import asyncio
from typing import Any

from services.agent.loop import DEFAULT_MAX_STEPS, run_turn
from services.gateway.chat import ChatResponse, UsageOut
from services.sdk import current_parent_span_id, current_trace_id


def _resp(content: list[dict[str, Any]], stop_reason: str = "end_turn") -> ChatResponse:
    return ChatResponse(
        id="msg_test",
        model="claude-haiku-4-5",
        role="assistant",
        content=content,
        stop_reason=stop_reason,
        usage=UsageOut(prompt_tokens=10, completion_tokens=5, cost_usd=0.001, latency_ms=1.0),
    )


def _text_block(text: str) -> dict[str, Any]:
    return {"type": "text", "text": text}


def _tool_use_block(tool_id: str, name: str, arguments: dict) -> dict[str, Any]:
    return {"type": "tool_use", "id": tool_id, "name": name, "input": arguments}


class FakeGateway:
    """Pops one scripted ChatResponse per call; records every request sent."""

    def __init__(self, responses: list[ChatResponse]) -> None:
        self._responses = list(responses)
        self.requests: list[Any] = []

    async def chat(self, request, *, session_id: str | None = None):
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("FakeGateway ran out of scripted responses")
        return self._responses.pop(0)


class FakeToolExecutor:
    def __init__(
        self, result: dict | None = None, *, delay: float = 0.0, raises: Exception | None = None
    ):
        self._result = result or {"results": []}
        self._delay = delay
        self._raises = raises
        self.calls: list[tuple[str, dict]] = []
        # What the agent's ambient trace context looked like at call time --
        # this is exactly what a real StdioToolExecutor would read via
        # current_trace_id()/current_parent_span_id() to build _trace_context
        # (see services/agent/mcp_client.py, ADR-003 #4).
        self.trace_contexts: list[tuple[str | None, str | None]] = []

    async def call_tool(self, name: str, arguments: dict) -> dict:
        self.calls.append((name, arguments))
        self.trace_contexts.append((current_trace_id(), current_parent_span_id()))
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises:
            raise self._raises
        return self._result


# ---------------------------------------------------------------------------
# (1) Happy path
# ---------------------------------------------------------------------------


def test_happy_path_executes_tool_then_answers(events) -> None:
    gateway = FakeGateway(
        [
            _resp(
                [_tool_use_block("tu1", "search_docs", {"query": "retention"})],
                stop_reason="tool_use",
            ),
            _resp([_text_block("log.retention.hours controls how long logs are kept.")]),
        ]
    )
    executor = FakeToolExecutor(result={"results": [{"text": "retention doc"}]})

    result = asyncio.run(
        run_turn("what does log.retention.hours do?", gateway=gateway, tool_executor=executor)
    )

    assert result.answer == "log.retention.hours controls how long logs are kept."
    assert result.truncated is False
    assert result.steps_used == 2
    assert result.tools_called == ["search_docs"]
    assert executor.calls == [("search_docs", {"query": "retention"})]

    # the observation was fed back as a tool_result in the next request
    second_request = gateway.requests[1]
    tool_result_message = second_request.messages[-1]
    assert tool_result_message.role == "user"
    assert tool_result_message.content[0]["type"] == "tool_result"
    assert tool_result_message.content[0]["tool_use_id"] == "tu1"


# ---------------------------------------------------------------------------
# (2) Budget exhaustion
# ---------------------------------------------------------------------------


def test_budget_exhaustion_forces_a_final_answer(events) -> None:
    always_tool_use = [
        _resp([_tool_use_block(f"tu{i}", "search_docs", {"query": "x"})], stop_reason="tool_use")
        for i in range(DEFAULT_MAX_STEPS)
    ]
    forced_final = _resp([_text_block("Best guess based on what I found.")])
    gateway = FakeGateway([*always_tool_use, forced_final])
    executor = FakeToolExecutor(result={"results": []})

    result = asyncio.run(run_turn("a hard question", gateway=gateway, tool_executor=executor))

    assert result.truncated is True
    assert result.answer == "Best guess based on what I found."
    assert result.steps_used == DEFAULT_MAX_STEPS
    # one call per step plus exactly one forced final call
    assert len(gateway.requests) == DEFAULT_MAX_STEPS + 1
    # the forced final call offers no tools, so the model can't keep stalling
    assert gateway.requests[-1].tools is None


# ---------------------------------------------------------------------------
# (3) Tool timeout becomes an observation, not a crash
# ---------------------------------------------------------------------------


def test_tool_timeout_becomes_an_observation(events) -> None:
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("tu1", "search_docs", {"query": "x"})], stop_reason="tool_use"),
            _resp([_text_block("I couldn't retrieve that in time, but here's what I know.")]),
        ]
    )
    slow_executor = FakeToolExecutor(delay=1.0)

    result = asyncio.run(
        run_turn(
            "question",
            gateway=gateway,
            tool_executor=slow_executor,
            tool_timeout=0.01,
        )
    )

    assert result.truncated is False
    assert result.answer == "I couldn't retrieve that in time, but here's what I know."
    second_request = gateway.requests[1]
    observation = second_request.messages[-1].content[0]["content"]
    assert "error" in observation


# ---------------------------------------------------------------------------
# (4) Span tree
# ---------------------------------------------------------------------------


def test_span_tree_and_session_propagation(events) -> None:
    """The agent opens exactly one span, AGENT_STEP -- no client-side
    TOOL_CALL (see loop.py's module docstring): the MCP server's own
    TOOL_CALL is the authoritative one, joined to this trace via the
    _trace_context a real StdioToolExecutor sends (ADR-003 #4). What this
    test *can* verify in-process is the invariant that makes that join
    correct: at the moment the loop calls tool_executor.call_tool(), the
    ambient trace context is exactly AGENT_STEP's own (trace_id, span_id) --
    proven via FakeToolExecutor.trace_contexts, captured with the same
    current_trace_id()/current_parent_span_id() accessors mcp_client.py uses.
    """
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("tu1", "search_docs", {"query": "x"})], stop_reason="tool_use"),
            _resp([_text_block("done")]),
        ]
    )
    executor = FakeToolExecutor()

    result = asyncio.run(
        run_turn("q", gateway=gateway, tool_executor=executor, session_id="fixed-session")
    )

    assert result.session_id == "fixed-session"

    agent_steps = [e for e in events if e["event_type"] == "AGENT_STEP"]
    assert len(agent_steps) == 1
    assert agent_steps[0]["parent_span_id"] is None
    assert agent_steps[0]["trace_id"] == result.trace_id
    assert [e["event_type"] for e in events] == ["AGENT_STEP"]

    assert executor.trace_contexts == [(result.trace_id, agent_steps[0]["span_id"])]

    for event in events:
        assert event["session_id"] == "fixed-session"
