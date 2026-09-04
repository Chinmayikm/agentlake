"""Tests for services/agent's loop against a scripted fake gateway and a fake
ToolExecutor -- no network, no real LLM, no MCP subprocess. Assertions are
written against docs/adr/ADR-003's spec, not the implementation.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from services.agent.loop import DEFAULT_MAX_STEPS, DEFAULT_PROMPT_VERSION, run_turn
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
        # What the loop passed alongside each request. prompt_version travels as
        # a header on the real client rather than in the body (ADR-007 #6), so
        # recording it here is the only way an in-process test can see that the
        # gateway would have been told.
        self.prompt_versions: list[str | None] = []

    async def chat(
        self,
        request,
        *,
        session_id: str | None = None,
        prompt_version: str | None = None,
    ):
        self.requests.append(request)
        self.prompt_versions.append(prompt_version)
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


# ---------------------------------------------------------------------------
# 5. prompt_version (ADR-007 #6)
# ---------------------------------------------------------------------------


def test_prompt_version_is_stamped_on_agent_step_and_sent_to_the_gateway(events) -> None:
    """The one-attribute promise, both halves of it.

    AGENT_STEP carries the version so a turn is self-describing. But AGENT_STEP
    rows have NULL cost_usd and NULL tokens by contract, so a dashboard dividing
    cost by turns would render zero from them alone -- the version has to reach
    the LLM_CALL span, which lives in services/gateway. This process cannot
    observe that span (the gateway is another process), so what is asserted here
    is the half services/agent owns: that every outbound call carried the
    version. tests/test_gateway.py asserts the other half.
    """
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("tu1", "search_docs", {"query": "x"})], stop_reason="tool_use"),
            _resp([_text_block("done")]),
        ]
    )

    asyncio.run(
        run_turn(
            "q",
            gateway=gateway,
            tool_executor=FakeToolExecutor(),
            session_id="s-pv",
        )
    )

    agent_steps = [e for e in events if e["event_type"] == "AGENT_STEP"]
    assert agent_steps[0]["attributes"]["prompt_version"] == DEFAULT_PROMPT_VERSION

    # Every call, not just the first: the forced final call after budget
    # exhaustion is a separate call site and has been forgotten before.
    assert gateway.prompt_versions == [DEFAULT_PROMPT_VERSION, DEFAULT_PROMPT_VERSION]


def test_prompt_version_is_overridable_and_reaches_every_call(events) -> None:
    """--prompt-version is what lets one run be attributed to a new template
    without a redeploy, which is how ADR-007's verification log gets a v4 row
    into fct_cost_by_prompt."""
    gateway = FakeGateway([_resp([_text_block("done")])])

    asyncio.run(
        run_turn(
            "q",
            gateway=gateway,
            tool_executor=FakeToolExecutor(),
            session_id="s-pv2",
            prompt_version="v4",
        )
    )

    agent_steps = [e for e in events if e["event_type"] == "AGENT_STEP"]
    assert agent_steps[0]["attributes"]["prompt_version"] == "v4"
    assert gateway.prompt_versions == ["v4"]


def test_default_prompt_version_matches_the_seeded_metadata_row() -> None:
    """DEFAULT_PROMPT_VERSION has to name a row metadata/sql/07_seed.sql
    actually inserts, or every span the agent emits lands in
    fct_cost_by_prompt as prompt_attribution='unknown' -- which is the state
    that is supposed to mean the CDC lander has not run.

    A file-level check on purpose: the agent deliberately does not read the
    metadata database (see loop.py), so nothing at runtime would ever notice.
    """
    seed = (
        Path(__file__).resolve().parents[1] / "metadata" / "sql" / "07_seed.sql"
    ).read_text(encoding="utf-8")
    assert f"'{DEFAULT_PROMPT_VERSION}'," in seed, (
        f"DEFAULT_PROMPT_VERSION={DEFAULT_PROMPT_VERSION!r} is not seeded in "
        "metadata/sql/07_seed.sql"
    )
