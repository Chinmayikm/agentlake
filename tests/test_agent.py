"""Tests for services/agent's loop against a scripted fake gateway and a fake
ToolExecutor -- no network, no real LLM, no MCP subprocess. Assertions are
written against docs/adr/ADR-003's spec, not the implementation.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any

import pytest

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


def test_default_prompt_version_has_a_prompt_file() -> None:
    """DEFAULT_PROMPT_VERSION has to name a file in services/agent/prompts/, or
    every turn raises UnknownPromptVersion before it reaches the gateway.

    The prompt files are the source of truth; scripts/load_prompts.py publishes
    them into prompt_versions. Whether the DATABASE has caught up is checked
    downstream, in lake.analytics.fct_cost_by_prompt, where a version the
    dimension has never held shows up as prompt_attribution='unknown' -- the
    agent deliberately does not read the metadata database (see loop.py).
    """
    from services.agent.prompts import available_versions, load_prompt

    assert DEFAULT_PROMPT_VERSION in available_versions()
    assert load_prompt(DEFAULT_PROMPT_VERSION).strip()


def test_promoted_prompt_files_still_match_the_metadata_seed() -> None:
    """v1-v3 were promoted verbatim out of metadata/sql/07_seed.sql. If the two
    drift, `make prompts-load` silently rewrites history: the file wins, the
    seeded row is overwritten, and every eval_result already attributed to that
    version was scored against text the database no longer holds.

    v4 and v5 are deliberately absent from the seed -- they are the loader's,
    not the migration's, and this test says so by only checking v1-v3.
    """
    from services.agent.prompts import load_prompt

    seed = (
        Path(__file__).resolve().parents[1] / "metadata" / "sql" / "07_seed.sql"
    ).read_text(encoding="utf-8")
    # The seed wraps long templates across adjacent SQL literals, which the
    # parser concatenates. Join them back before comparing, or this test would
    # be asserting on line-wrapping rather than on text.
    seed = re.sub(r"'\s*\n\s*'", "", seed)

    for version in ("v1", "v2", "v3"):
        # SQL string literals double their single quotes; the file does not.
        assert load_prompt(version).replace("'", "''") in seed, (
            f"services/agent/prompts/{version}.md no longer matches the "
            f"template_text seeded for {version} in metadata/sql/07_seed.sql"
        )


def test_v4_is_v3s_text_and_v5_drops_the_instructions_the_eval_scores() -> None:
    """The gate demo depends on v5 being WORSE for a stated reason, not just
    different. v4 is v3 verbatim -- the bump records that the text now actually
    reaches the model -- and v5 removes exactly the three instructions the
    harness measures: search first (hit@5), cite sources (citation_ok), and
    admit when the corpus does not answer (faithfulness on the unanswerable
    examples). If v5 stops differing in those, a green gate proves nothing.
    """
    from services.agent.prompts import load_prompt

    assert load_prompt("v4") == load_prompt("v3")

    v5 = load_prompt("v5")
    for instruction in (
        "Search before answering anything factual",
        "Cite the source of every claim",
        "say plainly when the corpus does not answer",
    ):
        assert instruction in load_prompt("v4")
        assert instruction not in v5


# ---------------------------------------------------------------------------
# (7) The system prompt, and what the eval harness reads off a turn (ADR-008)
# ---------------------------------------------------------------------------


def test_every_gateway_call_carries_the_system_prompt() -> None:
    """Before ADR-008 no system prompt was sent at all, so v1/v2/v3 produced
    byte-identical requests and `prompt_version` described nothing. If this
    regresses, the eval's whole prompt A/B silently compares a label to itself.
    """
    from services.agent.prompts import load_prompt

    gateway = FakeGateway(
        [
            _resp([_tool_use_block("t1", "search_docs", {"query": "q"})]),
            _resp([_text_block("done")]),
        ]
    )
    asyncio.run(
        run_turn("q", gateway=gateway, tool_executor=FakeToolExecutor(), prompt_version="v4")
    )

    assert len(gateway.requests) == 2
    assert all(r.system == load_prompt("v4") for r in gateway.requests)


def test_the_forced_final_call_keeps_its_system_prompt() -> None:
    """Budget exhaustion drops `tools`, deliberately -- but not `system`. A
    truncated turn silently switching to a different system prompt would change
    what is being measured exactly at the point the turn is already degraded.
    """
    from services.agent.prompts import load_prompt

    tool_calls = [
        _resp([_tool_use_block(f"t{i}", "search_docs", {"query": "q"})]) for i in range(3)
    ]
    gateway = FakeGateway([*tool_calls, _resp([_text_block("partial")])])

    result = asyncio.run(
        run_turn(
            "q",
            gateway=gateway,
            tool_executor=FakeToolExecutor(),
            max_steps=3,
            prompt_version="v5",
        )
    )

    assert result.truncated is True
    final = gateway.requests[-1]
    assert final.tools is None
    assert final.system == load_prompt("v5")


def test_run_turn_records_what_search_docs_returned() -> None:
    """The harness computes hit@k and citation_ok from this. It is what the
    agent actually received, so the RETRIEVAL span is a copy of it rather than
    the other way round -- which is why CI can score a run with no Kafka and no
    ClickHouse anywhere.
    """
    observation = {
        "results": [
            {
                "chunk_id": "c1",
                "source_path": "docs/design.html",
                "section_path": "Design > Persistence",
                "score": 0.9,
            },
            {
                "chunk_id": "c2",
                "source_path": "docs/ops.html",
                "section_path": "Operations",
                "score": 0.5,
            },
        ]
    }
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("t1", "search_docs", {"query": "q", "k": 5, "mode": "hybrid"})]),
            _resp([_text_block("done")]),
        ]
    )

    result = asyncio.run(
        run_turn("q", gateway=gateway, tool_executor=FakeToolExecutor(observation))
    )

    assert [r.chunk_id for r in result.retrieved] == ["c1", "c2"]
    assert [r.source_path for r in result.retrieved] == ["docs/design.html", "docs/ops.html"]
    assert result.retrieved[0].section_path == "Design > Persistence"
    assert all(r.call_index == 0 for r in result.retrieved)
    assert result.retrieval_calls == [{"k": 5, "mode": "hybrid", "call_index": 0}]


def test_call_index_separates_successive_search_docs_calls() -> None:
    """hit@k is scored on the FIRST search_docs call. Without call_index the
    metric would union every call, and an agent could brute-force it by
    searching five times -- measuring the agent, not the retriever."""
    observation = {"results": [{"chunk_id": "c1", "source_path": "a.md", "section_path": "S"}]}
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("t1", "search_docs", {"query": "one"})]),
            _resp([_tool_use_block("t2", "search_docs", {"query": "two"})]),
            _resp([_text_block("done")]),
        ]
    )

    result = asyncio.run(
        run_turn("q", gateway=gateway, tool_executor=FakeToolExecutor(observation))
    )

    assert [r.call_index for r in result.retrieved] == [0, 1]
    assert len([r for r in result.retrieved if r.call_index == 0]) == 1


def test_a_failed_search_docs_call_is_recorded_as_a_call_with_no_results() -> None:
    """'the retriever missed' and 'the tool broke' must not look identical. The
    call is counted so search_rate stays honest; it contributes no chunks, so
    hit@k does not credit it."""
    gateway = FakeGateway(
        [
            _resp([_tool_use_block("t1", "search_docs", {"query": "q"})]),
            _resp([_text_block("done")]),
        ]
    )

    result = asyncio.run(
        run_turn(
            "q",
            gateway=gateway,
            tool_executor=FakeToolExecutor(raises=RuntimeError("qdrant down")),
        )
    )

    assert result.retrieved == []
    assert len(result.retrieval_calls) == 1


def test_final_completion_tokens_is_the_last_calls_completion_tokens() -> None:
    """answer_len_tokens comes from here rather than len(text)//4, which
    measures a tokenizer nobody has -- and it is the x-axis of the judge's
    length-control correlation, so an estimate would put noise into the one
    number the bias check depends on."""
    tool_step = _resp([_tool_use_block("t1", "search_docs", {"query": "q"})])
    final = _resp([_text_block("the answer")])
    final.usage.completion_tokens = 137
    gateway = FakeGateway([tool_step, final])

    result = asyncio.run(
        run_turn("q", gateway=gateway, tool_executor=FakeToolExecutor())
    )

    assert result.final_completion_tokens == 137
    assert result.total_tokens == 10 + 5 + 10 + 137


def test_unknown_prompt_version_fails_before_any_gateway_call() -> None:
    """A typo'd --prompt-version must not spend money and then be discovered in
    a dashboard as prompt_attribution='unknown'."""
    from services.agent.prompts import UnknownPromptVersion

    gateway = FakeGateway([_resp([_text_block("never reached")])])

    with pytest.raises(UnknownPromptVersion, match="v99"):
        asyncio.run(
            run_turn(
                "q", gateway=gateway, tool_executor=FakeToolExecutor(), prompt_version="v99"
            )
        )

    assert gateway.requests == []


def test_prompt_params_sidecar_covers_every_prompt_file() -> None:
    """A version with no params entry publishes provenance and nothing else, so
    the dimension cannot say what that prompt was FOR. Cheap to keep in step;
    invisible if it drifts."""
    from services.agent.prompts import available_versions, load_params

    assert sorted(load_params()) == available_versions()


def test_prompt_params_match_the_seeded_metadata_rows() -> None:
    """v1-v3's params were seeded by metadata/sql/07_seed.sql before params.yaml
    existed. scripts/load_prompts.py merges rather than replaces, so if the two
    disagree the database ends up holding a union of two different intents and
    neither file is the source of truth any more."""
    import json

    from services.agent.prompts import load_params

    seed = (
        Path(__file__).resolve().parents[1] / "metadata" / "sql" / "07_seed.sql"
    ).read_text(encoding="utf-8")
    params = load_params()

    for version in ("v1", "v2", "v3"):
        for key, value in params[version].items():
            literal = json.dumps(value)
            assert f'"{key}": {literal}' in seed, (
                f"params.yaml says {version}.{key}={value!r}, which is not what "
                f"metadata/sql/07_seed.sql seeds"
            )


def test_the_degraded_prompt_is_flagged_as_such() -> None:
    """v5 exists only to make the regression gate go red. Nothing stops someone
    running `--prompt-version v5` for real, so the dimension has to carry the
    fact that this one is deliberately bad -- otherwise a quality drop looks
    like a mystery rather than a label somebody ignored."""
    from services.agent.prompts import load_params

    params = load_params()

    assert params["v5"]["degraded"] is True
    assert not any(p.get("degraded") for v, p in params.items() if v != "v5")
