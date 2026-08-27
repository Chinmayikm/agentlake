"""Tests for services/sdk/telemetry.py.

Assertions are written against the SDK's stated requirements, not against
whatever the implementation currently does. Where a requirement leaves a detail
open (how a bool stringifies, how long a truncated message is), the test asserts
the requirement and a separate test pins the documented convention.

No test opens a network connection: conftest injects a list-collector emitter
and blocks the Kafka path outright.
"""

import asyncio
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

import fastavro
import pytest

from services.sdk import TraceEvent, session, span, telemetry

REPO_ROOT = Path(__file__).resolve().parent.parent


def run_two_turns(session_id: str = "conv-1") -> None:
    """The canonical shape: one session, two turns, each with two children."""
    with session(session_id):
        for turn in (1, 2):
            with span("AGENT_STEP", "agent_turn", turn=turn):
                with span("RETRIEVAL", "vector_search", top_k=4) as retrieval:
                    retrieval.set(hits=4)
                with span("LLM_CALL", "chat_completion") as llm:
                    llm.set(
                        model="claude-haiku-4-5",
                        prompt_tokens=120,
                        completion_tokens=45,
                        cost_usd=0.0012,
                    )


def names(events: list[TraceEvent]) -> list[str]:
    return [e["attributes"]["name"] for e in events]


# ---------------------------------------------------------------------------
# 1. Contract conformance
# ---------------------------------------------------------------------------


def test_emitted_events_validate_against_avsc(
    events: list[TraceEvent], avro_schema: Any
) -> None:
    run_two_turns()
    assert events, "no events emitted"
    for event in events:
        assert fastavro.validate(event, avro_schema), event


def test_event_keys_match_schema_fields(
    events: list[TraceEvent], schema_field_names: set[str]
) -> None:
    """Compared against the .avsc itself, not against TraceEvent's annotations."""
    run_two_turns()
    for event in events:
        assert set(event.keys()) == schema_field_names


def test_error_events_also_match_the_contract(
    events: list[TraceEvent], avro_schema: Any
) -> None:
    """The error path adds attributes; it must not break the contract."""
    with pytest.raises(ValueError):
        with session("s-err"):
            with span("TOOL_CALL", "lookup"):
                raise ValueError("boom")
    assert fastavro.validate(events[0], avro_schema)


# ---------------------------------------------------------------------------
# 2. Nesting
# ---------------------------------------------------------------------------


def test_child_carries_parent_span_id(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("AGENT_STEP", "agent_turn"):
            with span("LLM_CALL", "chat_completion"):
                pass
    child, parent = events  # children emit first
    assert child["parent_span_id"] == parent["span_id"]


def test_nested_spans_share_one_trace_id(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("AGENT_STEP", "agent_turn"):
            with span("RETRIEVAL", "vector_search"):
                with span("LLM_CALL", "chat_completion"):
                    pass
    assert len(events) == 3
    assert len({e["trace_id"] for e in events}) == 1


def test_root_span_has_no_parent(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("AGENT_STEP", "agent_turn"):
            with span("LLM_CALL", "chat_completion"):
                pass
    roots = [e for e in events if e["parent_span_id"] is None]
    assert len(roots) == 1
    assert roots[0]["attributes"]["name"] == "agent_turn"


# ---------------------------------------------------------------------------
# 3. Per-turn trace scope
# ---------------------------------------------------------------------------


def test_two_turns_one_session_get_two_traces_one_session_id(
    events: list[TraceEvent],
) -> None:
    """session = one conversation, trace = one turn's causal graph."""
    run_two_turns("conv-1")

    assert {e["session_id"] for e in events} == {"conv-1"}
    assert len({e["trace_id"] for e in events}) == 2

    turn1, turn2 = events[0:3], events[3:6]
    assert len({e["trace_id"] for e in turn1}) == 1
    assert len({e["trace_id"] for e in turn2}) == 1
    assert turn1[0]["trace_id"] != turn2[0]["trace_id"]


# ---------------------------------------------------------------------------
# 4. Emission order
# ---------------------------------------------------------------------------


def test_children_emit_before_parents(events: list[TraceEvent]) -> None:
    """The inner finally runs first, so a consumer sees children before parents."""
    with session("s1"):
        with span("AGENT_STEP", "agent_turn"):
            with span("RETRIEVAL", "vector_search"):
                pass
            with span("LLM_CALL", "chat_completion"):
                pass
    assert names(events) == ["vector_search", "chat_completion", "agent_turn"]


# ---------------------------------------------------------------------------
# 5. Error path
# ---------------------------------------------------------------------------


def test_raising_span_records_error_and_reraises(events: list[TraceEvent]) -> None:
    with pytest.raises(ValueError) as excinfo:
        with session("s1"):
            with span("TOOL_CALL", "lookup_order"):
                raise ValueError("orders_api timed out")

    assert str(excinfo.value) == "orders_api timed out"  # original, not wrapped
    event = events[0]
    assert event["status"] == "error"
    assert event["attributes"]["error_class"] == "ValueError"
    assert event["attributes"]["error_message"] == "orders_api timed out"
    assert event["latency_ms"] > 0


def test_error_message_is_truncated(events: list[TraceEvent]) -> None:
    long_message = "x" * 5000
    with pytest.raises(RuntimeError):
        with session("s1"):
            with span("TOOL_CALL", "lookup_order"):
                raise RuntimeError(long_message)

    recorded = events[0]["attributes"]["error_message"]
    assert len(recorded) < len(long_message), "error_message was not truncated"
    assert long_message.startswith(recorded), "truncation must keep the prefix"


def test_span_emits_even_when_body_raises(events: list[TraceEvent]) -> None:
    """The event fires from the finally block, so a crash still produces telemetry."""
    with pytest.raises(ZeroDivisionError):
        with session("s1"):
            with span("AGENT_STEP", "agent_turn"):
                1 / 0
    assert len(events) == 1


# ---------------------------------------------------------------------------
# 6. Masking protection
# ---------------------------------------------------------------------------


def test_emitter_error_does_not_mask_body_exception() -> None:
    """A telemetry fault must never replace the application's own exception.

    The emit runs in span()'s finally; without the guard the emitter's error
    would become the propagating exception and the real bug would be demoted to
    __context__.
    """

    def broken(event: TraceEvent) -> None:
        raise RuntimeError("schema registry unreachable")

    previous = telemetry._EMITTER
    telemetry.configure(broken)
    try:
        with pytest.raises(ValueError) as excinfo:
            with session("s1"):
                with span("TOOL_CALL", "lookup_order"):
                    raise ValueError("the real application error")
    finally:
        telemetry._EMITTER = previous

    assert type(excinfo.value) is ValueError
    assert str(excinfo.value) == "the real application error"


def test_emit_failure_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    def broken(event: TraceEvent) -> None:
        raise RuntimeError("schema registry unreachable")

    previous = telemetry._EMITTER
    telemetry.configure(broken)
    try:
        with caplog.at_level(logging.WARNING, logger="agentlake.sdk"):
            with session("s1"):
                with span("GATEWAY", "http_request"):
                    pass
    finally:
        telemetry._EMITTER = previous

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, "emit failure was not logged"
    assert "schema registry unreachable" in caplog.text


def test_emitter_error_alone_does_not_propagate(events: list[TraceEvent]) -> None:
    """A failing emitter on a clean span must not raise out of the with block."""

    def broken(event: TraceEvent) -> None:
        raise RuntimeError("registry down")

    previous = telemetry._EMITTER
    telemetry.configure(broken)
    try:
        with session("s1"):
            with span("GATEWAY", "http_request"):
                pass  # must simply complete
    finally:
        telemetry._EMITTER = previous


# ---------------------------------------------------------------------------
# 7. BaseException
# ---------------------------------------------------------------------------


def test_cancelled_error_recorded_as_error(events: list[TraceEvent]) -> None:
    """CancelledError is a BaseException; a cancelled request is not a success."""
    with pytest.raises(asyncio.CancelledError):
        with session("s1"):
            with span("GATEWAY", "http_request"):
                raise asyncio.CancelledError()

    assert events[0]["status"] == "error"
    assert events[0]["attributes"]["error_class"] == "CancelledError"


# ---------------------------------------------------------------------------
# 8. Attribute coercion
# ---------------------------------------------------------------------------


def test_attribute_values_become_strings(events: list[TraceEvent]) -> None:
    """Avro types attributes as map<string,string>: every value must be a str."""
    with session("s1"):
        with span("LLM_CALL", "chat", turn=1, ratio=0.75, cached=True) as sp:
            sp.set(hits=4, enabled=False)

    attributes = events[0]["attributes"]
    assert all(isinstance(v, str) for v in attributes.values()), attributes
    assert attributes["turn"] == "1"
    assert attributes["ratio"] == "0.75"
    assert attributes["hits"] == "4"


def test_bool_attributes_use_json_casing(events: list[TraceEvent]) -> None:
    """Documented convention (ADR-000): "true"/"false", not Python's "True"."""
    with session("s1"):
        with span("LLM_CALL", "chat", cached=True) as sp:
            sp.set(streamed=False)

    attributes = events[0]["attributes"]
    assert attributes["cached"] == "true"
    assert attributes["streamed"] == "false"


def test_none_attribute_drops_key(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("LLM_CALL", "chat", missing=None) as sp:
            sp.set(also_missing=None)

    attributes = events[0]["attributes"]
    assert "missing" not in attributes
    assert "also_missing" not in attributes


def test_set_none_removes_a_previously_set_attribute(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("LLM_CALL", "chat") as sp:
            sp.set(finish_reason="stop")
            sp.set(finish_reason=None)

    assert "finish_reason" not in events[0]["attributes"]


def test_string_none_never_appears_as_an_attribute_value(
    events: list[TraceEvent],
) -> None:
    with session("s1"):
        with span("LLM_CALL", "chat", a=None, b=1) as sp:
            sp.set(c=None, d="kept")
    run_two_turns("conv-2")

    for event in events:
        assert "None" not in event["attributes"].values(), event["attributes"]


# ---------------------------------------------------------------------------
# 9. .set() top-level fields
# ---------------------------------------------------------------------------


def test_set_coerces_tokens_to_int_and_cost_to_float(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("LLM_CALL", "chat") as sp:
            sp.set(prompt_tokens="120", completion_tokens=45.0, cost_usd="0.0012")

    event = events[0]
    assert event["prompt_tokens"] == 120
    assert type(event["prompt_tokens"]) is int  # Avro long
    assert event["completion_tokens"] == 45
    assert type(event["completion_tokens"]) is int
    assert event["cost_usd"] == pytest.approx(0.0012)
    assert type(event["cost_usd"]) is float  # Avro double


def test_set_promotes_only_the_contract_fields(events: list[TraceEvent]) -> None:
    """model/tokens/cost/status are top-level; anything else is an attribute."""
    with session("s1"):
        with span("LLM_CALL", "chat") as sp:
            sp.set(model="claude-haiku-4-5", status="degraded", finish_reason="stop")

    event = events[0]
    assert event["model"] == "claude-haiku-4-5"
    assert event["status"] == "degraded"
    assert event["attributes"]["finish_reason"] == "stop"
    assert "finish_reason" not in event


def test_set_invalid_value_raises_at_call_site(events: list[TraceEvent]) -> None:
    with session("s1"):
        with span("LLM_CALL", "chat") as sp:
            with pytest.raises(ValueError):
                sp.set(prompt_tokens="not-a-number")
            with pytest.raises(ValueError):
                sp.set(cost_usd="free")
            with pytest.raises(TypeError):
                sp.set(completion_tokens=[1, 2])


def test_exception_overrides_an_explicit_status(events: list[TraceEvent]) -> None:
    """Status precedence: default "ok" < explicit set() < exception."""
    with pytest.raises(ValueError):
        with session("s1"):
            with span("LLM_CALL", "chat") as sp:
                sp.set(status="degraded")
                raise ValueError("boom")

    assert events[0]["status"] == "error"


# ---------------------------------------------------------------------------
# 10. Unknown event_type
# ---------------------------------------------------------------------------


def test_unknown_event_type_raises_before_any_emit(events: list[TraceEvent]) -> None:
    """Fails identically with a test collector and on the Kafka path.

    The Avro enum would only reject a typo at serialize time. span() is a
    generator-based context manager, so the ValueError surfaces at __enter__.
    """
    with session("s1"):
        with pytest.raises(ValueError, match="unknown event_type"):
            with span("LLM_CAL", "typo"):  # type: ignore[arg-type]
                pass

    assert events == []


# ---------------------------------------------------------------------------
# 11. Context hygiene
# ---------------------------------------------------------------------------


def context_state() -> tuple[str | None, str | None, str | None]:
    return (
        telemetry._session_id.get(),
        telemetry._trace_id.get(),
        telemetry._parent_span_id.get(),
    )


def test_context_clean_after_normal_exit(events: list[TraceEvent]) -> None:
    assert context_state() == (None, None, None)
    run_two_turns()
    assert context_state() == (None, None, None)


def test_context_clean_after_exception(events: list[TraceEvent]) -> None:
    with pytest.raises(ValueError):
        with session("s1"):
            with span("AGENT_STEP", "agent_turn"):
                with span("LLM_CALL", "chat"):
                    raise ValueError("boom")

    assert context_state() == (None, None, None)


def test_nested_session_restores_the_outer_one(events: list[TraceEvent]) -> None:
    """reset(token) restores the prior value, which set(None) would not."""
    with session("outer"):
        with session("inner"):
            assert telemetry.current_session_id() == "inner"
        assert telemetry.current_session_id() == "outer"
    assert telemetry.current_session_id() is None


# ---------------------------------------------------------------------------
# 12. Async isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concurrent_sessions_are_isolated(events: list[TraceEvent]) -> None:
    """The FastAPI story: asyncio.Task creation copies the context."""

    async def handler(n: int) -> None:
        with session(f"req-{n}"):
            with span("GATEWAY", "http_request"):
                await asyncio.sleep(0.01)
                with span("LLM_CALL", "chat"):
                    await asyncio.sleep(0.01)

    await asyncio.gather(*[handler(i) for i in range(3)])

    by_session: dict[str, list[TraceEvent]] = {}
    for event in events:
        by_session.setdefault(event["session_id"], []).append(event)

    assert set(by_session) == {"req-0", "req-1", "req-2"}
    assert len({e["trace_id"] for e in events}) == 3  # no trace shared across requests

    for session_id, evs in by_session.items():
        assert len({e["trace_id"] for e in evs}) == 1, session_id
        roots = [e for e in evs if e["parent_span_id"] is None]
        assert len(roots) == 1, session_id
        children = [e for e in evs if e["parent_span_id"] is not None]
        assert all(c["parent_span_id"] == roots[0]["span_id"] for c in children)


# ---------------------------------------------------------------------------
# 13. Laziness
# ---------------------------------------------------------------------------


def test_import_does_not_pull_in_confluent_kafka() -> None:
    """Run in a subprocess: an in-process check would depend on test ordering."""
    code = (
        "import sys, services.sdk\n"
        "leaked = sorted(m for m in sys.modules if 'kafka' in m)\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_configure_kafka_builds_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Config is resolved eagerly; the producer and serializer are not."""
    monkeypatch.setattr(telemetry, "_kafka_config", None)
    monkeypatch.setattr(telemetry, "_kafka_runtime", None)
    monkeypatch.setattr(telemetry, "_EMITTER", None)
    monkeypatch.setenv("AGENTLAKE_KAFKA", "broker:19092")
    monkeypatch.setenv("AGENTLAKE_REGISTRY", "http://sr:8081")

    telemetry.configure_kafka()

    assert telemetry._kafka_runtime is None, "producer built too early"
    assert telemetry._kafka_config is not None
    assert telemetry._kafka_config.bootstrap_servers == "broker:19092"
    assert telemetry._kafka_config.registry_url == "http://sr:8081"


def test_explicit_args_beat_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry, "_kafka_config", None)
    monkeypatch.setattr(telemetry, "_EMITTER", None)
    monkeypatch.setenv("AGENTLAKE_KAFKA", "broker:19092")

    telemetry.configure_kafka("explicit:9092")

    assert telemetry._kafka_config is not None
    assert telemetry._kafka_config.bootstrap_servers == "explicit:9092"
    assert telemetry._kafka_config.producer_config["enable.idempotence"] is True
    assert telemetry._kafka_config.producer_config["acks"] == "all"


# ---------------------------------------------------------------------------
# 14. Explicit cross-process trace propagation
# ---------------------------------------------------------------------------


def test_span_joins_an_explicit_trace_and_parent(events: list[TraceEvent]) -> None:
    """A fresh process (no contextvars) that receives trace context from
    elsewhere (an HTTP header, an MCP tool argument) joins that trace instead
    of rooting a new one -- the mechanism services/gateway and
    services/mcp_server both use. See ADR-003.
    """
    with session("s1"):
        with span(
            "TOOL_CALL", "search_docs", trace_id="remote-trace", parent_span_id="remote-span"
        ):
            pass
    (event,) = events
    assert event["trace_id"] == "remote-trace"
    assert event["parent_span_id"] == "remote-span"


def test_span_with_explicit_trace_id_still_generates_its_own_span_id(
    events: list[TraceEvent],
) -> None:
    with session("s1"):
        with span("TOOL_CALL", "search_docs", trace_id="remote-trace", parent_span_id=None) as sp:
            pass
    (event,) = events
    assert event["span_id"] != "remote-trace"
    assert event["parent_span_id"] is None
    assert sp.span_id == event["span_id"]


def test_nested_span_ignores_explicit_trace_context(events: list[TraceEvent]) -> None:
    """Explicit trace_id/parent_span_id are a fallback for a process's first
    span only -- ordinary same-process nesting always wins, so passing them
    from code that might run nested (or standalone) is safe.
    """
    with session("s1"):
        with span("AGENT_STEP", "agent_turn") as outer:
            with span("TOOL_CALL", "search_docs", trace_id="ignored", parent_span_id="ignored"):
                pass
    child, parent = events  # children emit first
    assert parent["span_id"] == outer.span_id
    assert child["trace_id"] == parent["trace_id"] != "ignored"
    assert child["parent_span_id"] == parent["span_id"] != "ignored"


def test_current_parent_span_id_reads_the_open_span(events: list[TraceEvent]) -> None:
    assert telemetry.current_parent_span_id() is None
    with session("s1"):
        with span("AGENT_STEP", "agent_turn") as step:
            assert telemetry.current_parent_span_id() == step.span_id
            with span("TOOL_CALL", "search_docs") as tool:
                assert telemetry.current_parent_span_id() == tool.span_id
        assert telemetry.current_parent_span_id() is None
