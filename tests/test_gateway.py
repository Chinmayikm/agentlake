"""Tests for services/gateway.

Mocking seam: app.dependency_overrides[get_anthropic_client] -- not global
monkeypatching of the anthropic package. No network, no real API key: the fake
client below implements only the subset of the real interface chat.py calls
(messages.create, messages.stream as an async context manager).

Every test uses the `events` fixture from tests/conftest.py (shared with the
SDK suite) and gets the autouse `_no_kafka` fixture for free -- warmup() at
gateway startup therefore always returns False here, never touching a socket.

Assertions are written against services/gateway's spec, not the
implementation. A failing assertion here means the gateway is wrong -- report
it, don't loosen the check.
"""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx2
import pytest
from fastapi.testclient import TestClient

from services.gateway.app import create_app
from services.gateway.chat import get_anthropic_client
from services.gateway.pricing import load_price_table

PRICE_TABLE = load_price_table()
FAST = PRICE_TABLE.get("fast")  # claude-haiku-4-5: $1.00 / $5.00 per MTok


# ---------------------------------------------------------------------------
# Fakes -- implement only what chat.py actually calls on the client
# ---------------------------------------------------------------------------


def fake_message(
    *, model: str, prompt_tokens: int, completion_tokens: int, text: str = "hi there"
) -> anthropic.types.Message:
    return anthropic.types.Message.model_validate(
        {
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": prompt_tokens, "output_tokens": completion_tokens},
        }
    )


def fake_rate_limit_error(retry_after: str | None = "12") -> anthropic.RateLimitError:
    req = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    headers = {"retry-after": retry_after} if retry_after else {}
    body = {"type": "error", "error": {"type": "rate_limit_error", "message": "slow down"}}
    resp = httpx2.Response(
        429, request=req, headers=headers, content=json.dumps(body).encode()
    )
    return anthropic.RateLimitError("slow down", response=resp, body=body)


class FakeStream:
    """Implements the subset of AsyncMessageStreamManager chat.py uses."""

    def __init__(
        self, events: list[Any], final_message: Any = None, error: Exception | None = None
    ):
        self._events = events
        self._final_message = final_message
        self._error = error

    async def __aenter__(self) -> FakeStream:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def __aiter__(self):
        for event in self._events:
            yield event
        if self._error:
            raise self._error

    async def get_final_message(self) -> Any:
        return self._final_message


class FakeMessages:
    def __init__(
        self,
        response: Any = None,
        exception: Exception | None = None,
        stream_events: list[Any] | None = None,
        stream_final: Any = None,
        stream_error: Exception | None = None,
    ):
        self._response = response
        self._exception = exception
        self._stream_events = stream_events or []
        self._stream_final = stream_final
        self._stream_error = stream_error
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if self._exception:
            raise self._exception
        return self._response

    def stream(self, **kwargs: Any) -> FakeStream:
        self.calls.append(kwargs)
        return FakeStream(self._stream_events, self._stream_final, self._stream_error)


class FakeClient:
    def __init__(self, messages: FakeMessages):
        self.messages = messages


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def app_and_client(monkeypatch):
    """A gateway app + TestClient with a fake anthropic client wired in.

    monkeypatch.setenv guarantees a key is present regardless of what's in a
    real .env on this machine -- lifespan's load_dotenv() never overrides an
    already-set env var, so this is enough to make startup succeed.
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")

    def _make(fake_messages: FakeMessages):
        app = create_app()
        app.dependency_overrides[get_anthropic_client] = lambda: FakeClient(fake_messages)
        return app

    return _make


# ---------------------------------------------------------------------------
# 1. Happy path: usage fields, exactly one GATEWAY + one LLM_CALL span,
#    LLM_CALL's parent is the GATEWAY span
# ---------------------------------------------------------------------------


def test_chat_happy_path(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=100, completion_tokens=50))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["usage"]["prompt_tokens"] == 100
    assert body["usage"]["completion_tokens"] == 50
    assert "cost_usd" in body["usage"]
    assert body["usage"]["latency_ms"] > 0

    gateway_events = [e for e in events if e["event_type"] == "GATEWAY"]
    llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
    assert len(gateway_events) == 1, events
    assert len(llm_events) == 1, events
    assert llm_events[0]["parent_span_id"] == gateway_events[0]["span_id"]


# ---------------------------------------------------------------------------
# 2. cost_usd matches a hand-computed value from models.yaml; price-table
#    version appears in span attributes
# ---------------------------------------------------------------------------


def test_chat_cost_matches_price_table(app_and_client, events):
    prompt_tokens, completion_tokens = 1000, 500
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=prompt_tokens,
                                               completion_tokens=completion_tokens))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )

    expected_cost = (
        prompt_tokens * FAST.input_price_per_mtok + completion_tokens * FAST.output_price_per_mtok
    ) / 1_000_000

    assert r.json()["usage"]["cost_usd"] == pytest.approx(expected_cost)

    llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
    assert llm_events[0]["cost_usd"] == pytest.approx(expected_cost)
    assert llm_events[0]["attributes"]["price_table_version"] == PRICE_TABLE.version


# ---------------------------------------------------------------------------
# 3. X-Session-Id header -> both spans carry that session_id
# ---------------------------------------------------------------------------


def test_chat_session_id_header_propagates(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=10, completion_tokens=5))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
            headers={"X-Session-Id": "conversation-42"},
        )

    assert r.status_code == 200, r.text
    assert len(events) == 2
    assert all(e["session_id"] == "conversation-42" for e in events)


# ---------------------------------------------------------------------------
# 3a. X-Trace-Id / X-Parent-Span-Id headers -> GATEWAY span joins that trace
# instead of rooting a new one (ADR-003 #4 -- cross-process trace propagation)
# ---------------------------------------------------------------------------


def test_chat_joins_caller_trace_when_headers_given(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=10, completion_tokens=5))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
            headers={"X-Trace-Id": "caller-trace", "X-Parent-Span-Id": "caller-span"},
        )

    assert r.status_code == 200, r.text
    gateway_events = [e for e in events if e["event_type"] == "GATEWAY"]
    llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
    assert gateway_events[0]["trace_id"] == "caller-trace"
    assert gateway_events[0]["parent_span_id"] == "caller-span"
    # LLM_CALL is a normal nested child -- it inherits the trace and gets
    # GATEWAY's span_id as its parent, same as with no headers at all.
    assert llm_events[0]["trace_id"] == "caller-trace"
    assert llm_events[0]["parent_span_id"] == gateway_events[0]["span_id"]


def test_chat_without_trace_headers_roots_its_own_trace(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=10, completion_tokens=5))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert r.status_code == 200, r.text
    gateway_events = [e for e in events if e["event_type"] == "GATEWAY"]
    assert gateway_events[0]["parent_span_id"] is None


# ---------------------------------------------------------------------------
# 3b. tools passthrough -- services/agent's Anthropic-style tool use (ADR-003)
# ---------------------------------------------------------------------------


def test_chat_forwards_tools_to_provider(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=10, completion_tokens=5))
    app = app_and_client(fake)
    tools = [{"name": "search_docs", "description": "search", "input_schema": {"type": "object"}}]

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={
                "model_alias": "fast",
                "messages": [{"role": "user", "content": "hi"}],
                "tools": tools,
            },
        )

    assert r.status_code == 200, r.text
    assert fake.calls[0]["tools"] == tools


def test_chat_omits_tools_when_not_given(app_and_client, events):
    fake = FakeMessages(response=fake_message(model=FAST.provider_model_id,
                                               prompt_tokens=10, completion_tokens=5))
    app = app_and_client(fake)

    with TestClient(app) as client:
        client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert "tools" not in fake.calls[0]


# ---------------------------------------------------------------------------
# 4. Provider error -> HTTP error code, span status="error", gateway stays up
# ---------------------------------------------------------------------------


def test_chat_provider_error_maps_to_http_and_records_span(app_and_client, events):
    fake = FakeMessages(exception=fake_rate_limit_error(retry_after="7"))
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert r.status_code == 429, r.text
        assert r.headers.get("retry-after") == "7"
        assert "error" in r.json()

        llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
        assert len(llm_events) == 1
        assert llm_events[0]["status"] == "error"
        assert llm_events[0]["attributes"]["error_class"] == "RateLimitError"

        # Gateway stays up: a follow-up request still succeeds.
        fake._exception = None
        fake._response = fake_message(
            model=FAST.provider_model_id, prompt_tokens=1, completion_tokens=1
        )
        r2 = client.post(
            "/v1/chat",
            json={"model_alias": "fast", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert r2.status_code == 200, r2.text


# ---------------------------------------------------------------------------
# 5. Missing ANTHROPIC_API_KEY -> startup fails with a clear message
# ---------------------------------------------------------------------------


def test_missing_api_key_fails_startup(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    # Decouple from whatever real .env happens to exist on this machine --
    # load_dotenv() would otherwise repopulate the var from disk and silently
    # defeat this test.
    monkeypatch.setattr("services.gateway.app.load_dotenv", lambda *a, **k: None)

    app = create_app()
    with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
        with TestClient(app):
            pass


# ---------------------------------------------------------------------------
# 6. Unknown model_alias -> 400, no LLM_CALL span emitted
# ---------------------------------------------------------------------------


def test_unknown_model_alias(app_and_client, events):
    fake = FakeMessages()
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={"model_alias": "nonexistent", "messages": [{"role": "user", "content": "hi"}]},
        )

    assert r.status_code == 400, r.text
    assert r.json()["error"]["type"] == "unknown_model_alias"
    assert events == []
    assert fake.calls == []  # provider was never called


# ---------------------------------------------------------------------------
# Streaming: not one of the six numbered specs, but the riskiest code path --
# proves the generator-scoped span design actually captures stream duration
# and correct parent/child linkage, not just that it doesn't crash.
# ---------------------------------------------------------------------------


def test_chat_streaming_emits_events_and_spans(app_and_client, events):
    stream_events = [
        anthropic.types.RawContentBlockDeltaEvent.model_validate(
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "hi"},
            }
        )
    ]
    final = fake_message(model=FAST.provider_model_id, prompt_tokens=30, completion_tokens=15)
    fake = FakeMessages(stream_events=stream_events, stream_final=final)
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={
                "model_alias": "fast",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )

    assert r.status_code == 200, r.text
    body = r.text
    assert "event: content_block_delta" in body
    assert "event: gateway_usage" in body

    usage_line = next(
        json.loads(line[len("data: "):])
        for line in body.splitlines()
        if line.startswith("data: ") and '"prompt_tokens"' in line
    )
    assert usage_line["prompt_tokens"] == 30
    assert usage_line["completion_tokens"] == 15
    assert usage_line["latency_ms"] > 0

    gateway_events = [e for e in events if e["event_type"] == "GATEWAY"]
    llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
    assert len(gateway_events) == 1
    assert len(llm_events) == 1
    assert llm_events[0]["parent_span_id"] == gateway_events[0]["span_id"]
    assert llm_events[0]["latency_ms"] > 0


def test_chat_streaming_provider_error_yields_error_frame(app_and_client, events):
    fake = FakeMessages(stream_error=fake_rate_limit_error())
    app = app_and_client(fake)

    with TestClient(app) as client:
        r = client.post(
            "/v1/chat",
            json={
                "model_alias": "fast",
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            },
        )

    assert r.status_code == 200  # headers already sent; body carries the error
    assert "event: gateway_error" in r.text

    gateway_events = [e for e in events if e["event_type"] == "GATEWAY"]
    llm_events = [e for e in events if e["event_type"] == "LLM_CALL"]
    assert gateway_events[0]["status"] == "error"
    assert llm_events[0]["status"] == "error"
    assert llm_events[0]["attributes"]["error_class"] == "RateLimitError"
