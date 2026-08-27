"""POST /v1/chat -- the only route that talks to the Anthropic Messages API.

Streaming and telemetry interact in a way that is easy to get wrong: Starlette
iterates a StreamingResponse's body generator *after* the route handler
coroutine has already returned. If spans were opened in the handler and it
just constructed-and-returned a StreamingResponse, they would close before a
single byte streamed -- near-zero latency, no token counts.

So for stream=True, the handler itself opens no spans at all. It validates the
model alias and immediately returns StreamingResponse(stream_chat(...)); every
context manager (session, GATEWAY span, LLM_CALL span) lives *inside*
stream_chat, opened on Starlette's first __anext__() call -- which happens
only once it actually starts sending the response body, so span lifetime
matches real streaming duration.

This is safe because there is no asyncio.Task boundary between the handler and
the generator: uvicorn runs one Task per request, and Starlette's
`async for chunk in body_iterator` iterates inside that same Task. Sync
generators (which is what @contextlib.contextmanager produces) don't isolate
contextvars either way -- see services/sdk/telemetry.py's own module
docstring -- so `.set()` / token resets inside stream_chat's `with` blocks
behave identically to the non-streaming path.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import nullcontext
from typing import Any, Literal

import anthropic
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from services.gateway.pricing import ModelConfig, PriceTable, cost_usd
from services.gateway.stats import GatewayStats
from services.sdk import Span, session, span

router = APIRouter()

# Per shared/error-codes.md: don't lowball max_tokens. Streaming gets more
# room since HTTP timeouts aren't a concern there.
DEFAULT_MAX_TOKENS_NON_STREAM = 16000
DEFAULT_MAX_TOKENS_STREAM = 64000


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str | list[dict[str, Any]]


class ChatRequest(BaseModel):
    model_alias: str
    messages: list[ChatMessage]
    max_tokens: int | None = None
    temperature: float | None = None
    stream: bool = False
    # Anthropic-style tool definitions, passed through verbatim. Only
    # services/agent uses this today -- see ADR-003. Never hardcoded or
    # inspected here; the gateway stays a pure passthrough per ADR-001.
    tools: list[dict[str, Any]] | None = None


class UsageOut(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    latency_ms: float


class ChatResponse(BaseModel):
    id: str
    model: str
    role: str
    content: list[dict[str, Any]]
    stop_reason: str | None
    usage: UsageOut


# ---------------------------------------------------------------------------
# Dependencies -- the mocking seam for tests (app.dependency_overrides)
# ---------------------------------------------------------------------------


def get_anthropic_client(request: Request) -> anthropic.AsyncAnthropic:
    return request.app.state.anthropic_client


def get_price_table(request: Request) -> PriceTable:
    return request.app.state.price_table


def get_stats(request: Request) -> GatewayStats:
    return request.app.state.stats


# ---------------------------------------------------------------------------
# Shared helpers -- used by both the streaming and non-streaming paths
# ---------------------------------------------------------------------------


def session_ctx(x_session_id: str | None):
    """session(x_session_id) if the header was given, else a no-op context.

    Absent a header, the SDK's own implicit-session mechanism in span() takes
    over -- this function does not fabricate a session id itself.
    """
    return session(x_session_id) if x_session_id else nullcontext(None)


def provider_kwargs(payload: ChatRequest, model_cfg: ModelConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "model": model_cfg.provider_model_id,
        "max_tokens": payload.max_tokens
        or (DEFAULT_MAX_TOKENS_STREAM if payload.stream else DEFAULT_MAX_TOKENS_NON_STREAM),
        "messages": [{"role": m.role, "content": m.content} for m in payload.messages],
    }
    if payload.temperature is not None:
        # temperature/top_p/top_k were removed from the Messages API on
        # current-generation models (confirmed: absent from the installed
        # anthropic SDK's typed messages.create/stream signatures entirely).
        # Forwarded via extra_body rather than silently dropped: a caller who
        # sets it gets the provider's real 400 back through
        # map_provider_error(), not silent no-op behaviour. See ADR-001.
        kwargs["extra_body"] = {"temperature": payload.temperature}
    if payload.tools:
        kwargs["tools"] = payload.tools
    return kwargs


def record_usage(
    llm: Span,
    model_cfg: ModelConfig,
    price_table: PriceTable,
    stats: GatewayStats,
    prompt_tokens: int,
    completion_tokens: int,
) -> float:
    """Set the LLM_CALL span's usage fields and update process stats.

    Must be called while the LLM_CALL span is still open, so .set() lands in
    the emitted event. Returns cost_usd, computed from models.yaml only.
    """
    cost = cost_usd(model_cfg, prompt_tokens, completion_tokens)
    llm.set(
        model=model_cfg.provider_model_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost_usd=cost,
        price_table_version=price_table.version,
    )
    stats.record_success(model_cfg.alias, prompt_tokens, completion_tokens, cost)
    return cost


def map_provider_error(exc: anthropic.APIError) -> tuple[int, str, str]:
    """(http_status, error_type, message). One table, shared by both paths.

    BadRequestError is caller-caused (e.g. malformed messages) -- safe and
    helpful to pass the provider's own message through. Authentication/
    permission/not-found are OUR configuration's fault, never the caller's, so
    the message stays generic and the key is never touched, let alone logged.
    RateLimitError is a meaningful backpressure signal worth passing through
    as-is. Everything else provider-side (connection failures, 5xx, 529
    overloaded) is retryable from the caller's point of view -> 503.

    Per the spec: this table only decides the HTTP-facing response. It never
    touches span status -- services.sdk already records status="error" and
    error_class automatically when the `with span(...)` block raises.
    """
    if isinstance(exc, anthropic.BadRequestError):
        return 400, "invalid_request", str(exc)
    if isinstance(exc, anthropic.AuthenticationError):
        return 502, "upstream_auth_failed", "The gateway's upstream credentials were rejected."
    if isinstance(exc, anthropic.PermissionDeniedError):
        return (
            502,
            "upstream_permission_denied",
            "The gateway's upstream credentials lack permission for this request.",
        )
    if isinstance(exc, anthropic.NotFoundError):
        return 502, "upstream_not_found", "The configured provider model was not found."
    if isinstance(exc, anthropic.RateLimitError):
        return 429, "rate_limited", "The upstream provider is rate-limiting this gateway."
    if isinstance(exc, anthropic.APIConnectionError):
        # Base class for both connection failures and APITimeoutError.
        return 503, "upstream_unavailable", "Could not reach the model provider."
    if isinstance(exc, anthropic.APIStatusError):
        return 503, "upstream_error", "The model provider returned an error."
    return 500, "internal_error", "An unexpected error occurred."


def retry_after_headers(exc: anthropic.APIError) -> dict[str, str]:
    if isinstance(exc, anthropic.RateLimitError):
        retry_after = exc.response.headers.get("retry-after")
        if retry_after:
            return {"Retry-After": retry_after}
    return {}


def error_body(error_type: str, message: str) -> dict[str, Any]:
    return {"error": {"type": error_type, "message": message}}


def sse_frame(event_name: str, data: str) -> str:
    return f"event: {event_name}\ndata: {data}\n\n"


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------


@router.post("/v1/chat")
async def chat(
    payload: ChatRequest,
    x_session_id: str | None = Header(default=None, alias="X-Session-Id"),
    x_trace_id: str | None = Header(default=None, alias="X-Trace-Id"),
    x_parent_span_id: str | None = Header(default=None, alias="X-Parent-Span-Id"),
    client: anthropic.AsyncAnthropic = Depends(get_anthropic_client),
    price_table: PriceTable = Depends(get_price_table),
    stats: GatewayStats = Depends(get_stats),
):
    model_cfg = price_table.get(payload.model_alias)
    if model_cfg is None:
        # Validated before any span opens: zero telemetry for a request that
        # never had a valid target, not just "no LLM_CALL span".
        return JSONResponse(
            status_code=400,
            content=error_body(
                "unknown_model_alias",
                f"unknown model_alias {payload.model_alias!r}; "
                f"known aliases: {sorted(price_table.models)}",
            ),
        )

    if payload.stream:
        return StreamingResponse(
            stream_chat(
                payload,
                model_cfg,
                price_table,
                stats,
                client,
                x_session_id,
                x_trace_id,
                x_parent_span_id,
            ),
            media_type="text/event-stream",
        )

    return await chat_once(
        payload, model_cfg, price_table, stats, client, x_session_id, x_trace_id, x_parent_span_id
    )


async def chat_once(
    payload: ChatRequest,
    model_cfg: ModelConfig,
    price_table: PriceTable,
    stats: GatewayStats,
    client: anthropic.AsyncAnthropic,
    x_session_id: str | None,
    x_trace_id: str | None = None,
    x_parent_span_id: str | None = None,
) -> JSONResponse | ChatResponse:
    try:
        with (
            session_ctx(x_session_id),
            span(
                "GATEWAY",
                "chat",
                model_alias=payload.model_alias,
                stream=False,
                trace_id=x_trace_id,
                parent_span_id=x_parent_span_id,
            ),
            span("LLM_CALL", "anthropic_messages") as llm,
        ):
            message = await client.messages.create(**provider_kwargs(payload, model_cfg))
            cost = record_usage(
                llm,
                model_cfg,
                price_table,
                stats,
                message.usage.input_tokens,
                message.usage.output_tokens,
            )
        # LLM_CALL span has closed here (finally already ran) -- llm.latency_ms
        # now holds the final measured value.
    except anthropic.APIError as exc:
        stats.record_error(payload.model_alias)
        status, error_type, message_text = map_provider_error(exc)
        return JSONResponse(
            status_code=status,
            content=error_body(error_type, message_text),
            headers=retry_after_headers(exc),
        )

    return ChatResponse(
        id=message.id,
        model=message.model,
        role=message.role,
        content=[block.model_dump(mode="json") for block in message.content],
        stop_reason=message.stop_reason,
        usage=UsageOut(
            prompt_tokens=message.usage.input_tokens,
            completion_tokens=message.usage.output_tokens,
            cost_usd=cost,
            latency_ms=llm.latency_ms,
        ),
    )


async def stream_chat(
    payload: ChatRequest,
    model_cfg: ModelConfig,
    price_table: PriceTable,
    stats: GatewayStats,
    client: anthropic.AsyncAnthropic,
    x_session_id: str | None,
    x_trace_id: str | None = None,
    x_parent_span_id: str | None = None,
) -> AsyncIterator[str]:
    with session_ctx(x_session_id):
        try:
            with span(
                "GATEWAY",
                "chat",
                model_alias=payload.model_alias,
                stream=True,
                trace_id=x_trace_id,
                parent_span_id=x_parent_span_id,
            ):
                with span("LLM_CALL", "anthropic_messages") as llm:
                    async with client.messages.stream(
                        **provider_kwargs(payload, model_cfg)
                    ) as stream_mgr:
                        async for event in stream_mgr:
                            # Full-fidelity passthrough of Anthropic's own
                            # event stream -- no reinvented envelope, so tool
                            # use / thinking blocks added later just work.
                            yield sse_frame(event.type, event.model_dump_json())
                        final = await stream_mgr.get_final_message()
                    cost = record_usage(
                        llm,
                        model_cfg,
                        price_table,
                        stats,
                        final.usage.input_tokens,
                        final.usage.output_tokens,
                    )
                # LLM_CALL span closed; llm.latency_ms is now final. Named
                # gateway_usage/gateway_error (below) so these never collide
                # with Anthropic's own event-type vocabulary.
                yield sse_frame(
                    "gateway_usage",
                    json.dumps(
                        {
                            "prompt_tokens": final.usage.input_tokens,
                            "completion_tokens": final.usage.output_tokens,
                            "cost_usd": cost,
                            "latency_ms": llm.latency_ms,
                        }
                    ),
                )
        except anthropic.APIError as exc:
            # Caught outside both `with span(...)` blocks so GATEWAY and
            # LLM_CALL both already recorded status="error" automatically
            # before this runs. SSE headers (200, text/event-stream) are
            # already flushed by this point -- the HTTP status can no longer
            # change, so an error frame is the only way to signal failure.
            stats.record_error(payload.model_alias)
            # HTTP status is unused here on purpose: it can't change once SSE
            # headers are sent, unlike the non-streaming path.
            _status, error_type, message_text = map_provider_error(exc)
            yield sse_frame("gateway_error", json.dumps(error_body(error_type, message_text)))
