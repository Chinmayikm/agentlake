"""GatewayClient -- the agent's only path to an LLM, per ADR-001: nothing in
services/agent imports anthropic or holds ANTHROPIC_API_KEY.

Reuses services.gateway.chat's ChatRequest/ChatResponse pydantic models
directly instead of duplicating the wire shape, so client and server can never
silently drift apart.
"""

from __future__ import annotations

import os
from typing import Protocol

import httpx

from services.gateway.chat import ChatRequest, ChatResponse
from services.sdk import current_parent_span_id, current_trace_id

DEFAULT_GATEWAY_URL = "http://localhost:8100"


class GatewayUnavailableError(Exception):
    pass


class GatewayClient(Protocol):
    async def chat(
        self, request: ChatRequest, *, session_id: str | None = None
    ) -> ChatResponse: ...


class HttpGatewayClient:
    def __init__(self, base_url: str | None = None, *, timeout: float = 60.0) -> None:
        self._base_url = (
            base_url or os.environ.get("AGENTLAKE_GATEWAY", DEFAULT_GATEWAY_URL)
        ).rstrip("/")
        self._client = httpx.AsyncClient(timeout=timeout)

    async def chat(self, request: ChatRequest, *, session_id: str | None = None) -> ChatResponse:
        headers = {"X-Session-Id": session_id} if session_id else {}
        # Cross-process trace propagation (ADR-003 #4): contextvars stop at
        # this process's boundary, so the GATEWAY span this call opens has no
        # way to learn our trace_id/parent_span_id except over the wire.
        trace_id = current_trace_id()
        if trace_id:
            headers["X-Trace-Id"] = trace_id
        parent_span_id = current_parent_span_id()
        if parent_span_id:
            headers["X-Parent-Span-Id"] = parent_span_id
        try:
            resp = await self._client.post(
                f"{self._base_url}/v1/chat",
                json=request.model_dump(mode="json", exclude_none=True),
                headers=headers,
            )
        except httpx.ConnectError as exc:
            raise GatewayUnavailableError(
                f"Cannot reach the inference gateway at {self._base_url} -- "
                "is `make gateway` running?"
            ) from exc
        resp.raise_for_status()
        return ChatResponse.model_validate(resp.json())

    async def aclose(self) -> None:
        await self._client.aclose()
