"""GET /v1/health and GET /v1/stats."""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/v1/health")
async def health(request: Request) -> dict:
    return {
        "status": "ok",
        # Set once at startup from services.sdk.warmup()'s return value, not
        # re-checked per request -- the Kafka runtime, once built, lives for
        # the process lifetime (see services/sdk/telemetry.py's _get_kafka).
        "kafka_warmed": request.app.state.kafka_warm,
    }


@router.get("/v1/stats")
async def stats(request: Request) -> dict:
    return request.app.state.stats.to_dict()
