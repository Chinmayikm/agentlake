"""FastAPI app factory and lifespan for the inference gateway.

An app factory (create_app()), not a module-level `app = FastAPI(...)`,
because the API-key check must run fresh every time a TestClient enters
lifespan -- see tests/test_gateway.py's missing-key test.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from anthropic import AsyncAnthropic
from dotenv import load_dotenv
from fastapi import FastAPI

from services.gateway import routes
from services.gateway.chat import router as chat_router
from services.gateway.pricing import DEFAULT_MODELS_PATH, load_price_table
from services.gateway.stats import GatewayStats
from services.sdk import flush, warmup


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_dotenv()
    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set. Copy .env.example to .env and add your key."
        )

    # AsyncAnthropic() resolves the key from the environment itself -- never
    # read or logged here.
    app.state.anthropic_client = AsyncAnthropic()
    app.state.price_table = load_price_table(DEFAULT_MODELS_PATH)
    app.state.stats = GatewayStats()
    # Resolves ADR-000's open item: pay the ~800ms lazy Kafka init now, not on
    # the first real request. Never raises; a False here just means the SDK's
    # own lazy path (still swallow+log, never crash the app) takes over.
    app.state.kafka_warm = warmup()

    yield

    flush()


def create_app() -> FastAPI:
    app = FastAPI(title="agentlake inference gateway", lifespan=lifespan)
    app.include_router(chat_router)
    app.include_router(routes.router)
    return app
