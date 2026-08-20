"""agentlake telemetry SDK.

Import the public surface from here, not from services.sdk.telemetry, so the
internals stay free to move::

    from services.sdk import session, span
"""

from services.sdk.telemetry import (
    TOPIC,
    EmitFn,
    EventType,
    Span,
    TraceEvent,
    configure,
    configure_kafka,
    current_session_id,
    current_trace_id,
    flush,
    session,
    span,
)

__all__ = [
    "EmitFn",
    "EventType",
    "Span",
    "TOPIC",
    "TraceEvent",
    "configure",
    "configure_kafka",
    "current_session_id",
    "current_trace_id",
    "flush",
    "session",
    "span",
]
