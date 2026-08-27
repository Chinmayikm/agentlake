"""Trace telemetry for agentlake: nested spans emitted as Avro TraceEvents.

Usage::

    from services.sdk import session, span

    with session() as session_id:
        with span("AGENT_STEP", "agent_turn") as step:
            with span("LLM_CALL", "chat_completion") as llm:
                llm.set(model="claude-haiku-4-5", prompt_tokens=120)

Every emitted event matches contracts/trace_event_v1.avsc field-for-field.

Session and trace scope
-----------------------
**session = one conversation. trace = one turn's causal graph.**

A session groups a whole conversation and is what Kafka partitions on. A trace
covers a single top-level operation within it -- one agent turn -- following the
OpenTelemetry convention that a trace is one operation, not one user's lifetime.
So ``get_trace(trace_id)`` returns exactly one turn, while filtering on
``session_id`` reassembles the conversation.

Mechanically: the outermost span in a session creates the trace_id and is the
only one that clears it on exit, so the next top-level span starts a fresh
trace. Spans nested inside a turn inherit that turn's trace_id and never reset
it. See the Token-or-None handling in span().

Why contextvars
---------------
Trace state lives in three ContextVars, never in globals or threading.local, so
that a FastAPI gateway can run concurrent requests without them seeing each
other's session.

Two properties of PEP 567 make this work, and both are worth knowing before
editing this module:

1. Generators do NOT get their own context (PEP 550 was rejected). So a
   ``ContextVar.set()`` inside a ``@contextmanager`` body mutates the *caller's*
   context -- which is exactly what span nesting needs.
2. ``asyncio.Task`` creation calls ``contextvars.copy_context()``. So each
   request handler runs in its own copy: concurrent requests cannot see each
   other's session_id, and a session opened inside a request tears down with it.

Consequently ``session()`` and ``span()`` are plain *sync* context managers, used
with ``with`` even inside ``async def``. Neither enter nor exit awaits anything,
so there is no reason for ``async with`` variants -- one API for both worlds.
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, TypedDict, get_args

if TYPE_CHECKING:  # imported lazily at runtime -- see _get_kafka()
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry.avro import AvroSerializer

__all__ = [
    "TOPIC",
    "EmitFn",
    "EventType",
    "Span",
    "TraceEvent",
    "configure",
    "configure_kafka",
    "current_parent_span_id",
    "current_session_id",
    "current_trace_id",
    "flush",
    "session",
    "span",
]

logger = logging.getLogger("agentlake.sdk")
# Deliberately no NullHandler: with no handler anywhere, logging's lastResort
# prints WARNING+ to stderr, so a broken emitter is visible out of the box
# instead of silently dropping telemetry for a week.

TOPIC = "traces.events.v1"

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

EventType = Literal["LLM_CALL", "TOOL_CALL", "RETRIEVAL", "AGENT_STEP", "GATEWAY", "ERROR"]

# Single source of truth: derived from the Literal so the two cannot drift.
_EVENT_TYPES: frozenset[str] = frozenset(get_args(EventType))


class TraceEvent(TypedDict):
    """The wire shape. Fields in contracts/trace_event_v1.avsc order."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    session_id: str
    event_type: str
    model: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_ms: float
    cost_usd: float | None
    status: str
    ts_epoch_ms: int
    attributes: dict[str, str]


EmitFn = Callable[[TraceEvent], None]


def _new_id() -> str:
    """32-char hex id, used for trace_id, span_id and generated session_id.

    One shape for every id: one column type and one set of assumptions in dbt
    and ClickHouse downstream. See ADR-000 for why this is not truncated.
    """
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Trace context
# ---------------------------------------------------------------------------

# Three scalar vars rather than one dict-valued var: a dict invites in-place
# mutation, which escapes the context copy and defeats the whole mechanism.
# Scalars can only be replaced. default=None means .get() never raises.
_session_id: ContextVar[str | None] = ContextVar("agentlake_session_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("agentlake_trace_id", default=None)
_parent_span_id: ContextVar[str | None] = ContextVar("agentlake_parent_span_id", default=None)


def current_session_id() -> str | None:
    """The session_id in scope, or None outside any session()."""
    return _session_id.get()


def current_trace_id() -> str | None:
    """The current turn's trace_id, or None when no top-level span is open."""
    return _trace_id.get()


def current_parent_span_id() -> str | None:
    """The span_id a span opened right now would attach to as a child, or
    None if none is open. Together with current_trace_id(), this is what a
    caller reads to propagate its trace context across a process boundary
    (HTTP header, MCP tool argument, ...) -- see span()'s trace_id/
    parent_span_id parameters on the receiving end.
    """
    return _parent_span_id.get()


# ---------------------------------------------------------------------------
# Attribute coercion
# ---------------------------------------------------------------------------

# Avro types the attributes map as map<string,string>, so every value must
# already be a str by the time it reaches the serializer. We coerce at the
# boundary -- never at emit time -- so Span.attributes is a dict[str, str] at
# all times, exactly as annotated.


def _coerce_attr(value: object) -> str | None:
    """None means "drop the key"; everything else becomes a str."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"  # match JSON/SQL, not Python's "True"
    return str(value)


def _coerce_attrs(attrs: Mapping[str, object]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in attrs.items():
        coerced = _coerce_attr(value)
        if coerced is not None:
            out[key] = coerced
    return out


# ---------------------------------------------------------------------------
# Span
# ---------------------------------------------------------------------------

# The only .set() kwargs that promote to top-level Avro fields. Everything else
# lands in the attributes map.
_TOP_LEVEL: frozenset[str] = frozenset(
    {"model", "prompt_tokens", "completion_tokens", "cost_usd", "status"}
)

_MAX_ERROR_MESSAGE = 200


@dataclass(slots=True)
class Span:
    """A span in progress. Mutable by nature: it accumulates until the block exits."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    session_id: str
    event_type: str
    ts_epoch_ms: int
    attributes: dict[str, str] = field(default_factory=dict)
    model: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    cost_usd: float | None = None
    status: str = "ok"
    latency_ms: float = 0.0

    def set(self, **kwargs: object) -> None:
        """Attach metadata to this span.

        ``model``, ``prompt_tokens``, ``completion_tokens``, ``cost_usd`` and
        ``status`` become top-level Avro fields. Any other keyword lands in the
        attributes map, coerced to a string (``None`` drops the key).

        Values are coerced here rather than at emit time so that a bad value --
        a Decimal cost, say -- raises at the call site with a real traceback,
        instead of inside AvroSerializer in production only.
        """
        for key, value in kwargs.items():
            if key not in _TOP_LEVEL:
                coerced = _coerce_attr(value)
                if coerced is None:
                    self.attributes.pop(key, None)
                else:
                    self.attributes[key] = coerced
            elif value is None:
                setattr(self, key, None)
            elif key in ("prompt_tokens", "completion_tokens"):
                setattr(self, key, int(value))  # Avro long
            elif key == "cost_usd":
                setattr(self, key, float(value))  # Avro double
            else:  # model, status
                setattr(self, key, str(value))

    def to_event(self) -> TraceEvent:
        """Project to the wire shape. Keys in .avsc order, so a diff is a visual scan."""
        return TraceEvent(
            trace_id=self.trace_id,
            span_id=self.span_id,
            parent_span_id=self.parent_span_id,
            session_id=self.session_id,
            event_type=self.event_type,
            model=self.model,
            prompt_tokens=self.prompt_tokens,
            completion_tokens=self.completion_tokens,
            latency_ms=round(self.latency_ms, 3),
            cost_usd=self.cost_usd,
            status=self.status,
            ts_epoch_ms=self.ts_epoch_ms,
            # Copy, so a caller holding the Span cannot mutate an emitted event.
            attributes=dict(self.attributes),
        )


# ---------------------------------------------------------------------------
# Public context managers
# ---------------------------------------------------------------------------


@contextmanager
def session(session_id: str | None = None) -> Iterator[str]:
    """Open a session -- one conversation -- generating an id if none is given.

    All three context vars are set and reset here. Clearing trace_id and
    parent_span_id on entry matters: without it, a session opened inside an
    enclosing span would inherit that span's trace and parent, silently gluing
    unrelated work together.
    """
    sid = session_id or _new_id()

    # Never var.set(None) to clear: reset(token) restores the exact prior value,
    # including "was unset in this context". Every set() below is paired with a
    # reset() in the finally, so no exception can leave the context corrupted.
    tok_session = _session_id.set(sid)
    tok_trace = _trace_id.set(None)
    tok_parent = _parent_span_id.set(None)
    try:
        yield sid
    finally:
        _parent_span_id.reset(tok_parent)
        _trace_id.reset(tok_trace)
        _session_id.reset(tok_session)


@contextmanager
def span(
    event_type: EventType,
    name: str,
    *,
    trace_id: str | None = None,
    parent_span_id: str | None = None,
    **attrs: object,
) -> Iterator[Span]:
    """Open a span; emit a TraceEvent when the block exits.

    ``name`` is not a field in the Avro contract -- it is stored as
    ``attributes["name"]``. Keep it a low-cardinality label ("chat_completion",
    "vector_search"), never an interpolated string containing an id: downstream
    this becomes a GROUP BY key.

    Nesting is automatic. The outermost span creates a trace_id and clears it
    again on exit; spans opened inside it inherit that trace_id and pick up the
    enclosing span as parent. One trace therefore covers one top-level
    operation -- one agent turn -- and a later top-level span in the same
    session starts a new trace.

    ``trace_id``/``parent_span_id``: explicit trace context carried in from
    another process. contextvars never cross a process boundary (an HTTP
    request, an MCP call over stdio), so a span that should join a trace
    that originated elsewhere has no other way to learn it -- see
    current_trace_id()/current_parent_span_id() on the sending side. They
    are used only as a fallback for when *this process's own context* has no
    trace open yet (i.e. this is the first span opened here for this
    operation); ordinary same-process nesting always takes precedence, so
    passing them is safe even from code that might sometimes run nested.
    Leave both None for the common, single-process case.

    The event is emitted from a ``finally`` block, so latency is recorded and
    the event fires even when the body raises. Status precedence: the default
    "ok", overridden by an explicit ``set(status=...)``, overridden in turn by
    an exception, which forces "error" and re-raises.
    """
    if event_type not in _EVENT_TYPES:
        # The Avro enum would only reject a typo at serialize time, i.e. only on
        # the Kafka path. Checking here fails identically under a test collector.
        raise ValueError(
            f"unknown event_type {event_type!r}; expected one of {sorted(_EVENT_TYPES)}"
        )

    # Tokens stay None unless *this* span was the one that set the var. That is
    # what scopes a trace to one turn: the outermost span holds tok_trace and
    # resets it on exit, while nested spans hold None and reset nothing, so an
    # inner span exiting can never end the trace its parent owns.
    tok_session: Token[str | None] | None = None
    tok_trace: Token[str | None] | None = None

    session_id = _session_id.get()
    implicit_session = session_id is None
    if session_id is None:
        # No active session. Rather than crash the application we are merely
        # observing, open an implicit session scoped to this span: children stay
        # coherent, the partition key is still a real id, and it dies with us.
        session_id = _new_id()
        tok_session = _session_id.set(session_id)
        logger.debug("span %r opened with no active session; implicit session %s", name, session_id)

    ctx_trace_id = _trace_id.get()
    if ctx_trace_id is None:
        # First span for this operation in this process: mint or adopt a
        # trace_id, and -- only here -- an explicit parent_span_id from
        # another process is honored too. A nested span always inherits its
        # enclosing span's trace/parent instead, regardless of what's passed.
        resolved_trace_id = trace_id or _new_id()
        tok_trace = _trace_id.set(resolved_trace_id)
        resolved_parent_span_id = parent_span_id
    else:
        resolved_trace_id = ctx_trace_id
        resolved_parent_span_id = _parent_span_id.get()  # read BEFORE we overwrite it below

    attributes = {"name": name, **_coerce_attrs(attrs)}
    if implicit_session:
        attributes["implicit_session"] = "true"

    sp = Span(
        trace_id=resolved_trace_id,
        span_id=_new_id(),
        parent_span_id=resolved_parent_span_id,
        session_id=session_id,
        event_type=event_type,
        ts_epoch_ms=time.time_ns() // 1_000_000,
        attributes=attributes,
    )

    tok_parent = _parent_span_id.set(sp.span_id)  # children see us as their parent
    start = time.perf_counter()
    try:
        yield sp
    except BaseException as exc:
        # BaseException, not Exception: asyncio.CancelledError is a
        # BaseException, and a cancelled gateway request must record an error
        # rather than show as "ok". Same for KeyboardInterrupt.
        sp.status = "error"
        sp.attributes["error_class"] = type(exc).__name__
        # Truncated: an unbounded exception string in a map<string,string> is
        # how you end up producing a 4 MB Kafka message.
        sp.attributes["error_message"] = str(exc)[:_MAX_ERROR_MESSAGE]
        raise  # bare re-raise preserves the original traceback
    finally:
        sp.latency_ms = (time.perf_counter() - start) * 1000.0
        # Reset before emitting, so no emitter behaviour -- slow, raising, or
        # itself opening a span -- can leave a token unreset.
        _parent_span_id.reset(tok_parent)
        if tok_trace is not None:
            _trace_id.reset(tok_trace)
        if tok_session is not None:
            _session_id.reset(tok_session)
        _emit_event(sp.to_event())


# ---------------------------------------------------------------------------
# Emitters
# ---------------------------------------------------------------------------

# A module global is right for the emitter, and does not contradict the
# contextvars rule above: trace state is per-execution-context, but the emitter
# is per-process configuration. In a ContextVar, a configure() call in main()
# would be invisible to request handlers running in copied contexts.
_EMITTER: EmitFn | None = None  # None means "not configured yet"


def configure(emit_fn: EmitFn) -> None:
    """Install a custom emitter, replacing any previous one.

    Tests pass a list-collector -- ``list.append`` satisfies EmitFn
    structurally, so there is no fixture class to write::

        events: list[TraceEvent] = []
        configure(events.append)
    """
    global _EMITTER
    _EMITTER = emit_fn


def _emit_event(event: TraceEvent) -> None:
    """Emit one event. Never raises.

    This runs inside span()'s finally block. If the span body raised ValueError
    and the emitter then raised KafkaException, the Kafka error would *replace*
    the application's real exception as the one propagating. That alone settles
    the question: telemetry failures are logged, never raised. See ADR-000.
    """
    try:
        if _EMITTER is None:
            configure_kafka()  # lazy default: resolve config, still connect nothing
        assert _EMITTER is not None  # configure_kafka() always installs one
        _EMITTER(event)
    except Exception:
        logger.warning("failed to emit span %s", event["span_id"], exc_info=True)


# ---------------------------------------------------------------------------
# Kafka emitter
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _KafkaConfig:
    bootstrap_servers: str
    registry_url: str
    schema_path: str
    topic: str
    producer_config: dict[str, object]


_kafka_config: _KafkaConfig | None = None
_kafka_runtime: tuple[Producer, AvroSerializer] | None = None
_kafka_lock = threading.Lock()


def configure_kafka(
    bootstrap_servers: str | None = None,
    registry_url: str | None = None,
    schema_path: str | None = None,
    *,
    topic: str = TOPIC,
    extra_producer_config: Mapping[str, object] | None = None,
) -> None:
    """Emit to Kafka, keyed by session_id, with Schema Registry validation.

    Resolves configuration but builds nothing: the producer, the registry client
    and the schema file are all deferred to the first emit. Precedence is
    explicit argument, then environment variable, then localhost default.

    Env vars: AGENTLAKE_KAFKA, AGENTLAKE_REGISTRY, AGENTLAKE_SCHEMA.
    """
    global _kafka_config, _kafka_runtime, _EMITTER

    bootstrap = bootstrap_servers or os.environ.get("AGENTLAKE_KAFKA", "localhost:9092")
    _kafka_config = _KafkaConfig(
        bootstrap_servers=bootstrap,
        registry_url=registry_url or os.environ.get("AGENTLAKE_REGISTRY", "http://localhost:8081"),
        schema_path=schema_path
        or os.environ.get("AGENTLAKE_SCHEMA", "contracts/trace_event_v1.avsc"),
        topic=topic,
        producer_config={
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            **(extra_producer_config or {}),
        },
    )
    _kafka_runtime = None  # force a rebuild if we were reconfigured
    _EMITTER = _kafka_emit


def _get_kafka() -> tuple[Producer, AvroSerializer, str]:
    """Build the producer and serializer on first use, then reuse them.

    Everything expensive happens here and not at import: constructing the
    Producer spawns librdkafka's background threads and starts connecting, and
    the serializer talks to Schema Registry. Keeping it lazy is what makes
    ``import services.sdk`` safe in a test suite or a --help invocation.
    """
    global _kafka_runtime

    cfg = _kafka_config
    if cfg is None:  # pragma: no cover -- _emit_event configures before calling
        raise RuntimeError("configure_kafka() has not been called")

    # Locked so two gateway worker threads cannot each build a Producer and leak
    # one along with its background threads.
    with _kafka_lock:
        if _kafka_runtime is None:
            # Imported here, not at module scope, so the SDK stays usable with an
            # injected emitter even where confluent-kafka is not installed.
            from confluent_kafka import Producer
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer

            registry = SchemaRegistryClient({"url": cfg.registry_url})
            with open(cfg.schema_path, encoding="utf-8") as fh:
                serializer = AvroSerializer(registry, fh.read())
            producer = Producer(cfg.producer_config)
            _kafka_runtime = (producer, serializer)
            # Registered only once the runtime exists, so the exit handler never
            # has to guard against a producer that was never built.
            atexit.register(_flush_at_exit)
        runtime = _kafka_runtime

    return runtime[0], runtime[1], cfg.topic


def _on_delivery(err: object, msg: object) -> None:
    if err is not None:
        logger.warning("delivery failed: %s", err)


def _kafka_emit(event: TraceEvent) -> None:
    from confluent_kafka.serialization import MessageField, SerializationContext

    producer, serializer, topic = _get_kafka()
    producer.produce(
        topic=topic,
        key=event["session_id"].encode("utf-8"),  # key by session_id -> per-session ordering
        value=serializer(event, SerializationContext(topic, MessageField.VALUE)),
        on_delivery=_on_delivery,
    )
    # Delivery callbacks only fire from poll()/flush(). Without this a
    # long-running gateway accumulates them forever.
    producer.poll(0)


def flush(timeout: float = 5.0) -> int:
    """Block until queued events are delivered; return the number still in flight."""
    if _kafka_runtime is None:
        return 0
    return _kafka_runtime[0].flush(timeout)


def warmup() -> bool:
    """Force the Kafka producer/serializer to build now, instead of on the first span.

    Resolves ADR-000's open item: lazy init costs ~800ms, and without this the
    bill lands on whichever span happens to be first -- misleading root latency
    for the first trace of any process. Call this from a server's startup hook.

    Never raises, consistent with the rest of this module's swallow-and-log
    emit contract (ADR-000 #2). Returns True if the producer is ready, False if
    it could not be built (registry unreachable, bad schema path, ...); the
    caller should still start and simply pay the lazy-init cost on the first
    real span in that case.

    Only calls configure_kafka() if no emitter is installed yet. That call has
    the side effect of setting _EMITTER, which would otherwise silently
    overwrite a deliberately-configured custom emitter (e.g. a test's
    configure(events.append)) with the Kafka one.
    """
    if _EMITTER is None:
        configure_kafka()
    try:
        _get_kafka()
        return True
    except Exception:
        logger.warning("warmup failed; first span will pay the lazy-init cost", exc_info=True)
        return False


def _flush_at_exit() -> None:
    pending = flush()
    if pending:
        logger.warning("exiting with %d span(s) undelivered", pending)
