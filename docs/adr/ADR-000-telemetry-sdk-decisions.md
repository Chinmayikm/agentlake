# ADR-000: Telemetry SDK design decisions

- **Status:** Accepted
- **Date:** 2026-08-20
- **Context:** `services/sdk/` — the library every agentlake component imports to emit
  TraceEvents to `traces.events.v1`.

Four decisions that are not obvious from reading the code, recorded so they can be
whiteboarded rather than rediscovered.

---

## 1. Trace scope is per turn, not per session

**Decision.** A `session_id` covers one conversation. A `trace_id` covers one top-level
operation within it — one agent turn. `session()` clears the trace context on entry; the
outermost `span()` creates the trace_id and is the only span that clears it on exit, so the
next top-level span in the same session starts a fresh trace. Spans nested inside a turn
inherit that turn's trace_id and reset nothing.

**Why.** This follows the OpenTelemetry convention that a trace is one operation, not one
user's lifetime. It makes `get_trace(trace_id)` return exactly one turn — a bounded, useful
unit for a waterfall view — while `WHERE session_id = ...` still reassembles the whole
conversation. The alternative (one trace per session) makes traces grow without bound over a
long chat and gives the trace view no natural unit.

**Mechanism.** In `span()`, `tok_trace` is `Token | None`. It is set only by the span that
created the trace_id; nested spans leave it `None` and skip the reset in their `finally`. That
one detail is what scopes a trace to a turn — an inner span exiting can never end the trace its
parent owns, and the parent exiting always ends it.

**Consequence.** Cross-turn causality is expressed by `session_id`, not by trace. Anything
wanting "the whole conversation as one graph" joins on `session_id` and orders by `ts_epoch_ms`.

---

## 2. Emit failures are swallowed and logged, never raised

**Decision.** `_emit_event()` catches `Exception`, logs at WARNING with `exc_info`, and
continues. There is no strict/re-raise mode.

**Why — the mechanical reason.** The emit happens in `span()`'s `finally` block. If the span
body raised `ValueError` and the emitter then raised `KafkaException`, the Kafka error would
**replace** the application's real exception as the one propagating; the original would survive
only as `__context__`. A telemetry fault would mask the actual bug. That settles it on its own,
independent of any philosophy about observability.

**Why — the supporting reasons.** Telemetry is not the workload: if Schema Registry is down the
agent should still answer. And propagating buys little anyway, because `producer.produce()` is
asynchronous — broker unreachable, leader election and replication failures all arrive in the
delivery callback, not as a raise. The only synchronous raises are local queue-full and
serializer errors, neither of which the calling code could sensibly handle inline.

**Guarding against silence.** The failure mode of this decision is discovering at 2am that
telemetry has been dead for a week. So the SDK uses `logging.getLogger("agentlake.sdk")` and
deliberately attaches **no** `NullHandler`. With no handler configured anywhere, logging's
`lastResort` prints WARNING+ to stderr, so a broken emitter is visible out of the box. An
application that configures logging takes over as normal. Delivery-callback failures log at
WARNING too.

**Rejected:** a `strict=True` flag that re-raises, for tests. Test collectors (`list.append`)
do not raise, so it would almost never fire, and contract conformance is proved far better by
validating emitted dicts against the `.avsc` with `fastavro.validate` — no Kafka required.
One behaviour is less API to explain.

---

## 3. The default emitter lazily auto-configures Kafka

**Decision.** `_EMITTER` starts as `None`. On the first emit, `_emit_event()` calls
`configure_kafka()` itself. Producing to Kafka is what an unconfigured SDK does.

**Why.** Nothing gets silently dropped by default: forgetting to call `configure_kafka()` in a
new service still produces telemetry rather than a silent void. Tests opt out explicitly with
`configure(events.append)`.

**Laziness is two-stage, and requirement 5 constrains both.**

1. `configure_kafka()` resolves argument > env var > localhost default into a frozen
   `_KafkaConfig` and **builds nothing**. Resolving at call time (not first emit) means "which
   broker am I talking to?" has an answer that does not depend on when the first span happened
   to fire.
2. `_get_kafka()` constructs the `SchemaRegistryClient`, reads the `.avsc`, and builds the
   `AvroSerializer` and `Producer` on the **first emit**. This is the expensive part —
   constructing a Producer spawns librdkafka's background threads and starts connecting.

Even `import confluent_kafka` is deferred into `_get_kafka()`, which additionally lets the SDK
run with an injected emitter in environments where confluent-kafka is not installed. Verified:
`import services.sdk` leaves `confluent_kafka` absent from `sys.modules`.

A `threading.Lock` guards the build so two gateway worker threads cannot each construct a
Producer and leak one plus its threads. `atexit.register(_flush_at_exit)` is called *inside* the
builder, after the runtime exists — registering it in `configure_kafka()` would register a flush
for a producer that may never be built.

**Known cost — the first parent span absorbs the init.** Measured locally: `_get_kafka()` ≈
690ms, first emit (schema registration round-trip) ≈ 126ms, subsequent emits ≈ 0.3ms. Because
children emit from their own `finally` while the parent is still open, that one-time ~800ms is
charged to the **first top-level span's** `latency_ms`, not to the child that triggered it. In
the demo, turn 1's `AGENT_STEP` reports ~2.4s against ~190ms of real work; turn 2 reports ~83ms.

This is inherent to lazy initialisation and is one-time per process, but it means the first
trace of any process has a misleading root latency. If that becomes a problem — a gateway where
the first user request must not eat 800ms — the fix is an explicit warm-up call to `_get_kafka()`
from FastAPI's `lifespan` startup hook, which pays the cost before serving traffic without
making anything happen at import time. Not added yet; revisit when the gateway lands.

---

## 4. IDs are full `uuid4().hex`, never truncated

**Decision.** One helper, `_new_id() -> uuid.uuid4().hex`, used for `trace_id`, `span_id` and
generated `session_id`. 32 hex characters, no dashes.

**Why not the 8-char form.** `services/emit_test.py` (an hour-one smoke script) used
`str(uuid.uuid4())[:8]` for `span_id`. That is 32 bits — a space of 4.29e9. By the birthday
bound, a 50% chance of at least one collision arrives at roughly

```
n ≈ 1.177 × sqrt(4.29e9) ≈ 77,000 spans
```

and a 1% chance at roughly `sqrt(2 × 4.29e9 × 0.01) ≈ 9,300 spans`. The demo emits 8 spans per
run; a single overnight load test clears 77,000 comfortably. Full `uuid4()` carries 122 random
bits, putting the 50% point near 2^61 ≈ 2.3e18.

The severity matters more than the probability. A duplicate `span_id` does not announce itself —
it silently corrupts parent/child joins, so one turn's children attach to another turn's parent.
That surfaces as a *logic* bug in the agent, and you would look everywhere before suspecting the
ID width.

**Why one shape for all three.** Uniform length means one column type, one index and one set of
assumptions across dbt, Iceberg and ClickHouse. Two ID shapes in the same table is how you get a
join that silently returns nothing. `.hex` over `str(uuid4())` drops the dashes: no format or
case ambiguity, smaller on the wire, and it matches how OpenTelemetry renders trace IDs.

Readability of long IDs is a *display* concern — `demo_sdk.py` can print `sid[:8]`. Do not
truncate the stored data to make output pretty.

**Future.** Because generation is one function, switching to ULIDs (time-sortable, better
clustering for Iceberg) is a one-line change.
