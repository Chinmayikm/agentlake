# ADR-001: Inference gateway design decisions

- **Status:** Accepted
- **Date:** 2026-08-25
- **Context:** `services/gateway/` -- a FastAPI service that is the only thing in this
  codebase allowed to hold `ANTHROPIC_API_KEY`, and the only thing allowed to call an
  LLM provider.

---

## 1. Single door: one gateway, not N direct callers

**Decision.** `services/agent/`, the eval harness, and everything else that ever needs an
LLM call goes through `POST /v1/chat` on this gateway. Nothing else imports `anthropic`
directly.

**Why.** Cost tracking, telemetry, retries and auth are cross-cutting concerns. Get them
right once here, and every future caller inherits them for free -- an `AGENT_STEP` calling
the gateway automatically gets a correctly-parented `LLM_CALL` span with real `cost_usd`,
with zero telemetry code in the agent itself. The alternative -- each caller wrapping its
own `anthropic.Anthropic()` in its own `span()` -- means N places to get span nesting,
cost math and error handling right instead of one, and N places for them to quietly drift
apart.

**Consequence.** The gateway is a genuine dependency for anything wanting to call an LLM
in this repo, not an optional convenience. `services/agent/` will call it over HTTP (or,
once that exists, import `services.gateway`'s helpers directly if it runs in-process) --
either way, it never constructs its own provider client.

---

## 2. Price-table versioning: `cost_usd` is only ever as good as its source

**Decision.** `services/gateway/models.yaml` carries a top-level `version` string. Every
`LLM_CALL` span stamps `attributes["price_table_version"]` with it (see
[chat.py](../../services/gateway/chat.py)'s `record_usage()`). `cost_usd` is computed from
this table only -- `services/gateway/pricing.py` is the one function that touches prices;
nothing hardcodes a number anywhere else.

**Why.** A `cost_usd` figure without a source is unauditable -- six months from now, "why
does this look off?" needs an answer better than "check git blame on a YAML file and hope
the LLM_CALL's timestamp lines up." Stamping the version directly onto the span means every
historical cost figure in Kafka/Iceberg/ClickHouse carries its own provenance, joinable
back to the exact price table that produced it, independent of when someone reads the code.

**The concrete case this exists for.** Seeded at `version: "2026-08-intro"`: Claude Sonnet
5 launched with an introductory rate ($2.00 / $10.00 per MTok) active only through
2026-08-31, after which the standard rate ($3.00 / $15.00) applies. There is no automatic
expiry mechanism -- `models.yaml` carries an explicit comment describing exactly what to
edit and when, but until a human does that on or after 2026-08-31, `cost_usd` will quietly
understate real spend. Versioning doesn't prevent that; it makes the eventual "our cost
dashboard doesn't match the invoice" investigation a five-minute `price_table_version`
lookup instead of an afternoon.

---

## 3. ADR-000's open item, resolved: `warmup()`

**Decision.** `services.sdk.warmup()` (new, thin) is called from the gateway's FastAPI
`lifespan` at startup, forcing the Kafka producer/serializer to build before the first
request is served. `flush()` runs on shutdown.

**Why this was open.** ADR-000 #3 measured the lazy Kafka init at ~800ms, landing on
whichever span happened to be first -- misleading root latency on the very first trace of
any process. The gateway is the first real (non-demo) caller, so it's where this gets
fixed properly instead of deferred again.

**Design constraint: must not clobber a configured emitter.** `warmup()` only calls
`configure_kafka()` -- which has the side effect of installing `_kafka_emit` as `_EMITTER`
-- when no emitter is installed yet. Without that guard, calling `warmup()` after a test
had already called `configure(events.append)` would silently swap the test's collector
back to the Kafka emitter. This is also what makes `warmup()` compose with the existing
autouse `_no_kafka` fixture in `tests/conftest.py` with zero test changes: `_get_kafka` is
monkeypatched to raise `AssertionError`, which `warmup()`'s `except Exception` catches
(it's a subclass), logs, and turns into a `False` return -- no network, no new fixture.

**Never raises.** Consistent with ADR-000 #2: telemetry failures are swallowed and logged,
never allowed to crash the application. A `False` return means the caller starts anyway and
simply pays the lazy-init cost on the first real span -- exactly the pre-`warmup()`
behavior, just opt-in per-caller instead of universal.

**What `/v1/health` reports.** `kafka_warmed` is captured *once*, from `warmup()`'s return
value at startup, not re-checked per request -- `_kafka_runtime`, once built, lives for the
process lifetime. No new SDK read accessor was added for this (e.g. an `is_kafka_warm()`);
`warmup()`'s own return value was sufficient, so the SDK's public surface grew by exactly
one function.

**A note on what "warmed" means.** Constructing the `confluent_kafka.Producer` does not
wait for a broker connection to succeed -- librdkafka connects on background threads and
retries independently. `warmup() -> True` means the expensive client objects were built
without raising, not that Kafka is confirmed reachable. That distinction matters less than
it sounds: either way, emit failures are swallowed and logged (ADR-000 #2), so a broker
that's down at startup and comes up later self-heals with no gateway-side action.

---

## 4. Alias indirection: callers never see a provider model string

**Decision.** `/v1/chat` takes `model_alias` (`"fast"` / `"quality"`), never a raw
`provider_model_id`. `services/gateway/pricing.py` is the only place that maps one to the
other.

**Why.** A model swap -- Haiku's next version, a provider price change, retiring an
alias -- becomes a one-line edit to `models.yaml` with no code touched and no caller
updated. It also means every cost figure is automatically re-baselined against the new
model the moment the table changes, since `cost_usd` is computed from the same table the
alias resolved through. The cost of this indirection is one extra lookup and a 400 for an
unrecognized alias (validated before any span opens -- see `chat.py`); the alternative
(callers naming `claude-haiku-4-5` directly) means a provider-side model retirement is a
grep-and-replace across every caller instead of a single YAML edit.
