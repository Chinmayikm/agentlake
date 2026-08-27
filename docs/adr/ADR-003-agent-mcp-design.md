# ADR-003: Agent + MCP server design decisions

- **Status:** Accepted
- **Date:** 2026-08-27
- **Context:** `services/mcp_server/` -- an MCP server exposing `search_docs`
  (wraps `services.rag.retrieve()`) and two honest-stub tools, `get_trace` and
  `query_metrics`, pending ClickHouse. `services/agent/` -- a bounded CLI
  agent, the first real consumer of the gateway (ADR-001) and the RAG corpus
  (ADR-002) together, and the first thing in this repo to run as more than one
  process for a single logical turn.

---

## 1. The agent is a real MCP client over stdio, not a `retrieve()` import

**Decision.** `services/agent` never imports `services.rag`. It spawns
`python -m services.mcp_server` as a subprocess (`StdioServerParameters` +
`stdio_client` + `ClientSession`, all from the `mcp` package) and calls tools
through the actual JSON-RPC-over-stdio protocol.

**Why.** Two things this repo is meant to demonstrate only hold if the
protocol boundary is real: that `services/mcp_server` is genuinely usable "from
any MCP client," and that the agent only knows tool *contracts*
(name/description/JSON Schema), not RAG internals. A direct `retrieve()`
import would make both of those true only by convention, not by construction
-- the moment `search_docs`'s signature and `retrieve()`'s signature drifted,
nothing would fail until someone actually tried a different client. The two
processes run on the same machine as siblings today, but the pattern is the
same one a networked MCP server would use.

**Cost accepted.** A subprocess spawn and stdio round-trip per tool call, and
one more process boundary that session_id and trace context both have to be
carried across explicitly (see #4). Both are small next to what would be
lost by faking the boundary.

---

## 2. Bounded-loop semantics: never hang, never crash, never return nothing

**Decision.** `run_turn()` (`services/agent/loop.py`) caps the loop at
`max_steps` (default 8) model/tool round trips. Each tool call gets its own
`asyncio.wait_for(..., timeout=tool_timeout)` (default 15s). Any tool failure
-- timeout, exception, or an honest-stub `{"error": ...}` result -- is
serialized as a `tool_result` observation and fed back to the model; nothing
from a tool call is ever allowed to propagate as a Python exception out of the
loop. If the budget is exhausted without the model producing a final text
answer, the loop makes exactly one more gateway call with no `tools` offered
and an explicit "give your best answer now" instruction, so the model
literally cannot ask for another tool -- and marks the result `truncated=True`.

**Why.** An agent that can hang on a slow tool, crash on a flaky one, or spin
forever chasing one more tool call is worse than one that degrades visibly.
Every one of these failure modes is something a *real* dependency (the
gateway, the MCP server, the docs corpus) will eventually produce in practice,
not a hypothetical -- so the loop treats "the tool did not give me a usable
answer" as the normal case to design for, not the exception. No retries here
(explicitly out of scope) -- a single bounded attempt per step is enough to
guarantee termination and an answer; retry/backoff policy is separate
hardening work.

---

## 3. `get_trace` / `query_metrics` are honest stubs, not mocked data

**Decision.** Both tools always return a structured `{"error": "... not yet
available (ClickHouse lands Day 3)"}`, with that behavior stated up front in
both the tool description (what the model sees) and the docstring (what a
future maintainer sees) -- never a plausible-looking fabricated number or
trace.

**Why.** This follows directly from ADR-002 #4's theme: an observability
platform's own instrumentation can't be the one place that lies. A `get_trace`
that silently returned an empty or synthetic trace would be worse than one
that errors, because the failure would be invisible -- the model (and a human
reading its answer) would have no way to distinguish "this trace genuinely has
no interesting spans" from "the trace store doesn't exist yet." A structured
error the model can reason about ("I don't have access to real trace data
right now") is strictly more useful than a number that looks real and isn't.
The docstrings are written as the permanent contract for these tools, not a
`# TODO`: the `{"error": ...}` shape is the correct response whenever the
trace/metrics store is unreachable, which will still be true after ClickHouse
lands.

---

## 4. Session *and* trace propagate across process boundaries

**Originally decided (superseded below).** The first version of this ADR kept
`trace_id`/`parent_span_id` per-process by design, deferring W3C-style trace
propagation as "a real capability, but a separate one, not needed for
anything this PR does." The first live agent turn against the real stack
falsified that "not needed": the tree viewer (`scripts/consume_tree.py`)
showed one turn as **six separate traces** -- `AGENT_STEP`, each `GATEWAY`
call, and each MCP-server `TOOL_CALL` all rooted their own, joined only by
`session_id`. `get_trace(trace_id)` would have returned a fragment of the
turn, not the turn -- which is exactly the tool `services/mcp_server` exists
to eventually serve for real. That's not a hypothetical the deferral could
outrun.

**Decision.** Both `session_id` and trace context (`trace_id` +
`parent_span_id`) now propagate across every process boundary the agent
crosses, so one turn is one trace end to end:

- **`services/sdk`**: `span()` gained optional `trace_id`/`parent_span_id`
  keyword arguments, and `current_parent_span_id()` joins the existing
  `current_trace_id()`/`current_session_id()` accessors. Both explicit
  arguments are used only as a fallback for a process's *first* span --
  ordinary same-process nesting always wins, so passing them is safe even
  from code that might sometimes run nested or standalone.
- **Agent -> gateway (HTTP)**: `X-Session-Id` (unchanged) plus new
  `X-Trace-Id` / `X-Parent-Span-Id` headers, read from
  `current_trace_id()`/`current_parent_span_id()` at the point of each
  gateway call. `services/gateway/chat.py` passes them straight into its
  `GATEWAY` span's new `trace_id`/`parent_span_id` arguments; `LLM_CALL`
  nests under it exactly as before.
- **Agent -> MCP server (stdio)**: `AGENTLAKE_SESSION_ID` (unchanged) plus a
  new `_trace_context: {"trace_id", "parent_span_id"}` sidecar key added to
  each tool call's `arguments` by `mcp_client.py`. `server.py`'s
  `dispatch_tool()` pops it before validation/dispatch (every tool schema
  sets `additionalProperties: false`, so an unstripped sidecar would itself
  fail validation) and passes it into the `TOOL_CALL` span it opens.

**No client-side `TOOL_CALL` span.** `services/agent/loop.py` does not wrap
each tool call in its own span. The MCP server's `TOOL_CALL` (now correctly
parented under `AGENT_STEP` via the mechanism above) is the sole span for
that call; a client-side one would nest as `TOOL_CALL` under `TOOL_CALL`
instead of both landing as `AGENT_STEP`'s direct children. What the loop
*does* still do in-process is exactly what made the fix provable without a
real subprocess: at the moment it calls `tool_executor.call_tool()`, the
ambient `(current_trace_id(), current_parent_span_id())` already equals
`AGENT_STEP`'s own `(trace_id, span_id)` -- asserted directly in
`tests/test_agent.py`, since that invariant is exactly what a real
`StdioToolExecutor` reads to build `_trace_context`.

**Result.** `AGENT_STEP` (root) -> `GATEWAY` and `TOOL_CALL` (children) ->
`LLM_CALL` and `RETRIEVAL` (grandchildren), all one `trace_id`. `session_id`
still does the cross-*turn* reassembly job ADR-000 #1 gave it; `trace_id` now
also does the cross-*process* job within one turn, which is what
`get_trace(trace_id)` needs once it has a real backing store.

---

## 5. Gateway extension: `tools` passthrough, not a new capability

**Decision.** `ChatRequest` (`services/gateway/chat.py`) gains one optional
field, `tools: list[dict] | None`, forwarded verbatim to
`client.messages.create(tools=...)` when present. Nothing else about the
gateway changes.

**Why this doesn't violate ADR-001.** ADR-001 #1's rule is "nothing but the
gateway calls the provider" -- it says nothing about which Messages API
parameters the gateway is allowed to forward. Anthropic-style tool use is a
property of a single `messages.create` call, not a new class of capability
requiring its own cost/telemetry/auth handling; `cost_usd` computation,
`price_table_version` stamping, and error mapping all already work unchanged
whether or not `tools` was set. The alternative -- `services/agent` calling
`anthropic` directly for tool-use turns because the gateway "doesn't support
it yet" -- is exactly the single-door violation ADR-001 exists to prevent.

---

## 6. Recurrence: the embedding-model observer-effect, second organ

**Problem.** The first live `RETRIEVAL` span (via `search_docs`) measured
7.3s; the second measured ~1s. `services/mcp_server` had no equivalent of the
gateway's `warmup()` (ADR-000 #3): `search_docs`'s first call was the first
thing to construct a `FastEmbedEmbedder`, so it paid fastembed's ONNX model
load cost inline, exactly the shape ADR-000 #3 already diagnosed and fixed
once for the gateway's lazy Kafka producer.

**Decision.** `services/mcp_server/server.py` gained a `warmup()`, called
from `run_stdio()` before the server starts accepting connections: it embeds
a throwaway string (forces the model to load) and calls
`QdrantStore().count_chunks()` (forces the qdrant-client connection to
establish). Same contract as the gateway's `warmup()` -- never raises; a
failure (Qdrant unreachable, model not cached) just means the process starts
anyway and the first real `search_docs` call pays the lazy-init cost, same as
before this existed.

**Why this is a recurrence, not a new lesson.** Two different one-time costs
(a Kafka producer's TCP/registry handshake, an ONNX model's disk load), two
different subsystems, same root cause: lazy initialization inside the first
*measured* operation makes the telemetry describing that operation lie about
what it measured. The fix generalizes the same way both times -- move the
one-time cost to process start, where nothing is being measured yet.
