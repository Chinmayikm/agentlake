#!/usr/bin/env python3
"""Generate synthetic agent traffic on traces.events.v1, for exercising the
cold path (ADR-004).

    python scripts/gen_traffic.py --events 600

Emits through the real SDK -- ``services.sdk``'s session()/span() -- rather than
a hand-rolled producer, so what lands on the topic is byte-identical to what a
real agent turn produces: same Avro contract, same session_id partition key,
same nesting, same error path. A generator that wrote its own records would
prove the pipeline can read the generator, not that it can read agentlake.

Shape of the traffic: sessions of a few turns each, every turn an AGENT_STEP
with a RETRIEVAL and an LLM_CALL child, spread across the model aliases in
services/gateway/models.yaml. A fraction of turns raise inside a TOOL_CALL so
the topic carries status='error' rows for the error_count aggregate.

Prints the exact number of spans emitted -- that count is what the verification
queries in stream/flink/verify/ are checked against.
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from services.sdk import flush, session, span, warmup  # noqa: E402

MODELS_YAML = REPO_ROOT / "services" / "gateway" / "models.yaml"

# Every turn emits this many spans, so --events maps to a whole number of turns.
SPANS_PER_OK_TURN = 3     # AGENT_STEP + RETRIEVAL + LLM_CALL
SPANS_PER_ERR_TURN = 2    # AGENT_STEP + TOOL_CALL, both status=error

QUESTIONS = [
    "what is agentlake?",
    "how does the avro contract evolve?",
    "which partition key does the sdk use?",
    "what closes a tumbling window?",
    "why is the gateway the only thing holding the api key?",
]


def load_models() -> list[str]:
    """Real model ids from services/gateway/models.yaml, so `model` is a
    dimension worth grouping by rather than an invented string."""
    try:
        doc = yaml.safe_load(MODELS_YAML.read_text(encoding="utf-8")) or {}
    except OSError:
        return ["claude-haiku-4-5"]
    aliases = doc.get("aliases") or {}
    ids = sorted(
        str(spec["provider_model_id"])
        for spec in aliases.values()
        if isinstance(spec, dict) and spec.get("provider_model_id")
    )
    return ids or ["claude-haiku-4-5"]


def ok_turn(turn: int, model: str) -> None:
    with span("AGENT_STEP", "agent_turn", turn=turn, question=random.choice(QUESTIONS)) as step:
        with span("RETRIEVAL", "vector_search", index="docs-v1", top_k=4) as retrieval:
            time.sleep(random.uniform(0.001, 0.006))
            retrieval.set(hits=random.randint(1, 4))
        with span("LLM_CALL", "chat_completion") as llm:
            time.sleep(random.uniform(0.002, 0.012))
            prompt_tokens = random.randint(180, 420)
            completion_tokens = random.randint(40, 160)
            llm.set(
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost_usd=round(prompt_tokens * 1e-6 + completion_tokens * 5e-6, 6),
                finish_reason="stop",
            )
        step.set(turn_result="answered")


def error_turn(turn: int) -> None:
    """A turn whose tool call raises. Both spans emit with status='error'."""
    try:
        # Deliberately nested, not flattened: the TOOL_CALL span has to be a
        # *child* of the AGENT_STEP for parent_span_id to be set. `with a, b:`
        # would produce the same two spans with the same nesting, but reads as
        # if they were siblings -- see the SIM117 note in pyproject.toml.
        with span("AGENT_STEP", "agent_turn", turn=turn):  # noqa: SIM117
            with span("TOOL_CALL", "lookup_order", tool="orders_api"):
                time.sleep(random.uniform(0.001, 0.004))
                raise TimeoutError("orders_api did not respond in 2s")
    except TimeoutError:
        pass  # the spans already emitted; the raise was the point


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--events", type=int, default=600, help="approximate spans to emit (default 600)"
    )
    parser.add_argument(
        "--sessions", type=int, default=20, help="spread across this many sessions (default 20)"
    )
    parser.add_argument(
        "--error-rate", type=float, default=0.08, help="fraction of turns that raise (default 0.08)"
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="seed the RNG for a reproducible run"
    )
    parser.add_argument(
        "--spread",
        type=float,
        default=0.0,
        help="stretch the run over roughly this many seconds instead of emitting "
        "as fast as possible. With --sessions 1 this produces a trickle into a "
        "single Kafka partition, which is what makes source idleness observable: "
        "the other partitions fall idle and the watermark has to advance from "
        "this one alone. See ADR-004 #3.",
    )
    args = parser.parse_args(argv)

    if args.seed is not None:
        random.seed(args.seed)

    models = load_models()
    # Pay the ~800ms producer/serializer build up front rather than charging it
    # to the first span's latency_ms -- see ADR-000 #3.
    warmup()

    emitted = 0
    turn = 0
    turns_per_session = max(1, (args.events // SPANS_PER_OK_TURN) // max(1, args.sessions))
    total_turns = max(1, args.events // SPANS_PER_OK_TURN)
    pause = args.spread / total_turns if args.spread else 0.0

    while emitted < args.events:
        with session():
            for _ in range(turns_per_session):
                if emitted >= args.events:
                    break
                turn += 1
                if random.random() < args.error_rate:
                    error_turn(turn)
                    emitted += SPANS_PER_ERR_TURN
                else:
                    ok_turn(turn, random.choice(models))
                    emitted += SPANS_PER_OK_TURN
                if pause:
                    time.sleep(pause)

    pending = flush(timeout=30.0)
    print(f"models:   {', '.join(models)}")
    print(f"turns:    {turn}")
    print(f"PRODUCED: {emitted} spans ({pending} still pending after flush)")
    return 1 if pending else 0


if __name__ == "__main__":
    sys.exit(main())
