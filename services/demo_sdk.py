"""Demo: nested agent spans emitted through the SDK to Kafka.

Run from the repo root, with the compose stack up::

    docker compose up -d
    python -m services.demo_sdk

Shows the scope rule the SDK enforces: **session = one conversation, trace =
one turn's causal graph.** The session_id printed below stays constant across
turns, while each turn gets its own trace_id -- so get_trace(trace_id) returns
exactly one turn, and filtering on session_id reassembles the conversation.

The last turn deliberately raises, to show that the span still emits with
status="error" and a recorded latency while the exception propagates normally.
"""

import random
import time

from services.sdk import configure_kafka, current_trace_id, flush, session, span


def agent_turn(turn: int, question: str) -> None:
    """One AGENT_STEP with a RETRIEVAL child and an LLM_CALL child."""
    with span("AGENT_STEP", "agent_turn", turn=turn, question=question) as step:
        # A new top-level span, so this turn owns a fresh trace_id. Both child
        # spans below inherit it.
        print(f"  turn {turn}: trace_id={current_trace_id()}")

        with span("RETRIEVAL", "vector_search", index="docs-v1", top_k=4) as retrieval:
            time.sleep(random.uniform(0.01, 0.05))
            retrieval.set(hits=4)  # int -> attributes["hits"] == "4"

        with span("LLM_CALL", "chat_completion") as llm:
            time.sleep(random.uniform(0.05, 0.15))
            prompt_tokens = random.randint(180, 420)
            completion_tokens = random.randint(40, 160)
            llm.set(
                model="claude-haiku-4-5",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost_usd=round(prompt_tokens * 1e-6 + completion_tokens * 5e-6, 6),
                finish_reason="stop",  # not an Avro field -> attributes
            )

        step.set(turn_result="answered")


def failing_turn(turn: int) -> None:
    """A turn whose tool call blows up, to exercise the error path."""
    with span("AGENT_STEP", "agent_turn", turn=turn):
        print(f"  turn {turn}: trace_id={current_trace_id()} (this one raises)")
        with span("TOOL_CALL", "lookup_order", tool="orders_api"):
            time.sleep(0.02)
            raise TimeoutError("orders_api did not respond in 2s")


def main() -> None:
    configure_kafka()  # resolves env vars; connects nothing until the first emit

    with session() as session_id:
        print(f"session_id = {session_id}")
        agent_turn(1, "what is agentlake?")
        agent_turn(2, "how does the avro contract evolve?")
        try:
            failing_turn(3)
        except TimeoutError as exc:
            # The span already emitted with status="error"; the exception still
            # reached us, which is the point.
            print(f"  turn 3 raised as expected: {exc}")

    pending = flush()
    print(f"done: 8 spans emitted across 3 traces, {pending} pending")


if __name__ == "__main__":
    main()
