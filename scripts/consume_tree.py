#!/usr/bin/env python3
"""Trace tree viewer -- consume traces.events.v1 and print each TraceEvent as
an indented line, live, until Ctrl+C.

Promoted from a /tmp scratch script to a permanent dev tool: `make traces` or
`python3 scripts/consume_tree.py`.

Indentation is one level for any event with parent_span_id set (a child span),
none for a root span -- this is a streaming printer, not a buffered tree
renderer, so it doesn't wait to reassemble ancestry before printing (and it
couldn't correctly anyway: a span's own `with` block emits on exit, so a
child's event reaches Kafka *before* its parent's -- see services/sdk/telemetry.py).

Usage::

    python3 scripts/consume_tree.py                  # replay everything, live-tail after
    python3 scripts/consume_tree.py --from-latest     # only new events from now on
    python3 scripts/consume_tree.py --group-id my-group   # reuse committed offsets

Default --group-id is randomized per run (tree-view-<8 hex chars>) so a fresh
consumer group -- and therefore a full replay from earliest -- is what you get
by default. Passing a fixed group name opts back into normal committed-offset
behavior, which is also how you can end up staring at an empty topic: a
previous run under that same group already consumed and committed past
everything currently on the topic.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

DEFAULT_TOPIC = "traces.events.v1"
DEFAULT_BOOTSTRAP = os.environ.get("AGENTLAKE_KAFKA", "localhost:9092")
DEFAULT_REGISTRY = os.environ.get("AGENTLAKE_REGISTRY", "http://localhost:8081")


def default_group_id() -> str:
    return f"tree-view-{uuid.uuid4().hex[:8]}"


def format_event(event: dict) -> str:
    parent_id = event["parent_span_id"]
    indent = "  └─ " if parent_id else ""
    parent = parent_id[:8] if parent_id else "--------"
    return (
        f"{indent}{event['event_type']:<10} "
        f"span={event['span_id'][:8]} parent={parent} trace={event['trace_id'][:8]} "
        f"session={event['session_id']} {event['latency_ms']:.2f}ms"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers")
    parser.add_argument("--registry", default=DEFAULT_REGISTRY, help="Schema Registry URL")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--group-id",
        default=None,
        help="consumer group id (default: randomized per run, see module docstring)",
    )
    parser.add_argument(
        "--from-latest",
        action="store_true",
        help="only tail new events from now on, instead of replaying from earliest",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    group_id = args.group_id or default_group_id()
    offset_reset = "latest" if args.from_latest else "earliest"

    registry = SchemaRegistryClient({"url": args.registry})
    deserializer = AvroDeserializer(registry)

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": group_id,
            "auto.offset.reset": offset_reset,
        }
    )
    consumer.subscribe([args.topic])

    print(
        f"listening on {args.topic} @ {args.bootstrap} as group {group_id!r} "
        f"(from {offset_reset}) -- Ctrl+C to stop",
        flush=True,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}", file=sys.stderr, flush=True)
                continue
            event = deserializer(msg.value(), SerializationContext(args.topic, MessageField.VALUE))
            print(format_event(event), flush=True)
    except KeyboardInterrupt:
        print("\nstopped.", flush=True)
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
