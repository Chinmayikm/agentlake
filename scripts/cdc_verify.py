#!/usr/bin/env python3
"""Read the CDC topic and print what Debezium actually put on it.

    python scripts/cdc_verify.py topic
    python scripts/cdc_verify.py topic --topic cdc.metadata.eval_runs

Needs Kafka. Does NOT need Postgres, Connect or Trino -- the topic is the
record, which is the whole point of putting a log between the database and the
warehouse.

Why this exists next to `make cdc-land`
---------------------------------------
The lander tells you what it wrote to Iceberg; this tells you what the connector
emitted, which is the other half of any "is the pipeline working" question. When
they disagree the difference is the lander's, and when they agree the difference
is upstream. Read-only and consumer-group-less -- it assigns partitions
directly and never commits, so running it cannot move `make cdc-land`'s resume
point. That is the same instinct as kafka-ui being read-only (docker-compose.yml).

The three counts at the end are the ones worth reading:

    rows       records that carry a row image and become Iceberg rows
    tombstones null-valued records; a delete emits one AFTER its op='d' record,
               for log compaction, and it carries nothing that record does not
    other      op='t' (TRUNCATE) / op='m' (logical decoding message)

`rows + tombstones + other` must equal the topic's own record count. If it does
not, something was skipped for a reason this script does not know about.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.cdc_land import CONSUMER_GROUP, TOPIC  # noqa: E402

DEFAULT_KAFKA = os.environ.get("AGENTLAKE_KAFKA", "localhost:9092")


def cmd_topic(args: argparse.Namespace) -> int:
    from confluent_kafka import Consumer, KafkaError, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": args.kafka,
            # A group id is required by librdkafka even when nothing is
            # committed. Deliberately NOT cdc_land.py's group: assign() plus no
            # commit means this cannot move the lander's resume point, but
            # sharing the name would still show up in `kafka-consumer-groups
            # --describe` as a second member and confuse the next person.
            "group.id": f"{CONSUMER_GROUP}-inspect",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(args.topic, timeout=10.0)
        topic_meta = metadata.topics.get(args.topic)
        if topic_meta is None or topic_meta.error:
            print(f"topic {args.topic} does not exist on {args.kafka}", file=sys.stderr)
            return 1
        consumer.assign([TopicPartition(args.topic, p, 0) for p in topic_meta.partitions])

        header = (
            f"{'off':>5} {'part':>4}  {'op':<3} {'snapshot':<24} {'lsn':>10} "
            f"{'id':>4} {'version':<10} payload"
        )
        print(f"{args.topic} on {args.kafka}\n")
        print(header)
        print("-" * len(header))

        rows = tombstones = other = 0
        while True:
            message = consumer.poll(timeout=args.poll_timeout)
            if message is None:
                break
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"kafka: {message.error()}", file=sys.stderr)
                return 1

            offset, partition = message.offset(), message.partition()
            if message.value() is None:
                tombstones += 1
                key = json.loads(message.key()) if message.key() else {}
                print(
                    f"{offset:>5} {partition:>4}  {'-':<3} {'(tombstone)':<24} {'':>10} "
                    f"{key.get('id')!s:>4} {'':<10} null value, for log compaction"
                )
                continue

            envelope = json.loads(message.value())
            op = envelope.get("op", "?")
            source = envelope.get("source") or {}
            image = envelope.get("after") or envelope.get("before") or {}
            if op in ("c", "u", "d", "r"):
                rows += 1
            else:
                other += 1
            payload = str(image.get("template_text") or "")[: args.width]
            print(
                f"{offset:>5} {partition:>4}  {op:<3} {source.get('snapshot')!s:<24} "
                f"{source.get('lsn')!s:>10} {image.get('id')!s:>4} "
                f"{image.get('version')!s:<10} {payload}"
            )
    finally:
        consumer.close()

    total = rows + tombstones + other
    print(f"\n{total} record(s): {rows} with a row image, {tombstones} tombstone(s), "
          f"{other} other (truncate / message)")
    print(f"scripts/cdc_land.py lands the {rows} with a row image and counts the rest.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    topic = sub.add_parser("topic", help="print every record on a CDC topic")
    topic.add_argument("--kafka", default=DEFAULT_KAFKA)
    topic.add_argument("--topic", default=TOPIC)
    topic.add_argument("--poll-timeout", type=float, default=5.0)
    topic.add_argument("--width", type=int, default=44, help="template_text preview width")
    topic.set_defaults(func=cmd_topic)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
