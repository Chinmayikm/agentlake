"""Hour-one smoke test: produce contract-validated TraceEvents to Kafka."""
import random
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "traces.events.v1"

sr = SchemaRegistryClient({"url": "http://localhost:8081"})
with open("contracts/trace_event_v1.avsc") as f:
    avro_ser = AvroSerializer(sr, f.read())

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all",
})

def delivery(err, msg):
    if err:
        print(f"DELIVERY FAILED: {err}")
    else:
        print(f"ok  partition={msg.partition()} offset={msg.offset()} key={msg.key().decode()}")

def make_event(session_id: str, event_type: str) -> dict:
    return {
        "trace_id": str(uuid.uuid4()),
        "span_id": str(uuid.uuid4())[:8],
        "parent_span_id": None,
        "session_id": session_id,
        "event_type": event_type,
        "model": "claude-haiku" if event_type == "LLM_CALL" else None,
        "prompt_tokens": random.randint(50, 500) if event_type == "LLM_CALL" else None,
        "completion_tokens": random.randint(20, 300) if event_type == "LLM_CALL" else None,
        "latency_ms": round(random.uniform(5, 900), 2),
        "cost_usd": round(random.uniform(0.0001, 0.01), 6) if event_type == "LLM_CALL" else None,
        "status": "ok",
        "ts_epoch_ms": int(time.time() * 1000),
        "attributes": {"source": "emit_test"},
    }

if __name__ == "__main__":
    sessions = [f"session-{i}" for i in range(3)]
    types = ["AGENT_STEP", "LLM_CALL", "TOOL_CALL", "RETRIEVAL"]
    for i in range(20):
        s = random.choice(sessions)
        event = make_event(s, random.choice(types))
        producer.produce(
            topic=TOPIC,
            key=s.encode(),  # key by session_id -> per-session ordering
            value=avro_ser(event, SerializationContext(TOPIC, MessageField.VALUE)),
            on_delivery=delivery,
        )
    producer.flush()
    print("done: 20 events produced")
