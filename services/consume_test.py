"""Hour-one smoke test: consume and decode TraceEvents."""
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

TOPIC = "traces.events.v1"

sr = SchemaRegistryClient({"url": "http://localhost:8081"})
deser = AvroDeserializer(sr)

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "hour-one-check",
    "auto.offset.reset": "earliest",
})
c.subscribe([TOPIC])

count = 0
try:
    while count < 20:
        msg = c.poll(2.0)
        if msg is None or msg.error():
            continue
        event = deser(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
        count += 1
        print(f"{count:2d}. {event['session_id']:<10} {event['event_type']:<10} "
              f"{event['latency_ms']:>7.2f}ms  status={event['status']}")
finally:
    c.close()
print("done: 20 events consumed and decoded")
