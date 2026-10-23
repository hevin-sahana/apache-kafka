from app.clickstream_poc.config import SCHEMA_REGISTRY_URL, AVRO_SCHEMA_FILE
import json
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from app.utility.kafka import bootstrap_servers

with open(AVRO_SCHEMA_FILE, "r", encoding="utf-8") as file:
    EVENT_SCHEMA = json.dumps(json.load(file))

schema_registry = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})

TOPIC_NAME = "clickstream_event_topic"


def event_to_dict(event, ctx):
    return event


avro_serializer = AvroSerializer(
    schema_registry,
    EVENT_SCHEMA,
    event_to_dict,
    conf={
        "auto.register.schemas": False,
        "subject.name.strategy": (
            lambda ctx, record_name:
            "clickstream-events-value"
        )
    }
)

producer = SerializingProducer({
    "bootstrap.servers": bootstrap_servers,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
    "enable.idempotence": True,
    "acks": "all"
})


def publish_event(event: dict):
    delivered = []

    def delivery_callback(err, msg):
        if err:
            delivered.append({"error": str(err)})
        else:
            delivered.append({
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            })

    producer.produce(
        topic=TOPIC_NAME,
        key=event["session_id"],
        value=event,
        on_delivery=delivery_callback
    )

    remaining = producer.flush(timeout=10)

    if remaining > 0:
        raise TimeoutError(
            f"{remaining} message(s) still pending delivery"
        )

    if not delivered or "error" in delivered[0]:
        raise RuntimeError(
            delivered[0].get("error", "Message delivery failed")
            if delivered else "No delivery confirmation received"
        )

    return delivered[0]
