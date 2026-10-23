from confluent_kafka import Consumer
from confluent_kafka.serialization import (
    StringDeserializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# Use the same module reference as router.py so record_event() and
# get_dashboard_snapshot() share the same in-memory counters.
from app.clickstream_poc.dashboard.metrics_store import record_event, get_dashboard_snapshot

# Use shared config so Docker / non-localhost setups work correctly.
from app.utility.kafka import bootstrap_servers
from app.clickstream_poc.config import SCHEMA_REGISTRY_URL

TOPIC_NAME = "clickstream_event_topic"
GROUP_ID = "clickstream-consumer-group"

# Schema Registry client
schema_registry = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})

# Deserialize Avro messages
avro_deserializer = AvroDeserializer(schema_registry)
key_deserializer = StringDeserializer("utf_8")

# Kafka consumer configuration
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})


def main():
    # Broad outer handler: if anything fails here (Kafka unreachable,
    # Schema Registry down, topic missing) the error is logged clearly
    # instead of the thread dying silently with no dashboard indication.
    try:
        consumer.subscribe([TOPIC_NAME])

        print(f"[Consumer] Subscribed to topic: {TOPIC_NAME}")
        print(f"[Consumer] Group: {GROUP_ID}")
        print("[Consumer] Waiting for clickstream events...")

        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"[Consumer] Kafka error: {msg.error()}")
                    continue

                try:
                    event = avro_deserializer(
                        msg.value(),
                        SerializationContext(
                            msg.topic(),
                            MessageField.VALUE
                        )
                    )

                    event_key = (
                        key_deserializer(msg.key())
                        if msg.key() is not None
                        else None
                    )

                    if event is not None:
                        print(
                            f"[Consumer] Event received — "
                            f"topic={msg.topic()} partition={msg.partition()} "
                            f"offset={msg.offset()} key={event_key} "
                            f"event_type={event.get('event_type')}"
                        )
                        # Update shared in-memory dashboard metrics
                        record_event(event)
                        print("[Consumer] record_event() called — dashboard updated")

                    # Commit only after successful processing.
                    consumer.commit(message=msg, asynchronous=False)

                except Exception as exc:
                    # Log clearly — message NOT committed so it can be retried.
                    print(
                        f"[Consumer] ERROR processing message at "
                        f"partition={msg.partition()} offset={msg.offset()}: {exc}"
                    )

        except KeyboardInterrupt:
            print("[Consumer] Stopped by KeyboardInterrupt.")

        finally:
            consumer.close()
            print("[Consumer] Kafka consumer closed.")

    except Exception as fatal:
        # Catches startup failures: Kafka unreachable, Schema Registry down,
        # topic not found, etc. Without this the thread dies with no log output.
        print(f"[Consumer] FATAL — consumer thread crashed and will NOT recover: {fatal}")
        raise


if __name__ == "__main__":
    main()
