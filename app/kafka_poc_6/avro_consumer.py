from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from app.kafka_poc_6.config import SCHEMA_REGISTRY_URL, AVRO_SCHEMA_FILE
from app.utility.kafka import bootstrap_servers

TOPIC = "products"
GROUP_ID = "product-avro-consumer-group-full_1"

# Schema Registry client
schema_registry_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})

with open(AVRO_SCHEMA_FILE, "r") as file:
    reader_schema = file.read()

# Avro deserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client,
    reader_schema
)

# Kafka consumer
consumer = DeserializingConsumer({
    "bootstrap.servers": bootstrap_servers,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "value.deserializer": avro_deserializer
})

consumer.subscribe([TOPIC])

print(f"Listening to topic: {TOPIC}")

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        product = msg.value()

        print(
            f"Received product: "
            f"{product}"
        )

        print(
            f"id={product['id']}, "
            f"name={product['name']}, "
            f"price={product['price']}, "
            f"quantity={product['quantity']}",
            f"category={product['category']}",
            f"discount_price={product['discount_price']}",
            f"description={product['description']}"
        )

        # Commit after successful processing
        consumer.commit(asynchronous=False)

except KeyboardInterrupt:
    print("Consumer stopped")

finally:
    consumer.close()
