from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from app.kafka_poc_6.config import AVRO_SCHEMA_FILE, SCHEMA_REGISTRY_URL
from app.utility.kafka import bootstrap_servers

TOPIC = "products"

with open(AVRO_SCHEMA_FILE, "r") as file:
    schema_str = file.read()

# Connect to Schema Registry
schema_registry_client = SchemaRegistryClient({
    "url": SCHEMA_REGISTRY_URL
})

# Create Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# Create Kafka producer
producer = SerializingProducer({
    "bootstrap.servers": bootstrap_servers,
    "value.serializer": avro_serializer
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Message delivered to "
            f"{msg.topic()} [{msg.partition()}] "
            f"offset={msg.offset()}"
        )


product = {
  "id": 110,
  "name": "Mouse",
  "price": 1200.0,
  "quantity": 5,
  "category": "Electronics",
  "discount_price": 999.99,
  "description": "Wireless keyboard"
}

try:
    producer.produce(
        topic=TOPIC,
        value=product,
        on_delivery=delivery_report
    )

    producer.flush()

except Exception as e:
    print(f"Producer error: {e}")
