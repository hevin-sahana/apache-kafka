import os

bootstrap_servers = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)