from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from app.kafka_poc_7.config import (
    BOOTSTRAP_SERVERS,
    TOPIC_NAME,
    PARTITIONS,
    REPLICATION_FACTOR,
    MIN_IN_SYNC_REPLICAS,
)


def create_topic():
    admin = AdminClient({
        "bootstrap.servers": BOOTSTRAP_SERVERS
    })

    topic = NewTopic(
        topic=TOPIC_NAME,
        num_partitions=PARTITIONS,
        replication_factor=REPLICATION_FACTOR,
        config={
            "min.insync.replicas": str(MIN_IN_SYNC_REPLICAS)
        },
    )

    futures = admin.create_topics([topic])

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Topic created successfully: {topic_name}")

        except KafkaException as exc:
            error = exc.args[0]

            if error.code() == -192:  # TOPIC_ALREADY_EXISTS
                print(f"Topic already exists: {topic_name}")
            else:
                print(f"Failed to create topic: {error}")

        except Exception as exc:
            print(f"Unexpected error: {exc}")


if __name__ == "__main__":
    create_topic()
