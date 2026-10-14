
import json
from confluent_kafka import Consumer, KafkaError

from app.kafka_poc_7.config import (
    BOOTSTRAP_SERVERS,
    TOPIC_NAME,
)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "poc-7-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([TOPIC_NAME])

    print(f"Listening to topic: {TOPIC_NAME}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"Consumer error: {message.error()}")
                continue

            try:
                order = json.loads(message.value().decode("utf-8"))

                print(
                    f"CONSUMED | order_id={order['order_id']} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}"
                )

                # Commit only after successful processing.
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                print(f"Message processing failed: {exc}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
