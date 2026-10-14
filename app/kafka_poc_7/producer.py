import json
from confluent_kafka import Producer

from app.kafka_poc_7.config import (
    BOOTSTRAP_SERVERS,
    TOPIC_NAME,
)


def delivery_report(err, message):
    if err:
        print(f"DELIVERY FAILED: {err}")
        return

    print(
        f"DELIVERED | key={message.key().decode()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
    })

    try:
        for order_id in range(1, 11):
            order = {
                "order_id": order_id,
                "product": "Keyboard",
                "quantity": 1,
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=str(order_id),
                value=json.dumps(order),
                callback=delivery_report,
            )

            producer.poll(0)

    except BufferError as exc:
        print(f"Producer queue is full: {exc}")

    finally:
        remaining = producer.flush(30)
        print(f"Messages remaining in queue: {remaining}")


if __name__ == "__main__":
    main()
