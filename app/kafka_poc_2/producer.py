import json

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

TOPIC = "poc_2_orders"


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered | "
        f"topic={message.topic()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()} | "
        f"key={message.key()}"
    )


def produce_order(order, key=None, partition=None):

    kwargs = {
        "topic": TOPIC,
        "value": json.dumps(order.model_dump()),
        "callback": delivery_report,
    }

    if key is not None:
        kwargs["key"] = key

    if partition is not None:
        kwargs["partition"] = partition

    producer.produce(**kwargs)
    producer.poll(0)