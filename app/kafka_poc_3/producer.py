import json
import time

from confluent_kafka import Producer

from app.kafka_poc_3.order import OrderRequest
from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import logger

TOPIC = "poc_3_orders"

producer = Producer({
    "bootstrap.servers": bootstrap_servers,
    "enable.idempotence": True,
    "acks": "all",
})


def delivery_report(error, message):
    if error:
        print(f"Delivery failed: {error}")
        return

    print(
        f"Delivered | "
        f"topic={message.topic()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()}"
    )


def produce_orders():
    for i in range(1, 1001):
        order = OrderRequest(
            order_id=i,
            user_id=100 + i,
            event_type="ORDER_PLACED",
            sequence=1,
            amount=10000
        )

        logger.info(
            f"Publishing order={order}"
        )

        producer.produce(
            topic=TOPIC,
            key=str(i),
            value=json.dumps(order.model_dump()),
            callback=delivery_report
        )

        producer.poll(0)
        time.sleep(0.2)

    # producer.flush()
