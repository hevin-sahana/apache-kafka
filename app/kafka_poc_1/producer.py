import json
import logging
import os
from confluent_kafka import Producer

from app.utility.kafka import bootstrap_servers

logger = logging.getLogger(__name__)

producer = Producer({
    "bootstrap.servers": bootstrap_servers,
})


def delivery_report(err, message):
    if err is not None:
        logger.error(
            f"Message delivery failed | error={err}"
        )
        return

    logger.info(
        f"Message delivered | "
        f"topic={message.topic()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()}"
    )


def send_order(order: dict):
    logger.info(
        f"Publishing order | order_id={order["order_id"]}")
    producer.produce(
        topic="orders",
        value=json.dumps(order),
        callback=delivery_report
    )

    producer.flush()
