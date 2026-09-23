import json
import logging

from confluent_kafka import Consumer
from utility.logging_config import configure_logging


logger = logging.getLogger(__name__)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

consumer.subscribe(["orders"])


def consume_orders(simulate_failure=False):

    logger.info(
        "Kafka consumer started | group=order-consumer-group")

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():

                logger.error(
                    f"Kafka consumer error | "
                    f"error={message.error()}"
                )

                continue

            order = json.loads(
                message.value().decode("utf-8")
            )

            logger.info(
                f"Order received | "
                f"topic={message.topic()} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"group=order-consumer-group"
            )

            logger.info(
                f"Order processed | "
                f"order_id={order['order_id']}"
            )

            # Simulate failure BEFORE committing
            if simulate_failure:
                raise Exception("Simulated consumer failure")

            consumer.commit(message=message)

            logger.info(
                f"Offset committed | "
                f"topic={message.topic()} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

    except KeyboardInterrupt:

        logger.info("Kafka consumer stopped")

    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


if __name__ == "__main__":

    configure_logging("poc-01")
    consume_orders()