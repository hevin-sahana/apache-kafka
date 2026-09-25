import json
import logging
import sys

from confluent_kafka import Consumer

from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import configure_logging

logger = logging.getLogger(__name__)

TOPIC = "poc_2_orders_new"


def consume_orders(consumer_name, group_id, simulate_failure=False):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC])

    logger.info(
        f"Kafka consumer started | "
        f"consumer={consumer_name} | "
        f"group={group_id}"
    )

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(
                    f"Kafka consumer error | "
                    f"consumer={consumer_name} | "
                    f"error={message.error()}"
                )
                continue

            order = json.loads(
                message.value().decode("utf-8")
            )

            logger.info(
                f"Order received | "
                f"consumer={consumer_name} | "
                f"topic={message.topic()} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()} | "
                f"group={group_id}"
            )

            logger.info(
                f"Order processed | "
                f"consumer={consumer_name} | "
                f"order_id={order['order_id']}"
            )

            # Simulate failure BEFORE committing
            if simulate_failure:
                raise Exception("Simulated consumer failure")

            consumer.commit(message=message)

            logger.info(
                f"Offset committed | "
                f"consumer={consumer_name} | "
                f"topic={message.topic()} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

    except KeyboardInterrupt:
        logger.info(
            f"Kafka consumer stopped | "
            f"consumer={consumer_name}"
        )

    finally:
        consumer.close()

        logger.info(
            f"Kafka consumer closed | "
            f"consumer={consumer_name}"
        )


if __name__ == "__main__":
    configure_logging("poc-02_new")

    consumer_name = sys.argv[1]
    consumer_group_id = sys.argv[2]

    consume_orders(
        consumer_name=consumer_name, group_id=consumer_group_id
    )
