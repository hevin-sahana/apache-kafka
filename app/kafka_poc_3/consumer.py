import json
import logging

from confluent_kafka import Consumer

from app.kafka_poc_3.db.database import SessionLocal
from app.kafka_poc_3.repository.processed_order_repository import (
    ProcessedOrderRepository,
)
from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import configure_logging

logger = logging.getLogger(__name__)

TOPIC = "poc_3_orders"
GROUP_ID = "poc_3_order_group"


def consume_orders(consumer_name):
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC])

    logger.info(
        f"Kafka consumer started | "
        f"consumer={consumer_name} | "
        f"group={GROUP_ID}"
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

            order_id = order["order_id"]

            db = SessionLocal()

            try:

                # 1. Check whether order was already processed
                already_processed = (
                    ProcessedOrderRepository.exists(
                        db=db,
                        order_id=order_id
                    )
                )

                if already_processed:
                    logger.info(
                        f"Duplicate order skipped | "
                        f"consumer={consumer_name} | "
                        f"order_id={order_id}"
                    )

                    # Commit Kafka offset because we intentionally
                    # skipped this duplicate.
                    consumer.commit(message=message)

                    continue

                # 2. Business processing
                logger.info(
                    f"Processing order | "
                    f"consumer={consumer_name} | "
                    f"order_id={order_id}"
                )

                # Simulate business operation
                logger.info(
                    f"Order processed | "
                    f"consumer={consumer_name} | "
                    f"order_id={order_id}"
                )

                # 3. Save order as processed
                ProcessedOrderRepository.save(
                    db=db,
                    order_id=order_id
                )

                logger.info(
                    f"Order marked as processed | "
                    f"consumer={consumer_name} | "
                    f"order_id={order_id}"
                )

                # 4. Commit Kafka offset
                consumer.commit(message=message)

                logger.info(
                    f"Offset committed | "
                    f"consumer={consumer_name} | "
                    f"partition={message.partition()} | "
                    f"offset={message.offset()}"
                )

            finally:
                db.close()

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
    configure_logging("poc-03")
    consume_orders("consumer-1")
