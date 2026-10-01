import json
import logging
from datetime import datetime, UTC

from confluent_kafka import Consumer
from pydantic import ValidationError

from app.kafka_poc_4.order import OrderRequest
from app.kafka_poc_4.producer import send_to_dlq
from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import configure_logging

logger = logging.getLogger(__name__)

TOPIC = "poc_4_orders"
GROUP_ID = "poc_4_order_group"


def consume_orders():
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([TOPIC])

    logger.info(
        f"Kafka Main Consumer started | "
        f"topic={TOPIC} | "
        f"group={GROUP_ID}"
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(f"Consumer error | error={message.error()}")
                continue

            raw_bytes = message.value()
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()

            logger.info(
                f"Message received | "
                f"topic={topic} | "
                f"partition={partition} | "
                f"offset={offset}"
            )

            # -------------------------------------------------------------
            # STEP 1: Attempt JSON Deserialization (Catch Corrupted JSON)
            # -------------------------------------------------------------
            try:
                decoded_str = raw_bytes.decode("utf-8")
                payload = json.loads(decoded_str)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                error_msg = f"Corrupted JSON payload: {str(e)}"
                logger.error(
                    f"⚠️ POISON PILL DETECTED (JSON Error) | "
                    f"offset={offset} | "
                    f"error={error_msg}"
                )

                error_meta = {
                    "error_type": "JSONDecodeError",
                    "error_message": error_msg,
                    "original_topic": topic,
                    "original_partition": partition,
                    "original_offset": offset,
                    "timestamp": datetime.utcnow().isoformat()
                }

                # 1. Route Poison Pill to DLQ
                send_to_dlq(raw_bytes, error_meta)

                # 2. Commit main offset so processing is NOT blocked
                consumer.commit(message=message)
                logger.info(f"Offset committed after DLQ routing | offset={offset}")
                continue

            # -------------------------------------------------------------
            # STEP 2: Schema & Business Validation (Catch Invalid Data)
            # -------------------------------------------------------------
            try:
                validated_order = OrderRequest(**payload)
            except ValidationError as ve:
                error_msg = f"Schema validation failed: {ve.errors()}"
                logger.error(
                    f"⚠️ POISON PILL DETECTED (Schema Error) | "
                    f"offset={offset} | "
                    f"order_id={payload.get('order_id')} | "
                    f"error={error_msg}"
                )

                error_meta = {
                    "error_type": "ValidationError",
                    "error_message": error_msg,
                    "original_topic": topic,
                    "original_partition": partition,
                    "original_offset": offset,
                    "timestamp": datetime.now(UTC).isoformat()
                }

                # 1. Route Poison Pill to DLQ
                send_to_dlq(raw_bytes, error_meta)

                # 2. Commit main offset so processing is NOT blocked
                consumer.commit(message=message)
                logger.info(f"Offset committed after DLQ routing | offset={offset}")
                continue

            # -------------------------------------------------------------
            # STEP 3: Business Processing for Valid Messages
            # -------------------------------------------------------------
            logger.info(
                f"Order processed successfully | "
                f"order_id={validated_order.order_id} | "
                f"amount=${validated_order.amount}"
            )

            # -------------------------------------------------------------
            # STEP 4: Commit Offset for Valid Message
            # -------------------------------------------------------------
            consumer.commit(message=message)
            logger.info(f"Offset committed | offset={offset}")

    except KeyboardInterrupt:
        logger.info("Kafka consumer stopped by user")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


if __name__ == "__main__":
    configure_logging("poc-04_consumer")
    consume_orders()
