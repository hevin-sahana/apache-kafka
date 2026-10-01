import json
import logging

from confluent_kafka import Consumer

from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import configure_logging

logger = logging.getLogger(__name__)

TOPIC_DLQ = "poc_4_orders_dlq"
GROUP_ID_DLQ = "poc_4_dlq_audit_group"


def consume_dlq_records():
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": GROUP_ID_DLQ,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })

    consumer.subscribe([TOPIC_DLQ])

    logger.info(
        f"🚨 DLQ Audit Consumer Started | "
        f"topic={TOPIC_DLQ} | "
        f"group={GROUP_ID_DLQ}"
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(f"DLQ Consumer Error | error={message.error()}")
                continue

            raw_bytes = message.value()
            try:
                dlq_record = json.loads(raw_bytes.decode("utf-8"))
                original_payload = dlq_record.get("original_payload")
                error_meta = dlq_record.get("error_metadata", {})

                logger.warning(
                    f"\n================ DLQ RECORD AUDITED ================\n"
                    f"Original Offset : {error_meta.get('original_offset')}\n"
                    f"Error Type      : {error_meta.get('error_type')}\n"
                    f"Error Detail    : {error_meta.get('error_message')}\n"
                    f"Original Payload: {original_payload}\n"
                    f"Timestamp       : {error_meta.get('timestamp')}\n"
                    f"===================================================="
                )
            except Exception as e:
                logger.error(f"Error parsing DLQ message | error={e}")

    except KeyboardInterrupt:
        logger.info("DLQ Consumer stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    configure_logging("poc-04_dlq_consumer")
    consume_dlq_records()
