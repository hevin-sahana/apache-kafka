import json
import logging

from confluent_kafka import Producer

from app.utility.kafka import bootstrap_servers

logger = logging.getLogger(__name__)

TOPIC_MAIN = "poc_4_orders"
TOPIC_DLQ = "poc_4_orders_dlq"

producer = Producer({
    "bootstrap.servers": bootstrap_servers,
})


def delivery_report(err, message):
    if err is not None:
        logger.error(f"Message delivery failed | error={err}")
        return

    logger.info(
        f"Message delivered | "
        f"topic={message.topic()} | "
        f"partition={message.partition()} | "
        f"offset={message.offset()}"
    )


def send_order(payload, key=None):
    """
    Generic Producer function.
    In real applications, the producer simply accepts payload data 
    (dict or raw string) and sends it to Kafka without knowing if it is valid or faulty.
    """
    if isinstance(payload, dict):
        value_bytes = json.dumps(payload).encode("utf-8")
        if key is None:
            key = str(payload.get("order_id", "key"))
    elif isinstance(payload, str):
        value_bytes = payload.encode("utf-8")
        if key is None:
            key = "raw-key"
    else:
        value_bytes = str(payload).encode("utf-8")

    logger.info(f"Publishing event to Kafka | key={key}")

    producer.produce(
        topic=TOPIC_MAIN,
        key=str(key) if key else None,
        value=value_bytes,
        callback=delivery_report
    )
    producer.flush()


def send_to_dlq(failed_payload: bytes, error_metadata: dict):
    """Forward a failed payload with error metadata to DLQ topic."""
    logger.info(
        f"Routing failed record to DLQ | "
        f"dlq_topic={TOPIC_DLQ} | "
        f"error={error_metadata.get('error_type')}"
    )

    dlq_record = {
        "original_payload": failed_payload.decode("utf-8", errors="replace"),
        "error_metadata": error_metadata
    }

    producer.produce(
        topic=TOPIC_DLQ,
        key=str(error_metadata.get("original_offset", "dlq")),
        value=json.dumps(dlq_record).encode("utf-8"),
        callback=delivery_report
    )
    producer.flush()
