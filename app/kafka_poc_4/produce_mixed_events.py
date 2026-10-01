import time
import logging

from app.kafka_poc_4.producer import send_order
from app.utility.logging_config import configure_logging

configure_logging("poc-04_producer")
logger = logging.getLogger(__name__)


def run_mixed_events_producer():
    logger.info("=== Starting Mixed Stream Producer (POC-04) ===")

    # A realistic list of incoming event payloads sent to Kafka.
    # The producer sends these payloads sequentially using the same generic send_order() method.
    events = [
        # Event 1: Valid Order
        {
            "order_id": 101,
            "user_id": 501,
            "event_type": "ORDER_PLACED",
            "amount": 1500.00
        },
        # Event 2: Corrupted JSON String (Legacy system / syntax bug)
        '{"order_id": 102, "user_id": 502, "event_type": "ORDER_PLACED", "amount": BAD_JSON_SYNTAX',

        # Event 3: Valid Order
        {
            "order_id": 103,
            "user_id": 503,
            "event_type": "ORDER_PLACED",
            "amount": 2500.50
        },
        # Event 4: Schema Violation (Upstream bug generating negative amount)
        {
            "order_id": 104,
            "user_id": 504,
            "event_type": "ORDER_PLACED",
            "amount": -999.00  # Fails gt=0 validation on consumer
        },
        # Event 5: Valid Order
        {
            "order_id": 105,
            "user_id": 505,
            "event_type": "ORDER_PLACED",
            "amount": 3200.00
        },
        # Event 6: Schema Violation (Missing required user_id field)
        {
            "order_id": 106,
            "event_type": "ORDER_PLACED",
            "amount": 400.00
        },
        # Event 7: Valid Order
        {
            "order_id": 107,
            "user_id": 507,
            "event_type": "ORDER_PLACED",
            "amount": 899.99
        }
    ]

    for idx, payload in enumerate(events, start=1):
        logger.info(f"\n--- Publishing Event #{idx} ---")
        send_order(payload)
        time.sleep(1.0)

    logger.info("=== Finished Publishing Mixed Stream ===")


if __name__ == "__main__":
    run_mixed_events_producer()
