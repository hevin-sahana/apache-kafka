import logging
from fastapi import APIRouter

from app.kafka_poc_4.order import OrderRequest
from app.kafka_poc_4.producer import send_order
from app.kafka_poc_4.produce_mixed_events import run_mixed_events_producer
from app.utility.logging_config import configure_logging

router = APIRouter(tags=["kafka_poc_4"])
configure_logging("poc-04_api")
logger = logging.getLogger(__name__)


@router.post("/orders/valid")
def create_valid_order(order: OrderRequest):
    order_data = order.model_dump()
    send_order(order_data)
    return {
        "message": "Valid order published successfully",
        "order": order_data
    }


@router.post("/orders/poison-pill/corrupted-json")
def trigger_corrupted_json_poison_pill(raw_payload: str = '{"order_id": 999, "amount": BAD_JSON'):
    send_order(raw_payload)
    return {
        "message": "Corrupted JSON poison pill published to Kafka",
        "raw_payload": raw_payload
    }


@router.post("/orders/poison-pill/invalid-schema")
def trigger_invalid_schema_poison_pill():
    invalid_data = {"order_id": 888, "amount": -50.0}
    send_order(invalid_data)
    return {
        "message": "Invalid schema poison pill published to Kafka",
        "invalid_data": invalid_data
    }


@router.post("/produce-mixed-stream")
def trigger_mixed_stream():
    run_mixed_events_producer()
    return {
        "message": "Mixed event stream published successfully"
    }
