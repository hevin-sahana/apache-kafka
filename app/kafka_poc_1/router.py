import logging

from fastapi import APIRouter

from app.utility.logging_config import configure_logging
from app.kafka_poc_1.models import OrderRequest
from app.kafka_poc_1.producer import send_order

router = APIRouter(tags=["kafka_poc_1"])
configure_logging("poc-01")
logger = logging.getLogger(__name__)


@router.post("/orders")
def create_order(order: OrderRequest):
    logger.info(
        f"Order API request received | order_id={order.order_id}")

    order_data = order.model_dump()
    send_order(order_data)
    logger.info(
        f"Order published | order_id={order.order_id}")
    return {
        "message": "Order published successfully",
        "order": order_data
    }
