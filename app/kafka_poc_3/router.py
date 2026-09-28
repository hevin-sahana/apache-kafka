import logging

from fastapi import APIRouter

from app.kafka_poc_3.order import OrderRequest
from app.kafka_poc_3.producer import produce_orders
from app.utility.logging_config import configure_logging

router = APIRouter(tags=["kafka_poc_3"])
configure_logging("kafka_poc_3")
logger = logging.getLogger(__name__)


# @router.post("/orders")
# def create_order(order: OrderRequest):
#     logger.info(
#         f"Order API request received | order_id={order.order_id}")
#
#     order_data = order.model_dump()
#     produce_order(order_data)
#     logger.info(
#         f"Order published | order_id={order.order_id}")
#     return {
#         "message": "Order published successfully",
#         "order": order_data
#     }



@router.post("/orders")
def create_orders():

    logger.info("Starting producer idempotency test")

    produce_orders()

    logger.info("200 orders published")

    return {
        "message": "200 orders published successfully"
    }