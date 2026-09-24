import logging

from fastapi import FastAPI

from app.models import OrderRequest
from app.producer import send_order
from app.utility.logging_config import configure_logging

configure_logging("poc-01")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Kafka POC-1 Order Processing",
    description="FastAPI and Apache Kafka notification POC",
    version="1.0.0",
)

# PYTHONPATH=.. uvicorn app.main:app --reload in kafka-poc-1

@app.get("/health")
def health_check():
    return {
        "status": "UP"
    }


@app.post("/orders")
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
