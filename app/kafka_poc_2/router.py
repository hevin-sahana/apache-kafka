import logging

from fastapi import APIRouter, Query

from app.kafka_poc_2.order import OrderRequest
from app.kafka_poc_2.partition_mode import PartitionMode
from app.kafka_poc_2.producer import produce_order
from app.utility.logging_config import configure_logging

router = APIRouter(tags=["kafka_poc_2"])
configure_logging("kafka_poc_2")
logger = logging.getLogger(__name__)


@router.post("/orders")
def create_order(
        order: OrderRequest,
        mode: PartitionMode = Query(PartitionMode.KEY),
        partition: int | None = Query(None),
):
    key = None
    selected_partition = None

    if mode == PartitionMode.NO_KEY:

        # Let Kafka's producer partitioner choose
        key = None

    elif mode == PartitionMode.KEY:

        # Use user_id as Kafka key
        key = str(order.user_id)

    elif mode == PartitionMode.EXPLICIT_PARTITION:

        if partition is None:
            return {
                "error": "partition is required when mode=partition"
            }

        selected_partition = partition

    produce_order(
        order=order,
        key=key,
        partition=selected_partition,
    )

    return {
        "message": "Order sent",
        "mode": mode,
        "key": key,
        "partition": selected_partition,
        "order_id": order.order_id,
        "user_id": order.user_id,
        "sequence": order.sequence,
    }
