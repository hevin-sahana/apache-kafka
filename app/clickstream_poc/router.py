import asyncio
import json
import logging

from fastapi import FastAPI, HTTPException, APIRouter
from confluent_kafka.admin import AdminClient, NewTopic
from starlette.responses import StreamingResponse

from app.clickstream_poc.config import AVRO_SCHEMA_FILE, \
    CLICKSTREAM_SCHEMA_SUBJECT
from app.clickstream_poc.dashboard.metrics_store import get_dashboard_snapshot
from app.clickstream_poc.producer import publish_event
from app.clickstream_poc.schema.clickstream_schema import ClickstreamEvent
from app.kafka_poc_6 import schema_registery_service
from app.utility.kafka import bootstrap_servers
from app.utility.logging_config import configure_logging

admin_client = AdminClient({
    "bootstrap.servers": bootstrap_servers
})

router = APIRouter(tags=["clickstream_event"])
configure_logging("clickstream_event")
logger = logging.getLogger(__name__)


@router.post("/topics/clickstream-events")
def create_clickstream_topic(name: str):
    topic = NewTopic(
        name,
        num_partitions=3,
        replication_factor=1
    )

    futures = admin_client.create_topics([topic])

    try:
        futures[name].result(timeout=10)

        return {
            "message": "Kafka topic created successfully",
            "topic": name,
            "partitions": 3,
            "replication_factor": 1
        }

    except Exception as exc:
        error_message = str(exc)

        if "TOPIC_ALREADY_EXISTS" in error_message:
            return {
                "message": "Kafka topic already exists",
                "topic": name
            }

        raise HTTPException(
            status_code=500,
            detail=f"Failed to create Kafka topic: {error_message}"
        )


@router.post("/schemas/register")
def register_product_schema():
    logger.info("register_product_schema")
    result = schema_registery_service.register_schema(
        subject=CLICKSTREAM_SCHEMA_SUBJECT,
        schema_file=AVRO_SCHEMA_FILE
    )
    logger.info(f"register_product_schema: {result}")
    return result


@router.post("/events")
def create_event(event: ClickstreamEvent):
    try:
        metadata = publish_event(event.model_dump())

        return {
            "message": "Clickstream event published",
            "event_id": event.event_id,
            "kafka": metadata
        }

    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to publish event: {exc}"
        )


@router.get("/dashboard/summary")
def dashboard_summary():
    return get_dashboard_snapshot()


@router.get("/dashboard/stream")
async def dashboard_stream():
    async def generate():
        while True:
            snapshot = get_dashboard_snapshot()

            yield f"data: {json.dumps(snapshot, default=str)}\n\n"

            await asyncio.sleep(1)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
