from contextlib import asynccontextmanager
import logging
import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.kafka_poc_5.db.base import Base
from app.kafka_poc_5.db.database import engine
from app.kafka_poc_1.router import router as kafka_poc_1
from app.kafka_poc_2.router import router as kafka_poc_2
from app.kafka_poc_3.router import router as kafka_poc_3
from app.kafka_poc_4.router import router as kafka_poc_4
from app.kafka_poc_5.router import router as kafka_poc_5
from app.kafka_poc_6.router import router as kafka_poc_6
from app.kafka_poc_8.router import router as kafka_poc_8
from app.clickstream_poc.router import router as clickstream_event
from app.clickstream_poc.consumer import main as start_consumer

from app.kafka_poc_5.product_model import Product

logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Start the Kafka clickstream consumer in a daemon background thread
    so it runs inside the same process as FastAPI. This is essential:
    metrics_store is in-memory, so the consumer and the router MUST share
    the same process — otherwise record_event() and get_dashboard_snapshot()
    operate on separate memory and the dashboard always shows zeros.
    """
    thread = threading.Thread(
        target=start_consumer,
        daemon=True,          # exits automatically when the FastAPI process stops
        name="clickstream-consumer",
    )
    thread.start()
    logger.info("Clickstream consumer background thread started.")
    yield
    # daemon=True means no explicit shutdown needed; thread exits with the process.
    logger.info("FastAPI shutting down — consumer thread will stop.")


app = FastAPI(
    title="Kafka POC",
    description="FastAPI and Apache Kafka notification POC",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    kafka_poc_1,
    prefix="/poc/01",
    tags=["kafka_poc_1"]
)

app.include_router(
    kafka_poc_2,
    prefix="/poc/02",
    tags=["kafka_poc_2"]
)

app.include_router(
    kafka_poc_3,
    prefix="/poc/03",
    tags=["kafka_poc_3"]
)

app.include_router(
    kafka_poc_4,
    prefix="/poc/04",
    tags=["kafka_poc_4"]
)

app.include_router(
    kafka_poc_5,
    prefix="/poc/05",
    tags=["kafka_poc_5"]
)

app.include_router(
    kafka_poc_6,
    prefix="/poc/06",
    tags=["kafka_poc_6"]
)

app.include_router(
    kafka_poc_8,
    prefix="/poc/08",
    tags=["kafka_poc_8"]
)

app.include_router(
    clickstream_event,
    prefix="/clickstream_event/09",
    tags=["clickstream_event"]
)


# PYTHONPATH=.. uvicorn app.main:app --reload in kafka_poc_1

@app.get("/health")
def health_check():
    return {
        "status": "UP"
    }
