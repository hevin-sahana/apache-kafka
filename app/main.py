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


from app.kafka_poc_5.product_model import Product

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Kafka POC",
    description="FastAPI and Apache Kafka notification POC",
    version="1.0.0",
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



# PYTHONPATH=.. uvicorn app.main:app --reload in kafka_poc_1

@app.get("/health")
def health_check():
    return {
        "status": "UP"
    }
