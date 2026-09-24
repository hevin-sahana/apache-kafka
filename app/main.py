from fastapi import FastAPI
from kafka_poc_1.router import router as kafka_poc_1

app = FastAPI(
    title="Kafka POC",
    description="FastAPI and Apache Kafka notification POC",
    version="1.0.0",
)

app.include_router(
    kafka_poc_1,
    prefix="/poc/01",
    tags=["kafka_poc_1"]
)


# PYTHONPATH=.. uvicorn app.main:app --reload in kafka_poc_1

@app.get("/health")
def health_check():
    return {
        "status": "UP"
    }
