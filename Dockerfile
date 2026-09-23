FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kafka-poc-1/app ./app

# Copy shared utility package
COPY utility ./utility