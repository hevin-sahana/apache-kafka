import json
import logging

import requests

from app.kafka_poc_6.config import SCHEMA_REGISTRY_URL

logger = logging.getLogger(__name__)


def register_schema(subject: str, schema_file: str):
    with open(schema_file, "r") as file:
        schema = json.load(file)

    payload = {
        "schema": json.dumps(schema)
    }

    logger.info(payload)

    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    }

    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        headers=headers,
        json=payload,
        timeout=10
    )

    response.raise_for_status()

    return response.json()


def get_schema_versions(subject: str):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
        timeout=10
    )

    response.raise_for_status()

    return response.json()


def get_latest_schema(subject: str):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest",
        timeout=10
    )

    response.raise_for_status()

    return response.json()


def get_specific_version(subject: str, version: int):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/{version}",
        timeout=10
    )

    response.raise_for_status()

    return response.json()


def delete_schema(subject: str):
    url = (
        f"{SCHEMA_REGISTRY_URL}/subjects/"
        f"{subject}"
    )

    response = requests.delete(
        url,
        timeout=10
    )

    print("URL:", url)
    print("STATUS:", response.status_code)
    print("RESPONSE:", response.text)

    response.raise_for_status()

    return response.json()

def set_compatibility(subject: str, compatibility: str):
    url = f"{SCHEMA_REGISTRY_URL}/config/{subject}"

    payload = {
        "compatibility": compatibility
    }

    headers = {
        "Content-Type": "application/vnd.schemaregistry.v1+json"
    }

    response = requests.put(
        url,
        headers=headers,
        json=payload,
        timeout=10
    )

    response.raise_for_status()

    return response.json()

