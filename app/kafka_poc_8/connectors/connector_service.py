import os

import requests
from fastapi import HTTPException

KAFKA_CONNECT_URL = os.getenv(
    "KAFKA_CONNECT_URL",
    "http://localhost:8083"
)


def register_connector(config: dict):
    try:
        response = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json=config,
            timeout=10,
        )

        if response.status_code not in (200, 201):
            raise HTTPException(
                status_code=response.status_code,
                detail=response.text,
            )

        return response.json()

    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


def get_connectors():
    response = requests.get(
        f"{KAFKA_CONNECT_URL}/connectors",
        timeout=10,
    )

    response.raise_for_status()

    return response.json()


def get_connector_status(name: str):
    response = requests.get(
        f"{KAFKA_CONNECT_URL}/connectors/{name}/status",
        timeout=10,
    )

    response.raise_for_status()

    return response.json()


def update_connector(connector_name: str, config: dict):
    try:
        response = requests.put(
            f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config",
            json=config,
            timeout=10,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=response.text,
            )

        return response.json()

    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


def restart_connector(connector_name: str):
    response = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/restart",
        timeout=10
    )

    if response.status_code not in (200, 202, 204):
        raise HTTPException(
            status_code=response.status_code,
            detail=response.text
        )

    return {
        "message": "Connector restarted",
        "connector": connector_name
    }