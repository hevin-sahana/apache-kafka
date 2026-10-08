import logging
from typing import Annotated

from fastapi import APIRouter, HTTPException

from app.kafka_poc_6 import schema_registery_service
from app.kafka_poc_6.config import AVRO_SCHEMA_FILE, PRODUCT_SCHEMA_SUBJECT
from app.utility.logging_config import configure_logging

router = APIRouter(tags=["kafka_poc_6"])
configure_logging("poc-06_api")
logger = logging.getLogger(__name__)


@router.post("/schemas/product/register")
def register_product_schema():
    logger.info("register_product_schema")
    result = schema_registery_service.register_schema(
        subject=PRODUCT_SCHEMA_SUBJECT,
        schema_file=AVRO_SCHEMA_FILE
    )
    logger.info(f"register_product_schema: {result}")
    return result


@router.post("/schemas/{subject}/version")
def get_schema_versions(subject: str):
    logger.info("get_schema_versions")
    result = schema_registery_service.get_schema_versions(
        subject=subject
    )
    logger.info(f"get_schema_versions: {result}")
    return result


@router.post("/schemas/{subject}/version/latest")
def get_latest_version(subject: str):
    logger.info("get_latest_version")
    result = schema_registery_service.get_latest_schema(
        subject=subject
    )
    logger.info(f"get_latest_schema: {result}")
    return result


@router.post("/schemas/{subject}/version/{version_no}")
def get_specific_version(subject: str, version_no: int):
    logger.info("get_specific_version")
    result = schema_registery_service.get_specific_version(
        subject=subject,
        version=version_no
    )
    logger.info(f"get_specific_version: {result}")
    return result


@router.delete("/schemas/{subject}")
def delete_schema_api(
        subject: str):
    return schema_registery_service.delete_schema(
        subject=subject
    )


@router.put("/subjects/{subject}/compatibility")
def update_compatibility(
        subject: str,
        compatibility: str
):
    try:
        return schema_registery_service.set_compatibility(
            subject,
            compatibility
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
