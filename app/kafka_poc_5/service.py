# app/kafka_poc_5/service.py

from fastapi import HTTPException
from sqlalchemy.orm import Session

from . import product_repository
from app.kafka_poc_5.schemas import ProductCreate


def create_product(
    db: Session,
    request: ProductCreate,
):
    return product_repository.create_product(
        db,
        request,
    )


def search_products_db(
    db: Session,
    query: str
):
    return product_repository.search_products_db(
        db,
        query
    )


def update_product(
    db: Session,
    product_id: int,
    request: ProductCreate
):
    product = product_repository.update_product(
        db=db,
        product_id=product_id,
        name=request.name,
        price=request.price,
        quantity=request.quantity,
        category=request.category,
    )

    if not product:
        raise HTTPException(
            status_code=404,
            detail="Product not found"
        )

    return product


def delete_product(
    db: Session,
    product_id: int
):
    product = product_repository.delete_product(
        db=db,
        product_id=product_id
    )

    if not product:
        raise HTTPException(
            status_code=404,
            detail="Product not found"
        )

    return {
        "message": "Product deleted successfully",
        "product_id": product_id
    }