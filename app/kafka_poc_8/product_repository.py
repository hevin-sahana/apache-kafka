from sqlalchemy.orm import Session

from .product_model import Product
from .schemas import ProductCreate


def create_product(db: Session, request: ProductCreate) -> Product:
    product = Product(
        name=request.name,
        description=request.description,
        price=request.price,
        quantity=request.quantity,
        category=request.category,
    )

    db.add(product)
    db.commit()
    db.refresh(product)

    return product


from sqlalchemy import or_, func


def search_products_db(db: Session, query: str):
    q = (query or "").strip()
    if not q or q == "*":
        return db.query(Product).all()

    search_term = f"%{q}%"
    return db.query(Product).filter(
        or_(
            Product.name.ilike(search_term),
            func.coalesce(Product.category, "").ilike(search_term)
        )
    ).all()


def get_product_by_id(
        db: Session,
        product_id: int
):
    return (
        db.query(Product)
        .filter(Product.id == product_id)
        .first()
    )


def update_product(
        db: Session,
        product_id: int,
        name: str,
        price: float,
        quantity: int,
        category: str
):
    product = get_product_by_id(db, product_id)

    if not product:
        return None

    product.name = name
    product.price = price
    product.quantity = quantity
    product.category = category

    db.commit()
    db.refresh(product)

    return product


def delete_product(
        db: Session,
        product_id: int
):
    product = get_product_by_id(db, product_id)

    if not product:
        return None

    db.delete(product)
    db.commit()

    return product
