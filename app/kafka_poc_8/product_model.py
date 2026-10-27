# app/kafka_poc_5/product_model.py
from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, Numeric, Column, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from app.kafka_poc_8.db.base import Base


class Product(Base):
    __tablename__ = "products_poc_8"

    id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )

    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )
