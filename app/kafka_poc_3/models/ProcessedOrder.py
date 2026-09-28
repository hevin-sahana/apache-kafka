from datetime import datetime

from sqlalchemy import Column, Integer, DateTime

from app.kafka_poc_3.db.base import Base


class ProcessedOrder(Base):
    __tablename__ = "processed_orders"

    id = Column(
        Integer,
        primary_key=True,
        index=True
    )

    order_id = Column(
        Integer,
        unique=True,
        nullable=False,
        index=True
    )

    processed_at = Column(
        DateTime,
        default=datetime.utcnow,
        nullable=False
    )