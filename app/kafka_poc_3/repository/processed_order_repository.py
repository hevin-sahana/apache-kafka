from datetime import datetime

from sqlalchemy.orm import Session

from app.kafka_poc_3.models.ProcessedOrder import ProcessedOrder


class ProcessedOrderRepository:

    @staticmethod
    def exists(
            db: Session,
            order_id: int
    ) -> bool:
        return (
                db.query(ProcessedOrder)
                .filter(
                    ProcessedOrder.order_id == order_id
                )
                .first()
                is not None
        )

    @staticmethod
    def save(
            db: Session,
            order_id: int
    ) -> ProcessedOrder:
        processed_order = ProcessedOrder(
            order_id=order_id,
            processed_at=datetime.utcnow()
        )

        db.add(processed_order)
        db.commit()
        db.refresh(processed_order)

        return processed_order
