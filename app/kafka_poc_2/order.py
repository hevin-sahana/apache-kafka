from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    order_id: int = Field(..., gt=0)
    user_id: int = Field(..., gt=0)
    event_type: str = Field(..., min_length=1)
    sequence: int = Field(..., gt=0)
    amount: float = Field(..., gt=0)

    # sequence = 1 → ORDER_CREATED
    # sequence = 2 → PAYMENT_COMPLETED
    # sequence = 3 → ORDER_SHIPPED
    # sequence = 4 → ORDER_DELIVERED