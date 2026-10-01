from pydantic import BaseModel, Field


class OrderRequest(BaseModel):
    order_id: int = Field(..., gt=0, description="Order ID must be a positive integer")
    user_id: int = Field(..., gt=0, description="User ID must be a positive integer")
    event_type: str = Field(..., description="Event type string")
    amount: float = Field(..., gt=0, description="Order amount must be greater than zero")
