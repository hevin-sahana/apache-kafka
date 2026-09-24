from pydantic import BaseModel


class OrderRequest(BaseModel):
    order_id: int
    customer_id: int
    product_id: int
    quantity: int
    amount: float
