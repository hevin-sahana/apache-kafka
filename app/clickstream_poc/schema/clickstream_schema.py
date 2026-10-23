from pydantic import BaseModel, Field
from typing import Optional


class ClickstreamEvent(BaseModel):
    event_id: str
    event_type: str = Field(
        ...,
        pattern="^(page_view|product_clicked|add_to_cart|purchase_completed)$"
    )
    user_id: Optional[str] = None
    session_id: str
    page_url: str
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    timestamp: str


