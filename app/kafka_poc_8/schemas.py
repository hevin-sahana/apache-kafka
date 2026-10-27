from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class ProductCreate(BaseModel):
    name: str = Field(..., example="Wireless Headphones")
    description: Optional[str] = Field(None,
                                       example="Noise cancelling Bluetooth headphones")
    price: float
    category: Optional[str] = Field(None, example="Electronics")
    quantity: int = Field(..., gt=0)


class ProductResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    price: float
    category: Optional[str] = None
    quantity: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
