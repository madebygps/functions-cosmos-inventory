from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone
import uuid
from enum import Enum


class ItemStatus(str, Enum):
    IN_STOCK = "in_stock"
    LOW_STOCK = "low_stock"
    OUT_OF_STOCK = "out_of_stock"

class Item(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category: str 
    description: Optional[str] = None
    quantity: int = 0
    price: float
    tags: List[str] = []
    status: ItemStatus = ItemStatus.IN_STOCK
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None
    class Config:
        schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "name": "Wireless Mouse",
                "category": "electronics",
                "description": "Ergonomic wireless mouse",
                "quantity": 15,
                "price": 29.99,
                "tags": ["peripheral", "wireless"],
                "status": "in_stock",
                "created_at": "2025-05-10T12:00:00Z",
                "updated_at": "2025-05-10T12:00:00Z"
            }
        }

class ItemUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[int] = None
    price: Optional[float] = None
    tags: Optional[List[str]] = None
    status: Optional[ItemStatus] = None
    class Config:
        schema_extra = {
            "example": {
                "description": "Updated description",
                "quantity": 20
            }
        }