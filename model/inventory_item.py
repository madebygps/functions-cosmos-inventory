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