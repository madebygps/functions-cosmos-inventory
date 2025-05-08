from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime, timezone
import uuid

class Item(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category: str  # Used as partition key
    description: Optional[str] = None
    quantity: int = 0
    price: float
    tags: List[str] = []
    status: str = "in_stock"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = None
    
    @field_validator('status')
    def validate_status(cls, value):
        allowed_statuses = ['in_stock', 'low_stock', 'out_of_stock']
        if value not in allowed_statuses:
            raise ValueError(f"Status must be one of {allowed_statuses}")
        return value
    
    def to_dict(self):
        """Convert to dictionary for Cosmos DB"""
        data = self.model_dump(by_alias=True)
        if data.get('created_at'):
            data['created_at'] = self.created_at.isoformat()
        if data.get('updated_at') and self.updated_at:
            data['updated_at'] = self.updated_at.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create Item from Cosmos DB dictionary"""
        for dt_field in ['created_at', 'updated_at']:
            if dt_field in data and isinstance(data[dt_field], str) and data[dt_field]:
                data[dt_field] = datetime.fromisoformat(data[dt_field])
        return cls(**data)