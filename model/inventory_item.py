from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict
from datetime import datetime
import uuid

class Item(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    category: str 
    document_type: str = "item"  
    description: Optional[str] = None
    quantity: int = 0
    price: float
    cost: float
    supplier_id: Optional[str] = None
    location: Optional[str] = None 
    tags: List[str] = []
    status: str = "in_stock" 
    reorder_point: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))
    updated_at: Optional[datetime] = None
    metadata: Optional[Dict] = Field(default_factory=dict)
    
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
        if data.get('updated_at'):
            data['updated_at'] = self.updated_at.isoformat() if self.updated_at else None
        return data
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create Item from Cosmos DB dictionary"""
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if 'updated_at' in data and isinstance(data['updated_at'], str) and data['updated_at']:
            data['updated_at'] = datetime.fromisoformat(data['updated_at'])
        return cls(**data)