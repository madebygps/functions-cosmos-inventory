import os
import logging
from typing import List, Optional
from azure.cosmos import exceptions  # Import from main cosmos package
from model.inventory_item import Item

class CosmosService:
    def __init__(self, client=None):
        """Initialize service with client"""
        self.client = client
        self.database_name = os.environ.get("COSMOSDB_DATABASE", "inventory")
        self.container_name = os.environ.get("COSMOSDB_CONTAINER", "items")
        
        # Database and container clients will be initialized when needed
        self.database = None
        self.container = None
    
    async def _ensure_initialized(self):
        """Ensure database and container clients are initialized"""
        if self.database is None:
            self.database = self.client.get_database_client(self.database_name)
            self.container = self.database.get_container_client(self.container_name)
    
    async def create_item(self, item: Item) -> Item:
        """Create a new inventory item in Cosmos DB"""
        await self._ensure_initialized()
        try:
            data = item.to_dict()
            response = await self.container.create_item(body=data)
            return Item.from_dict(response)
        except exceptions.CosmosResourceExistsError:
            logging.error(f"Item with id {item.id} already exists")
            raise ValueError(f"Item with id {item.id} already exists")
        
    async def list_items(self, category: Optional[str] = None, max_items: int = 100) -> List[Item]:
        """List inventory items, optionally filtered by category"""
        await self._ensure_initialized()
        
        if category:
            query = "SELECT * FROM c WHERE c.category = @category"
            parameters = [{"name": "@category", "value": category}]
            # When category is provided, we can query within a single partition
            query_items = self.container.query_items(
                query=query,
                parameters=parameters,
                max_item_count=max_items
            )
        else:
            query = "SELECT * FROM c"
            parameters = []
            # When no category is provided, we need to enable cross-partition query
            query_items = self.container.query_items(
                query=query,
                parameters=parameters,
                max_item_count=max_items # Enable cross-partition query
            )
        
        items = []
        async for item in query_items:
            items.append(Item.from_dict(item))
            
        return items