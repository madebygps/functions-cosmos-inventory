import os
import logging
from azure.cosmos import CosmosClient, exceptions
from azure.identity import DefaultAzureCredential
from typing import List, Optional
from model.inventory_item import Item

class CosmosService:
    def __init__(self):
        """Initialize Cosmos DB client with managed identity only"""
        self.endpoint = os.environ.get("COSMOSDB_ENDPOINT")
        if not self.endpoint:
            raise ValueError("COSMOSDB_ENDPOINT environment variable must be set")
            
        self.database_name = os.environ.get("COSMOSDB_DATABASE", "inventory")
        self.container_name = os.environ.get("COSMOSDB_CONTAINER", "items")
        
        # Only use DefaultAzureCredential
        credential = DefaultAzureCredential()
        self.client = CosmosClient(self.endpoint, credential=credential)
        logging.info("Connected to Cosmos DB using DefaultAzureCredential")
        
        self.database = self.client.get_database_client(self.database_name)
        self.container = self.database.get_container_client(self.container_name)
    
    async def create_item(self, item: Item) -> Item:
        """Create a new inventory item in Cosmos DB"""
        try:
            data = item.to_dict()
            response = self.container.create_item(body=data)
            return Item.from_dict(response)
        except exceptions.CosmosResourceExistsError:
            logging.error(f"Item with id {item.id} already exists")
            raise ValueError(f"Item with id {item.id} already exists")
    
    async def get_item(self, item_id: str, category: str) -> Optional[Item]:
        """Get an item by ID and partition key (category)"""
        try:
            response = self.container.read_item(item=item_id, partition_key=category)
            return Item.from_dict(response)
        except exceptions.CosmosResourceNotFoundError:
            logging.warning(f"Item with id {item_id} in category {category} not found")
            return None
    
    async def update_item(self, item: Item) -> Item:
        """Update an existing inventory item"""
        try:
            data = item.to_dict()
            response = self.container.replace_item(item=item.id, body=data, partition_key=item.category)
            return Item.from_dict(response)
        except exceptions.CosmosResourceNotFoundError:
            logging.error(f"Item with id {item.id} not found for update")
            raise ValueError(f"Item with id {item.id} not found")
    
    async def delete_item(self, item_id: str, category: str) -> bool:
        """Delete an inventory item"""
        try:
            self.container.delete_item(item=item_id, partition_key=category)
            return True
        except exceptions.CosmosResourceNotFoundError:
            logging.warning(f"Item with id {item_id} in category {category} not found for deletion")
            return False
    
    async def list_items(self, category: Optional[str] = None, max_items: int = 100) -> List[Item]:
        """List inventory items, optionally filtered by category"""
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
                max_item_count=max_items,
                enable_cross_partition_query=True  # Enable cross-partition query
            )
        
        items = []
        for item in query_items:
            items.append(Item.from_dict(item))
            
        return items