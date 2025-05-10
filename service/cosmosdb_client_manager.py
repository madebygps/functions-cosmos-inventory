import os
from azure.cosmos.aio import CosmosClient

class CosmosClientManager:
    def __init__(self, client=None):
        self.client = client
        self.database_name = os.environ.get("COSMOSDB_DATABASE", "inventory")
        self.container_name = os.environ.get("COSMOSDB_CONTAINER", "items")
        self.database = None
        self.container = None

    async def __aenter__(self):
        await self.initialize()
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def initialize(self):
        if self.client is None:
            endpoint = os.environ.get("COSMOSDB_ENDPOINT")
            key = os.environ.get("COSMOSDB_KEY")
            self.client = CosmosClient(endpoint, credential=key)

        self.database = self.client.get_database_client(self.database_name)
        self.container = self.database.get_container_client(self.container_name)
        return self

    async def close(self):
        if self.client:
            await self.client.close()

    async def _ensure_initialized(self):
        if self.database is None:
            await self.initialize()
