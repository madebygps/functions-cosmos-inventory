import os
from azure.cosmos.aio import CosmosClient
from azure.identity import DefaultAzureCredential

async def get_cosmos_client():
    """Create a configured Cosmos DB client with proper lifecycle management"""
    endpoint = os.environ.get("COSMOSDB_ENDPOINT")
    credential = DefaultAzureCredential()
    
    client = CosmosClient(
        endpoint, 
        credential=credential,
        preferred_locations=['East US', 'West US'], 
        consistency_level='Session', 
        connection_retry_policy={
            'max_retry_attempts': 9,
            'max_retry_wait_time_in_seconds': 30,
            'fixed_retry_interval_in_milliseconds': 1000
        }
    )

    try:
        yield client
    finally:
        await client.close()