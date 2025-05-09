import os
from azure.cosmos.aio import CosmosClient
from azure.identity import DefaultAzureCredential

async def get_cosmos_client():
    """Create a configured Cosmos DB client"""
    endpoint = os.environ.get("COSMOSDB_ENDPOINT")
    credential = DefaultAzureCredential()
    
    # Performance configuration
    return CosmosClient(
        endpoint, 
        credential=credential,
        preferred_locations=['East US', 'West US'],  # Geo-distribution
        consistency_level='Session',  # Tuned consistency
        connection_retry_policy={
            'max_retry_attempts': 9,
            'max_retry_wait_time_in_seconds': 30,
            'fixed_retry_interval_in_milliseconds': 1000
        }
    )