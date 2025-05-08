import os
import json
import asyncio
import logging
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions, PartitionKey
from azure.identity import DefaultAzureCredential
from pathlib import Path
from model.inventory_item import Item
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration (replace with your actual values or use environment variables)
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
DATABASE_NAME = os.getenv("COSMOS_DB_NAME")
CONTAINER_NAME = os.getenv("COSMOS_CONTAINER_NAME")


async def create_database_if_not_exists(client):
    """Create the database if it doesn't exist."""
    try:
        database = await client.create_database_if_not_exists(id=DATABASE_NAME)
        logger.info(f"Database '{DATABASE_NAME}' ensured")
        return database
    except exceptions.CosmosHttpResponseError as e:
        logger.error(f"Failed to create database: {e}")
        raise

async def create_container_if_not_exists(database):
    """Create the container if it doesn't exist."""
    try:
        # Consider setting autoscale ughput for production workloads
        container = await database.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=PartitionKey(path="/category"),
            offer_throughput=400  # Minimum RU/s
        )
        logger.info(f"Container '{CONTAINER_NAME}' ensured")
        return container
    except exceptions.CosmosHttpResponseError as e:
        logger.error(f"Failed to create container: {e}")
        raise

def map_json_to_item(item_data):
    """Map fields from JSON structure to Item model structure"""
    mapped_data = item_data.copy()

    return mapped_data

async def load_sample_data(container):
    """Load sample data from the JSON file into the container."""
    try:
        # Check if sample_data.json exists
        sample_data_path = Path("sample_data.json")
        if not sample_data_path.exists():
            logger.error(f"Sample data file not found: {sample_data_path.absolute()}")
            raise FileNotFoundError(f"Could not find {sample_data_path.absolute()}")
        
        # Load JSON data from file
        with open(sample_data_path, 'r') as file:
            sample_items_raw = json.load(file)
        
        logger.info(f"Loaded {len(sample_items_raw)} items from sample_data.json")
        
        # First, collect all existing item names
        query = "SELECT c.name FROM c"
        existing_names = set()
        async for item in container.query_items(query):
            existing_names.add(item['name'])
        
        # Create validated Item objects
        items_to_insert = []
        invalid_items = []
        for item_data in sample_items_raw:
            try:
                # Map JSON fields to Item model fields
                mapped_data = map_json_to_item(item_data)
                
                # Skip items that already exist
                if mapped_data.get('name') in existing_names:
                    logger.info(f"Skipping existing item: {mapped_data.get('name')}")
                    continue
                
                # Create a validated Item object
                item = Item(**mapped_data)
                items_to_insert.append(item)
            except Exception as e:
                invalid_items.append((item_data.get('name', 'Unknown'), str(e)))
                logger.warning(f"Invalid item data: {item_data.get('name', 'Unknown')}. Error: {str(e)}")
        
        # Log validation results
        if invalid_items:
            logger.warning(f"Skipped {len(invalid_items)} invalid items")
            for name, error in invalid_items:
                logger.debug(f"- {name}: {error}")
        
        # Batch process items with RU monitoring
        total_rus = 0
        batch_size = 10  # Adjust based on your item size and RU capacity
        
        for i in range(0, len(items_to_insert), batch_size):
            batch = items_to_insert[i:i + batch_size]
            
            # Process each item in the batch
            for item in batch:
                try:
                    # Use parameterized query to check if item exists (extra safety)
                    query = "SELECT * FROM c WHERE c.name = @name"
                    parameters = [{"name": "@name", "value": item.name}]
                    
                    # Convert to dictionary for Cosmos DB
                    item_dict = item.to_dict()
                    
                    # Create the item
                    response = await container.create_item(body=item_dict)
                    
                    # Track RU consumption
                    request_charge = response['_response_headers']['x-ms-request-charge']
                    total_rus += float(request_charge)
                    
                    logger.info(f"Added item: {item.name} (ID: {item.id}) - {request_charge} RUs")
                    
                    # If we're consuming too many RUs, add a small delay
                    if float(request_charge) > 20:  # Adjust threshold as needed
                        await asyncio.sleep(0.5)  # Throttle requests
                
                except exceptions.CosmosResourceExistsError:
                    logger.info(f"Item already exists: {item.name}")
                except Exception as e:
                    logger.error(f"Error creating item {item.name}: {str(e)}")
        
        logger.info(f"Sample data loading completed. {len(items_to_insert)} items inserted. Total RU consumption: {total_rus:.2f}")
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        raise
    except exceptions.CosmosHttpResponseError as e:
        logger.error(f"Cosmos DB error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

async def main():
    """Main function to initialize DB and load data."""
    try:
        # Always use DefaultAzureCredential 
        credential = DefaultAzureCredential()
        client = CosmosClient(COSMOS_ENDPOINT, credential=credential)
        logger.info("Using Azure identity authentication")
        
        async with client:
            # Create database and container if they don't exist
            database = await create_database_if_not_exists(client)
            container = await create_container_if_not_exists(database)
            
            # Load the sample data
            await load_sample_data(container)
            
            logger.info("Script completed successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())