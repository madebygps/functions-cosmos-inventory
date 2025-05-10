import os
import json
import asyncio
import logging
from azure.cosmos.aio import CosmosClient
from azure.cosmos import exceptions
from azure.identity import DefaultAzureCredential
from pathlib import Path

from fastapi.encoders import jsonable_encoder
from model.inventory_item import Item
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

COSMOS_ENDPOINT = os.environ.get('COSMOSDB_ENDPOINT', 'https://serverlessinvenghyi746x3-cosmos.documents.azure.com:443/')
DATABASE_NAME = os.environ.get('COSMOSDB_DATABASE', 'inventory')
CONTAINER_NAME = os.environ.get('COSMOSDB_CONTAINER', 'items')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))
THROTTLE_THRESHOLD = float(os.environ.get('THROTTLE_THRESHOLD', '20'))
DATA_FILE_PATH = os.environ.get('DATA_FILE_PATH', 'sample_data.json')


async def get_existing_items(container, query="SELECT c.name FROM c"):
    """Query existing items to avoid duplicates"""
    existing_names = set()
    
    async for item in container.query_items(query=query, parameters=[]):
        existing_names.add(item['name'])
    
    return existing_names


async def insert_item(container, item):
    """Insert a single item with RU tracking"""
    try:
        item_dict = jsonable_encoder(item)
        response = await container.create_item(body=item_dict, partition_key=item.category)
        request_charge = response['_response_headers']['x-ms-request-charge']
        logger.info(f"Added item: {item.name} (ID: {item.id}) - {request_charge} RUs")
        
        return float(request_charge)
    except exceptions.CosmosResourceExistsError:
        logger.info(f"Item already exists: {item.name}")
        return 0
    except Exception as e:
        logger.error(f"Error creating item {item.name}: {str(e)}")
        raise


async def load_sample_data(container, data_file_path, batch_size, throttle_threshold):
    """Load sample data from the JSON file into the container."""
    try:
        file_path = Path(data_file_path)
        if not file_path.exists():
            logger.error(f"Sample data file not found: {file_path.absolute()}")
            raise FileNotFoundError(f"Could not find {file_path.absolute()}")
        
        with open(file_path, 'r') as file:
            sample_items_raw = json.load(file)
        
        logger.info(f"Loaded {len(sample_items_raw)} items from {data_file_path}")
        
        existing_names = await get_existing_items(container)
        logger.info(f"Found {len(existing_names)} existing items in the container")
        
        items_to_insert = []
        invalid_items = []
        for item_data in sample_items_raw:
            try:
                if item_data.get('name') in existing_names:
                    logger.info(f"Skipping existing item: {item_data.get('name')}")
                    continue
        
                item = Item(**item_data)
                items_to_insert.append(item)
            except Exception as e:
                invalid_items.append((item_data.get('name', 'Unknown'), str(e)))
                logger.warning(f"Invalid item data: {item_data.get('name', 'Unknown')}. Error: {str(e)}")
        
        if invalid_items:
            logger.warning(f"Skipped {len(invalid_items)} invalid items")
            for name, error in invalid_items:
                logger.debug(f"- {name}: {error}")
        
        logger.info(f"Preparing to insert {len(items_to_insert)} new items")
        
        total_rus = 0
        
        for i in range(0, len(items_to_insert), batch_size):
            batch = items_to_insert[i:i + batch_size]
            batch_rus = 0
            
            logger.info(f"Processing batch {i//batch_size + 1} of {(len(items_to_insert) + batch_size - 1)//batch_size}")

            for item in batch:
                try:
                    request_charge = await insert_item(container, item)
                    total_rus += request_charge
                    batch_rus += request_charge
                except Exception as e:
                    logger.error(f"Failed to insert item {item.name}: {str(e)}")
            
            if batch_rus > throttle_threshold * len(batch):
                logger.info(f"Throttling after batch consumed {batch_rus:.2f} RUs")
                await asyncio.sleep(1.0) 
        
        logger.info(f"Sample data loading completed. {len(items_to_insert)} items inserted. Total RU consumption: {total_rus:.2f}")
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
        raise


async def main():
    """Main function to load data into existing container."""
    try:
        credential = DefaultAzureCredential()
    
        async with CosmosClient(COSMOS_ENDPOINT, credential=credential) as client:
            logger.info(f"Connecting to {COSMOS_ENDPOINT} using Azure identity authentication")
            
            database = client.get_database_client(DATABASE_NAME)
            container = database.get_container_client(CONTAINER_NAME)
            
            logger.info(f"Connected to database '{DATABASE_NAME}' and container '{CONTAINER_NAME}'")
            
            await load_sample_data(
                container,
                data_file_path=DATA_FILE_PATH,
                batch_size=BATCH_SIZE,
                throttle_threshold=THROTTLE_THRESHOLD
            )
            
            logger.info("Script completed successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())