import logging
import azure.functions as func
from fastapi import FastAPI, Depends, HTTPException, status
from typing import List, Optional
from datetime import datetime

from model.inventory_item import Item
from service.cosmosdb_service import CosmosService
from service.dependency import get_cosmos_client

app = FastAPI()

def get_cosmos_service(client=Depends(get_cosmos_client)):
    return CosmosService(client)

@app.post("/api/item", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_inventory_item(item: Item, cosmos_service: CosmosService = Depends(get_cosmos_service)):
    try:
        return await cosmos_service.create_item(item)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logging.error(f"Error creating item: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.get("/api/items", response_model=List[Item])
async def list_inventory_items(
    category: Optional[str] = None, 
    cosmos_service: CosmosService = Depends(get_cosmos_service)
):
    try:
        return await cosmos_service.list_items(category)
    except Exception as e:
        logging.error(f"Error listing items: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.get("/api/item/{item_id}", response_model=Item)
async def get_inventory_item(
    item_id: str, 
    category: str, 
    cosmos_service: CosmosService = Depends(get_cosmos_service)
):
    item = await cosmos_service.get_item(item_id, category)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Item with ID {item_id} in category {category} not found"
        )
    return item

@app.put("/api/item/{item_id}", response_model=Item)
async def update_inventory_item(
    item_id: str, 
    item_update: Item,
    cosmos_service: CosmosService = Depends(get_cosmos_service)
):
    if item_id != item_update.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Path ID doesn't match item ID"
        )
    
    item_update.updated_at = datetime.now(datetime.timezone.utc)
    
    try:
        return await cosmos_service.update_item(item_update)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        logging.error(f"Error updating item: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))

@app.delete("/api/item/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_inventory_item(
    item_id: str, 
    category: str,
    cosmos_service: CosmosService = Depends(get_cosmos_service)
):
    success = await cosmos_service.delete_item(item_id, category)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Item with ID {item_id} in category {category} not found"
        )
    return None


function_app = func.FunctionApp()

@function_app.route(route="{*route}", auth_level=func.AuthLevel.ANONYMOUS)
async def main(req: func.HttpRequest) -> func.HttpResponse:
    """Each function is automatically registered through the @function_app decorator."""
    logging.info('Python HTTP trigger function processed a request.')
    return await func.AsgiMiddleware(app).handle_async(req)