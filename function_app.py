import logging
from azure.cosmos import exceptions as cosmos_exceptions
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import azure.functions as func
from fastapi import FastAPI, Depends, HTTPException, status, Query, Request, Header
from fastapi.responses import JSONResponse

from model.inventory_item import Item, ItemUpdate
from service.cosmosdb_service import CosmosService
from service.dependency import get_cosmos_client

app = FastAPI(
    title="Inventory API",
    version="1.0.0",
    openapi_url="/api/openapi.json",  
    docs_url="/api/docs",            
)

@app.exception_handler(cosmos_exceptions.CosmosHttpResponseError)
async def handle_cosmos_http_error(request: Request, exc: cosmos_exceptions.CosmosHttpResponseError):
    if exc.status_code == 401:
        return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Unauthorized to access Cosmos DB"})
    if exc.status_code == 403:
        return JSONResponse(status_code=status.HTTP_403_FORBIDDEN, content={"detail": "Forbidden to access Cosmos DB"})
    logging.error(f"Cosmos DB HTTP error: {exc}")
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content={"detail": str(exc)})

@app.exception_handler(ValueError)
async def handle_value_error(request: Request, exc: ValueError):
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(exc)})


def get_cosmos_service(client=Depends(get_cosmos_client)):
    """Return a CosmosService instance with a managed client."""
    return CosmosService(client)


@app.post("/api/items", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_item(item: Item, cosmos_service: CosmosService = Depends(get_cosmos_service)):
    return await cosmos_service.create_item(item)


@app.post("/api/items/batch", response_model=List[Item], status_code=status.HTTP_201_CREATED)
async def batch_create_items(items: List[Item], cosmos_service: CosmosService = Depends(get_cosmos_service)):
    """Create multiple inventory items transactionally."""
    return await cosmos_service.batch_create_items(items)


@app.get("/api/items", response_model=Dict[str, Any])
async def list_items(
    category: Optional[str] = None,
    page_size: int = Query(default=20, ge=1, le=100),
    continuation_token: Optional[str] = None,
    cosmos_service: CosmosService = Depends(get_cosmos_service),
):
    """List inventory items with optional pagination."""
    items, next_token = await cosmos_service.list_items(category, page_size, continuation_token)
    if continuation_token is None and next_token is None:
        return {"items": items}
    return {"items": items, "continuation_token": next_token, "has_more": next_token is not None}


@app.get("/api/items/{item_id}", response_model=Item)
async def get_item(item_id: str, category: str, cosmos_service: CosmosService = Depends(get_cosmos_service)):
    item = await cosmos_service.get_item(item_id, category)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item {item_id} in {category} not found")
    return item


@app.post("/api/items/batch-read", response_model=List[Item])
async def batch_read_items(items: List[Dict[str, str]], cosmos_service: CosmosService = Depends(get_cosmos_service)):
    return await cosmos_service.batch_read_items(items)


@app.put("/api/items/{item_id}", response_model=Item)
async def update_item(item_id: str, item_update: Item, cosmos_service: CosmosService = Depends(get_cosmos_service)):
    if item_id != item_update.id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Path ID doesn't match body ID")
    return await cosmos_service.update_item(item_update)

@app.patch("/api/items/{item_id}", response_model=Item)
async def patch_item(
    item_id: str,
    category: str,
    updates: ItemUpdate,
    if_match: Optional[str] = Header(None, alias="If-Match"),
    cosmos_service: CosmosService = Depends(get_cosmos_service),
):
    """Partial update of inventory item; supply only fields to change."""
    update_dict = updates.model_dump(exclude_unset=True)
    return await cosmos_service.patch_item(item_id, category, update_dict, etag=if_match)


@app.put("/api/items/batch", response_model=List[Item])
async def batch_update_items(items: List[Item], cosmos_service: CosmosService = Depends(get_cosmos_service)):
    for item in items:
        item.updated_at = datetime.now(timezone.utc)
    return await cosmos_service.batch_update_items(items)


@app.delete("/api/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_item(item_id: str, category: str, cosmos_service: CosmosService = Depends(get_cosmos_service)):
    deleted = await cosmos_service.delete_item(item_id, category)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Item {item_id} in {category} not found")
    return None


@app.delete("/api/items/batch", status_code=status.HTTP_200_OK)
async def batch_delete_items(items: List[Dict[str, str]], cosmos_service: CosmosService = Depends(get_cosmos_service)):
    deleted = await cosmos_service.batch_delete_items(items)
    return {"deleted_count": len(deleted), "items": deleted}


function_app = func.FunctionApp()

@function_app.route(route="{*route}", auth_level=func.AuthLevel.FUNCTION)
async def main(req: func.HttpRequest) -> func.HttpResponse:
    """Azure Functions entrypoint routing through FastAPI app."""
    logging.info("Python HTTP trigger function processed a request.")
    return await func.AsgiMiddleware(app).handle_async(req)
