import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple, Any
from fastapi.encoders import jsonable_encoder
from azure.cosmos import exceptions
from model.inventory_item import Item
from service.cosmosdb_client_manager import CosmosClientManager

class CosmosService(CosmosClientManager):
    async def create_item(self, item: Item) -> Item:
        await self._ensure_initialized()
        try:
            data = jsonable_encoder(item)
            response = await self.container.create_item(body=data, partition_key=item.category)
            return Item(**response)
        except exceptions.CosmosResourceExistsError:
            msg = f"Item with id {item.id} already exists"
            logging.error(msg)
            raise ValueError(msg)

    async def batch_create_items(self, items: List[Item]) -> List[Item]:
        await self._ensure_initialized()
        items_by_category = {}
        for item in items:
            items_by_category.setdefault(item.category, []).append(item)
        created_items = []
        for category, category_items in items_by_category.items():
            batch_operations = [("create", (jsonable_encoder(i),), {}) for i in category_items]
            try:
                results = await self.container.execute_item_batch(batch_operations=batch_operations, partition_key=category)
                created_items.extend(Item(**r) for r in results)
            except exceptions.CosmosBatchOperationError as e:
                i = e.error_index
                logging.error(f"Batch failed: {batch_operations[i]}, response: {e.operation_responses[i]}")
                raise ValueError(f"Batch failed: {e.operation_responses[i]}")
        return created_items

    async def update_item(self, item: Item, etag: Optional[str] = None) -> Item:
        """Full replace with optimistic concurrency and timestamp logic."""
        await self._ensure_initialized()
        # validate immutable fields
        existing = await self.get_item(item.id, item.category)
        if not existing:
            raise ValueError(f"Item with id {item.id} not found")
        if item.category != existing.category:
            raise ValueError("Cannot change category on update")
        if item.created_at != existing.created_at:
            raise ValueError("Cannot modify created_at on update")
        # set updated timestamp
        item.updated_at = datetime.now(timezone.utc)
        data = jsonable_encoder(item)
        # concurrency control via ETag
        options = {}
        if etag:
            options['etag'] = etag
            options['match_condition'] = exceptions.MatchConditions.IfNotModified
        try:
            response = await self.container.replace_item(
                item=item.id,
                body=data,
                partition_key=item.category,
                **options
            )
            return Item(**response)
        except exceptions.CosmosResourceNotFoundError:
            msg = f"Item with id {item.id} not found"
            logging.error(msg)
            raise ValueError(msg)
        except exceptions.CosmosHttpResponseError as e:
            if isinstance(e, exceptions.CosmosAccessConditionFailedError):
                raise ValueError("Update conflict: resource was modified by another process")
            raise

    async def patch_item(self, item_id: str, category: str, updates: Dict[str, Any], etag: Optional[str] = None) -> Item:
        """Partial update via patch semantics, merging changes and then replace."""
        await self._ensure_initialized()
        existing = await self.container.read_item(item=item_id, partition_key=category)
        # apply immutables check
        if existing.get('category') != category:
            raise ValueError("Cannot change category on patch")
        if 'created_at' in updates and updates['created_at'] != existing.get('created_at'):
            raise ValueError("Cannot modify created_at on patch")
        # merge updates
        for k, v in updates.items():
            existing[k] = v
        # update timestamp
        existing['updated_at'] = datetime.now(timezone.utc).isoformat()
        # concurrency control
        options = {}
        if etag:
            options['etag'] = etag
            options['match_condition'] = exceptions.MatchConditions.IfNotModified
        response = await self.container.replace_item(
            item=item_id,
            body=existing,
            partition_key=category,
            **options
        )
        return Item(**response)

    async def batch_update_items(self, items: List[Item]) -> List[Item]:
        await self._ensure_initialized()
        items_by_category = {}
        for item in items:
            items_by_category.setdefault(item.category, []).append(item)
        updated_items = []
        for category, category_items in items_by_category.items():
            batch_operations = [("replace", (i.id, jsonable_encoder(i)), {}) for i in category_items]
            try:
                results = await self.container.execute_item_batch(batch_operations=batch_operations, partition_key=category)
                updated_items.extend(Item(**r) for r in results)
            except exceptions.CosmosBatchOperationError as e:
                i = e.error_index
                logging.error(f"Batch failed: {batch_operations[i]}, response: {e.operation_responses[i]}")
                raise ValueError(f"Batch failed: {e.operation_responses[i]}")
        return updated_items

    async def get_item(self, item_id: str, category: str) -> Optional[Item]:
        await self._ensure_initialized()
        try:
            response = await self.container.read_item(item=item_id, partition_key=category)
            return Item(**response)
        except exceptions.CosmosResourceNotFoundError:
            return None

    async def batch_read_items(self, items: List[Dict[str, str]]) -> List[Item]:
        await self._ensure_initialized()
        items_by_category = {}
        for item in items:
            items_by_category.setdefault(item["category"], []).append(item)
        read_items = []
        for category, category_items in items_by_category.items():
            batch_operations = [("read", (i["id"],), {}) for i in category_items]
            try:
                results = await self.container.execute_item_batch(batch_operations=batch_operations, partition_key=category)
                read_items.extend(Item(**r) for r in results)
            except exceptions.CosmosBatchOperationError as e:
                i = e.error_index
                logging.error(f"Batch failed: {batch_operations[i]}, response: {e.operation_responses[i]}")
                raise ValueError(f"Batch failed: {e.operation_responses[i]}")
        return read_items

    async def delete_item(self, item_id: str, category: str) -> bool:
        await self._ensure_initialized()
        try:
            await self.container.delete_item(item=item_id, partition_key=category)
            return True
        except exceptions.CosmosResourceNotFoundError:
            return False

    async def batch_delete_items(self, items: List[Dict[str, str]]) -> List[Dict[str, str]]:
        await self._ensure_initialized()
        items_by_category = {}
        for item in items:
            items_by_category.setdefault(item["category"], []).append(item)
        deleted_items = []
        for category, category_items in items_by_category.items():
            batch_operations = [("delete", (i["id"],), {}) for i in category_items]
            try:
                await self.container.execute_item_batch(batch_operations=batch_operations, partition_key=category)
                deleted_items.extend(category_items)
            except exceptions.CosmosBatchOperationError as e:
                i = e.error_index
                logging.error(f"Batch failed: {batch_operations[i]}, response: {e.operation_responses[i]}")
                raise ValueError(f"Batch failed: {e.operation_responses[i]}")
        return deleted_items

    async def list_items(self, category: Optional[str] = None, max_items: int = 100, continuation_token: Optional[str] = None) -> Tuple[List[Item], Optional[str]]:
        await self._ensure_initialized()
        query = "SELECT * FROM c"
        parameters = []
        if category:
            query += " WHERE c.category = @category"
            parameters.append({"name": "@category", "value": category})
        items = []
        query_response = self.container.query_items(query=query, parameters=parameters, max_item_count=max_items, continuation=continuation_token)
        async for item in query_response:
            items.append(Item(**item))
        next_token = getattr(query_response, 'response_headers', {}).get('x-ms-continuation')
        return items, next_token
