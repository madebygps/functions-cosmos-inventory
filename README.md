# Inventory System with Cosmos DB, FastAPI on Azure Functions

## Overview

This project implements an inventory management system using Azure Cosmos DB as the database backend, FastAPI for API development, and deployed as serverless Azure Functions.

## Architecture

- **Azure Functions**: Serverless compute for hosting the API endpoints
- **FastAPI**: Modern, fast web framework for building APIs
- **Azure Cosmos DB**: NoSQL database for storing inventory data
- **Python**: Primary programming language

## Prerequisites

- Azure subscription
- Python 3.8+
- Azure Functions Core Tools v4+
- Azure CLI
- Azure Developer CLI (azd) for simplified deployment
- Azure Cosmos DB account or Cosmos DB Emulator for local development
- Visual Studio Code with Azure Functions extension (recommended)
- Azure Developer CLI

## Setup and Development

> **IMPORTANT**: You must deploy the Azure infrastructure (using `azd up`) or set up the Cosmos DB emulator BEFORE attempting to run the application locally. This ensures you have a Cosmos DB account to connect to.

### 1. Clone the repository

```bash
git clone https://github.com/madebygps/functions-cosmos-inventory.git
cd functions-cosmos-inventory
```

### 1. Deploy Azure Resources with AZD

```bash
# Login to Azure
azd auth login

# Deploy the application infrastructure
azd up
```

### 1. Create a virtual environment and install dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 1. Grant your user principal access to the Cosmos DB account

```bash
# Use the script provided in cosmosdb_access.sh
az cosmosdb sql role assignment create \
  --account-name <your-deployed-cosmos-account-name> \
  --resource-group <your-resource-group> \
  --scope "/" \
  --principal-id $(az ad signed-in-user show --query id -o tsv) \
  --role-definition-id "00000000-0000-0000-0000-000000000002"
```

You can find the values to these in the .azure folder

### 1. Configure local settings

```bash
cp local-sample.settings.json local.settings.json
```

Update `local.settings.json` with your Azure Cosmos DB information:

#### If using Azure Cosmos DB

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "COSMOSDB_ENDPOINT": "YOUR_DEPLOYED_COSMOS_DB_ENDPOINT",
    "COSMOSDB_DATABASE": "inventory",
    "COSMOSDB_CONTAINER": "items"
  }
}
```

#### If using Cosmos DB Emulator

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "COSMOSDB_ENDPOINT": "https://localhost:8081",
    "COSMOSDB_KEY": "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
    "COSMOSDB_DATABASE": "inventory",
    "COSMOSDB_CONTAINER": "items"
  }
}
```

If using the emulator, you'll need to modify `service/dependency.py` to use a connection key instead of RBAC.

### 1. Run the function locally

```bash
func start
```

Alternatively, in VS Code, press F5 or use the Azure Functions extension to start debugging.

### 1. Load sample data (optional)

```bash
python load_data.py
```

## API Endpoints

- `POST /api/item`: Create a new inventory item
- `GET /api/items`: List all inventory items
  - Query parameter: `category` (optional) - Filter by category
- `GET /api/item/{item_id}`: Get an item by ID
  - Query parameter: `category` (required) - The category of the item
- `PUT /api/item/{item_id}`: Update an existing item
- `DELETE /api/item/{item_id}`: Delete an item
  - Query parameter: `category` (required) - The category of the item

## Data Model

Inventory items follow this schema:

```python
{
    "id": "string", # UUID, auto-generated if not provided
    "name": "string", # Required
    "category": "string", # Required - Used as partition key
    "description": "string", # Optional
    "quantity": int, # Default: 0
    "price": float, # Required
    "tags": ["string"], # Optional list of tags
    "status": "string", # in_stock, low_stock, or out_of_stock
    "created_at": "datetime", # Auto-generated
    "updated_at": "datetime" # Auto-updated on PUT requests
}
```
