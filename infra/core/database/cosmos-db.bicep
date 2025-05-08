@description('Name of the Cosmos DB account')
param name string

@description('Azure region where the Cosmos DB account will be deployed')
param location string = resourceGroup().location

@description('Tags to apply to the Cosmos DB account')
param tags object = {}

@description('Name of the database to create in the Cosmos DB account')
param databaseName string

@description('Name of the container to create in the Cosmos DB database')
param containerName string


@description('Path used for the partition key')
param partitionKeyPath string = '/category'

resource cosmosDbAccount 'Microsoft.DocumentDB/databaseAccounts@2023-04-15' = {
  name: name
  location: location
  tags: tags
  kind: 'GlobalDocumentDB'
  properties: {
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
    locations: [
      {
        locationName: location  // Changed from location to locationName
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    capabilities: [
      {
        name: 'EnableServerless'
      }
    ]
    databaseAccountOfferType: 'Standard'
    enableAutomaticFailover: false
    enableMultipleWriteLocations: false
    publicNetworkAccess: 'Enabled'
  }
}

resource cosmosDatabase 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2023-04-15' = {
  name: databaseName
  parent: cosmosDbAccount
  properties: {
    resource: {
      id: databaseName
    }
  }
}

resource cosmosContainer 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2023-04-15' = {
  name: containerName
  parent: cosmosDatabase
  properties: {
    resource: {
      id: containerName
      partitionKey: {
        paths: [
          partitionKeyPath
        ]
        kind: 'Hash'
      }
    }
    // No throughput property as serverless doesn't support it
  }
}

output cosmosEndpoint string = cosmosDbAccount.properties.documentEndpoint
output cosmosDatabaseName string = cosmosDatabase.name
output cosmosContainerName string = cosmosContainer.name
output cosmosAccountId string = cosmosDbAccount.id
output cosmosAccountName string = cosmosDbAccount.name
