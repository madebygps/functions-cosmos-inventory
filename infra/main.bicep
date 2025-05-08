targetScope = 'subscription'

@description('Name of the environment which is used to generate a short unique hash used in all resources.')
@minLength(1)
@maxLength(64)
param environmentName string

@description('Primary location for all resources')
@minLength(1)
param location string

@description('Name for the resource group. If empty, a default name will be created based on the environment name.')
param resourceGroupName string = ''

@description('Tags to apply to all resources.')
param tags object = {}

// Variables
var prefix = '${environmentName}-${uniqueString(environmentName)}'
var rgName = resourceGroupName == '' ? 'rg-${environmentName}' : resourceGroupName
var resourceTags = union(tags, { 'azd-env-name': environmentName })

// Create a resource group
resource resourceGroup 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: rgName
  location: location
  tags: resourceTags
}

// Monitor application with Azure Monitor
module monitoringResources 'core/monitor/monitoring.bicep' = {
  name: 'monitoring'
  scope: resourceGroup
  params: {
    location: location
    tags: resourceTags
    logAnalyticsName: '${prefix}-logworkspace'
    applicationInsightsName: '${prefix}-appinsights'
    applicationInsightsDashboardName: '${prefix}-appinsights-dashboard'
  }
}

// Storage for hosting static website
module storageAccount 'core/storage/storage-account.bicep' = {
  name: 'storage'
  scope: resourceGroup
  params: {
    name: '${toLower(take(replace(prefix, '-', ''), 17))}storage'
    location: location
    tags: resourceTags
  }
}

// Cosmos db
module cosmosDatabase 'core/database/cosmos-db.bicep' = {
  name: 'cosmos-db'
  scope: resourceGroup
  params: {
    name: '${toLower(take(replace(prefix, '-', ''), 24))}-cosmos'
    location: location
    tags: resourceTags
    databaseName: 'inventory'
    containerName: 'items'
    partitionKeyPath: '/category'
  }
}

// App Service Plan
module appServicePlan 'core/host/appserviceplan.bicep' = {
  name: 'appserviceplan'
  scope: resourceGroup
  params: {
    name: '${prefix}-plan'
    location: location
    tags: resourceTags
    sku: {
      name: 'Y1'
      tier: 'Dynamic'
      size: 'Y1'
      family: 'Y'
      capacity: 0
    }
  }
}



// Azure Functions
module functionApp 'core/host/functions.bicep' = {
  name: 'function'
  scope: resourceGroup
  params: {
    name: '${prefix}-function-app'
    location: location
    tags: union(resourceTags, { 'azd-service-name': 'api' })
    alwaysOn: false
    appSettings: {
      AzureWebJobsFeatureFlags: 'EnableWorkerIndexing'
      COSMOSDB_ENDPOINT: cosmosDatabase.outputs.cosmosEndpoint
      COSMOSDB_DATABASE: cosmosDatabase.outputs.cosmosDatabaseName
      COSMOSDB_CONTAINER: cosmosDatabase.outputs.cosmosContainerName
    }
    applicationInsightsName: monitoringResources.outputs.applicationInsightsName
    appServicePlanId: appServicePlan.outputs.id
    runtimeName: 'python'
    runtimeVersion: '3.10'
    storageAccountName: storageAccount.outputs.name
    skipRoleAssignment: true  // Add this to prevent role assignment errors
  }
}

module cosmosRoleAssignment 'core/database/cosmos-db-role-assignment.bicep' = {
  name: 'cosmos-role-assignment'
  scope: resourceGroup
  params: {
    principalId: functionApp.outputs.identityPrincipalId
    cosmosAccountName: cosmosDatabase.outputs.cosmosAccountName
    roleType: 'contributor' 
  }
}
// Function app diagnostics
module functionDiagnostics 'core/host/app-diagnostics.bicep' = {
  name: 'functionDiagnostics'
  scope: resourceGroup
  params: {
    appName: functionApp.outputs.name
    kind: 'functionapp'
    diagnosticWorkspaceId: monitoringResources.outputs.logAnalyticsWorkspaceId
  }
}

// Output
output azureLocation string = location
output azureTenantId string = tenant().tenantId
output resourceGroupId string = resourceGroup.id
output functionAppName string = functionApp.outputs.name
output functionAppEndpoint string = functionApp.outputs.uri
output cosmosDbEndpoint string = cosmosDatabase.outputs.cosmosEndpoint
