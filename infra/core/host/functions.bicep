metadata description = 'Creates an Azure Function in an existing Azure App Service plan.'

@description('Name of the Function App')
param name string

@description('Azure region where the Function App will be deployed')
param location string = resourceGroup().location

@description('Tags to apply to the Function App')
param tags object = {}

// Reference Properties
@description('Name of the Application Insights resource')
param applicationInsightsName string = ''

@description('Resource ID of the App Service Plan')
param appServicePlanId string

@description('Name of the Key Vault resource')
param keyVaultName string = ''

@description('Whether to enable managed identity')
param managedIdentity bool = true // Always enable managed identity

@description('Name of the Storage Account')
param storageAccountName string

// Runtime Properties
@description('Runtime name for the Function App')
@allowed([
  'dotnet', 'dotnetcore', 'dotnet-isolated', 'node', 'python', 'java', 'powershell', 'custom'
])
param runtimeName string

@description('Combined runtime name and version string')
param runtimeNameAndVersion string = '${runtimeName}|${runtimeVersion}'

@description('Runtime version for the Function App')
param runtimeVersion string

// Function Settings
@description('Azure Functions extension version')
@allowed([
  '~4', '~3', '~2', '~1'
])
param extensionVersion string = '~4'

// Microsoft.Web/sites Properties
@description('Kind of Function App')
param kind string = 'functionapp,linux'

// Microsoft.Web/sites/config
@description('List of allowed origins for CORS')
param allowedOrigins array = []

@description('Whether the Function App should be always on')
param alwaysOn bool = true

@description('Command line to execute at startup')
param appCommandLine string = ''

@description('App settings for the Function App')
@secure()
param appSettings object = {}

@description('Whether client affinity is enabled')
param clientAffinityEnabled bool = false

@description('Whether Oryx build should be enabled')
param enableOryxBuild bool = contains(kind, 'linux')

@description('Scale limit for the Function App')
param functionAppScaleLimit int = -1

@description('Linux FX version')
param linuxFxVersion string = runtimeNameAndVersion

@description('Minimum number of elastic instances')
param minimumElasticInstanceCount int = -1

@description('Number of workers')
param numberOfWorkers int = -1

@description('Whether to build during deployment')
param scmDoBuildDuringDeployment bool = true

@description('Whether to use 32-bit worker process')
param use32BitWorkerProcess bool = false

@description('Path for health check')
param healthCheckPath string = ''

@description('Flag to skip role assignment - useful when role already exists')
param skipRoleAssignment bool = false

module functionAppModule 'appservice.bicep' = {
  name: '${name}-functions'
  params: {
    name: name
    location: location
    tags: union(tags, {
      'azd-service-name': 'api'
    })
    allowedOrigins: allowedOrigins
    alwaysOn: alwaysOn
    appCommandLine: appCommandLine
    applicationInsightsName: applicationInsightsName
    appServicePlanId: appServicePlanId
    appSettings: union(appSettings, {
        AzureWebJobsStorage: 'DefaultEndpointsProtocol=https;AccountName=${functionStorageAccount.name};AccountKey=${functionStorageAccount.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}' // Keep connection string as fallback
        AzureWebJobsStorage__accountName: functionStorageAccount.name // Add managed identity configuration
        FUNCTIONS_EXTENSION_VERSION: extensionVersion
        FUNCTIONS_WORKER_RUNTIME: 'python'
        PYTHON_ENABLE_WORKER_EXTENSIONS: '1'
        PYTHON_ISOLATE_WORKER_DEPENDENCIES: '1'
        ENABLE_ORYX_BUILD: 'true'
        SCM_DO_BUILD_DURING_DEPLOYMENT: 'true'
        PYTHON_VERSION: '3.10' // Updated to match main.bicep
        PYTHON_ENABLE_GUNICORN_MULTIWORKERS: '0'
        WEBSITE_HTTPLOGGING_RETENTION_DAYS: '7'
      })
    clientAffinityEnabled: clientAffinityEnabled
    enableOryxBuild: enableOryxBuild
    functionAppScaleLimit: functionAppScaleLimit
    healthCheckPath: healthCheckPath
    keyVaultName: keyVaultName
    kind: kind
    linuxFxVersion: linuxFxVersion
    managedIdentity: managedIdentity
    minimumElasticInstanceCount: minimumElasticInstanceCount
    numberOfWorkers: numberOfWorkers
    runtimeName: runtimeName
    runtimeVersion: runtimeVersion
    runtimeNameAndVersion: runtimeNameAndVersion
    scmDoBuildDuringDeployment: scmDoBuildDuringDeployment
    use32BitWorkerProcess: use32BitWorkerProcess
  }
}

resource functionStorageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

// Define role names symbolically using well-known Azure built-in role names
// This encapsulates the knowledge of role definitions IDs in a single file
// These can be moved to a separate module if used across multiple files
var builtInRoleNames = {
  Owner: '8e3af657-a8ff-443c-a75c-2fe8c4bcb635'
  Contributor: 'b24988ac-6180-42a0-ab88-20f7382dd24c'
  Reader: 'acdd72a7-3385-48ef-bd42-f606fba81ae7'
  'User Access Administrator': '18d7d88d-d35e-4fb5-a5c3-7773c20a72d9'
  'Storage Blob Data Contributor': 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
  'Storage Blob Data Owner': 'b7e6dc6d-f1e8-4753-8033-0f276bb0955b'
  'Storage Blob Data Reader': '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
}

// Skip role assignment if the flag is set
resource roleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!skipRoleAssignment) {
  name: guid(resourceGroup().id, name, builtInRoleNames['Storage Blob Data Contributor'])
  scope: functionStorageAccount
  properties: {
    principalId: functionAppModule.outputs.identityPrincipalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', builtInRoleNames['Storage Blob Data Contributor'])
    principalType: 'ServicePrincipal'
  }
}

output identityPrincipalId string = functionAppModule.outputs.identityPrincipalId
output name string = functionAppModule.outputs.name
output uri string = functionAppModule.outputs.uri
