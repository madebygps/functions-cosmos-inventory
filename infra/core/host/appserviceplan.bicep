metadata description = 'Creates an Azure App Service plan.'

@description('Name of the App Service Plan')
param name string

@description('Azure region where the App Service Plan will be deployed')
param location string = resourceGroup().location

@description('Tags to apply to the App Service Plan')
param tags object = {}

@description('Kind of App Service Plan')
param kind string = ''

@description('Whether the App Service Plan is reserved')
param reserved bool = true

@description('SKU of the App Service Plan')
param sku object

resource appServicePlan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: name
  location: location
  tags: tags
  sku: sku
  kind: kind
  properties: {
    reserved: reserved
  }
}

output id string = appServicePlan.id
output name string = appServicePlan.name
