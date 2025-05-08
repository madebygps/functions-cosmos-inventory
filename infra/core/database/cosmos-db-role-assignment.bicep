@description('Principal ID to assign the role to')
param principalId string

@description('Name of the Cosmos DB account')
param cosmosAccountName string

// Define the built-in role IDs for Cosmos DB
var cosmosDbDataContributor = '00000000-0000-0000-0000-000000000002'
var cosmosDbDataReader = '00000000-0000-0000-0000-000000000001'

// Using a simpler approach for role assignment
@description('Role to assign: contributor or reader')
@allowed([
  'contributor'
  'reader'
])
param roleType string = 'contributor'

var roleDefinitionId = roleType == 'contributor' ? cosmosDbDataContributor : cosmosDbDataReader

resource cosmosDbAccount 'Microsoft.DocumentDB/databaseAccounts@2023-04-15' existing = {
  name: cosmosAccountName
}

// Format the role definition ID with the proper path structure
var fullRoleDefinitionId = '${cosmosDbAccount.id}/sqlRoleDefinitions/${roleDefinitionId}'

resource roleAssignment 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2023-04-15' = {
  name: guid(cosmosDbAccount.id, principalId, roleDefinitionId)
  parent: cosmosDbAccount
  properties: {
    principalId: principalId
    roleDefinitionId: fullRoleDefinitionId
    scope: cosmosDbAccount.id
  }
}

output roleAssignmentId string = roleAssignment.id
