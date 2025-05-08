az cosmosdb sql role assignment create \
  --account-name accountname \
  --resource-group rg-name \
  --scope "/" \
  --principal-id $(az ad signed-in-user show --query id -o tsv) \
  --role-definition-id "00000000-0000-0000-0000-000000000002"