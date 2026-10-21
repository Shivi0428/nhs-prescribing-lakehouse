// Azure footprint for the lakehouse: ADLS Gen2 for the Delta tables and
// a Databricks workspace to run the same PySpark code.
//   az group create -n rg-rxlake -l uksouth
//   az deployment group create -g rg-rxlake -f infra/bicep/main.bicep -p prefix=rxlake

targetScope = 'resourceGroup'

@description('Short lowercase name used to build resource names')
@minLength(3)
@maxLength(11)
param prefix string

param location string = resourceGroup().location

var suffix = uniqueString(resourceGroup().id)
var storageName = toLower('${prefix}${suffix}')

resource lake 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: take(storageName, 24)
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    isHnsEnabled: true               // hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: lake
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [
  for name in ['landing', 'lake']: {
    parent: blob
    name: name
  }
]

resource dbx 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: '${prefix}-dbx-${take(suffix, 6)}'
  location: location
  sku: { name: 'standard' }
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      '${prefix}-dbx-managed-${take(suffix, 6)}'
    )
  }
}

output storageAccount string = lake.name
output lakeRoot string = 'abfss://lake@${lake.name}.dfs.${environment().suffixes.storage}'
output landingRoot string = 'abfss://landing@${lake.name}.dfs.${environment().suffixes.storage}'
output databricksUrl string = 'https://${dbx.properties.workspaceUrl}'
