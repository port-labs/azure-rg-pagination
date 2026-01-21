#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run the query
python standalone_resource_graph.py -s --check-duplicates --query '
resources
| where tags !has '\''hosted_on_behalf_of'\'' and type in (
    "microsoft.compute/virtualmachines",
    "microsoft.network/loadbalancers",
    "microsoft.cache/redis",
    "microsoft.dbformysql/flexibleservers",
    "microsoft.cache/redisenterprise",
    "microsoft.network/bastionhosts",
    "microsoft.sql/servers",
    "microsoft.network/azurefirewalls",
    "microsoft.eventhub/clusters",
    "microsoft.dbforpostgresql/servergroupsv2",
    "microsoft.storage/storageaccounts",
    "microsoft.network/publicipaddresses",
    "microsoft.kusto/clusters",
    "microsoft.dbforpostgresql/flexibleservers",
    "microsoft.containerservice/managedclusters",
    "microsoft.dbforpostgresql/servers"
)
| join kind=leftouter (
    resourcecontainers
    | where type =~ '\''microsoft.resources/subscriptions'\''
    | mv-expand managementGroupParent = properties.managementGroupAncestorsChain
    | where managementGroupParent.name =~ '\''yaeli'\''
    | project name, subscriptionId, isHostedCompute = true
) on subscriptionId
| where isHostedCompute != true
| project id, type, name, location, tags, subscriptionId, resourceGroup
| order by id asc
| extend id=tolower(id), type=tolower(type), name=tolower(name), resourceGroup=tolower(resourceGroup), subscriptionId=tolower(subscriptionId), resourceGroupId = tolower(strcat("/subscriptions/", subscriptionId, "/resourcegroups/", resourceGroup))
' "$@"
