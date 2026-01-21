# Azure Resource Graph Pagination Issue Demo

This repository demonstrates a **pagination inconsistency issue** in Azure Resource Graph API when queries lack explicit ordering.

## The Problem

When querying Azure Resource Graph without an `ORDER BY` clause, the API returns **inconsistent results across pages**. This causes:

- **Duplicate resources** - the same resource appears multiple times
- **Missing resources** - some resources are skipped (10 duplicates = 10 missing resources)
- **Data integrity issues** - you receive 1062 records but only 1052 are unique

This happens because Azure Resource Graph doesn't guarantee a stable sort order for paginated results. As you iterate through pages using `$skipToken`, the underlying data order shifts, causing resources to be skipped or duplicated.

## The Solution

Adding `| order by id asc` to your query ensures consistent pagination results.

---

## Installation

### Prerequisites

- Python 3.11+
- Azure Service Principal with Reader access to your subscriptions

### Setup

1. **Clone and enter the directory:**
   ```bash
   cd azure-rg-pagination
   ```

2. **Run the install script:**
   ```bash
   ./install.sh
   ```

3. **Configure your Azure credentials** by editing the `.env` file:
   ```env
   AZURE_TENANT_ID=your-tenant-id
   AZURE_CLIENT_ID=your-client-id
   AZURE_CLIENT_SECRET=your-client-secret
   ```

---

## Usage

### `./run.sh` - WITHOUT ordering (demonstrates the bug)

```bash
./run.sh
```

**Actual output:**
```
Total resources processed: 1062
Unique resource IDs: 1052
Duplicate IDs found: 10
Total duplicate occurrences: 10
```

⚠️ **10 resources appear twice, meaning 10 other resources are missing!**

### `./run_ordered.sh` - WITH ordering (correct behavior)

```bash
./run_ordered.sh
```

**Actual output:**
```
Total resources processed: 1062
Unique resource IDs: 1062
Duplicate IDs found: 0
Total duplicate occurrences: 0

No duplicates found!
```

✅ **All 1062 resources are unique - complete data.**

---

## Results Comparison

| Metric | `run.sh` (unordered) | `run_ordered.sh` (ordered) |
|--------|----------------------|----------------------------|
| Total processed | 1062 | 1062 |
| Unique resources | **1052** | **1062** |
| Duplicates | **10** | **0** |
| Missing resources | **10** | **0** |
| Data complete? | ❌ No | ✅ Yes |

---

## Example Duplicate Detection Output

When running `./run.sh`, the tool detects duplicates appearing in later pages:

```
DUPLICATE DETECTED: id='.../storageaccounts/xxxxxx'
  First seen: [page=8, position=4, global_position=704]
  Duplicate at: [page=11, position=53, global_position=1053]

DUPLICATE DETECTED: id='.../storageaccounts/yyyyyy'
  First seen: [page=1, position=3, global_position=3]
  Duplicate at: [page=11, position=54, global_position=1054]
```

Notice how resources from early pages (page 1, 8) reappear on page 11. This means other resources that should have been on page 11 are missing entirely.

---

## Technical Details

### The Query Difference

**`run.sh`** (problematic - no ordering):
```kusto
resources
| where ...
| project id, type, name, location, tags, subscriptionId, resourceGroup
| extend ...
```

**`run_ordered.sh`** (fixed - with ordering):
```kusto
resources
| where ...
| project id, type, name, location, tags, subscriptionId, resourceGroup
| order by id asc    # <-- This line fixes the issue
| extend ...
```

### Why This Happens

Azure Resource Graph uses cursor-based pagination (`$skipToken`). Without a deterministic sort order:

1. Page 1 returns resources in arbitrary order
2. Between page requests, the underlying index may reorder data
3. The `$skipToken` cursor position shifts relative to the data
4. Resources that were "ahead" of the cursor move "behind" it (duplicates)
5. Resources that were "behind" the cursor move "ahead" of it (missing)

### Recommendation

**Always include an `ORDER BY` clause** in Resource Graph queries that may return paginated results (more than 1000 resources).

---

## Script Options

Both scripts support additional flags:

```bash
# Save results to JSON file
./run.sh -o results.json

# Show verbose logging
./run.sh -v

# Show only summary (default behavior)
./run.sh -s
```

---

## References

- [Azure Resource Graph pagination](https://learn.microsoft.com/en-us/azure/governance/resource-graph/concepts/work-with-data#paging-results)
- [Azure Resource Graph query language](https://learn.microsoft.com/en-us/azure/governance/resource-graph/concepts/query-language)
