# Dynamic Price Reduction Filtering (No Pre-calculated Summaries)

## The Problem with Current Approach

**Current State:**
- `enrichment.price_history_summary` stores pre-calculated values:
  - `total_reductions` - Total amount reduced
  - `days_since_last_reduction` - Days since last price cut
  - `number_of_reductions` - Count of reductions
  
- Filter uses fixed values:
  ```javascript
  query['enrichment.price_history_summary.total_reductions'] = { $gte: minReduction }
  query['enrichment.price_history_summary.days_since_last_reduction'] = { $lte: days }
  ```

**Problems:**
1. **Fixed time window**: Summary calculates from ALL time, filter can't use custom time windows
2. **Duplication**: Same data stored in `change_logs` AND summary
3. **Stale data**: Summary needs recalculation when `change_logs` updates
4. **Inflexible**: Can't query "reductions in last 7 days" vs "reductions in last 30 days" dynamically

## The Solution: Calculate On-Demand from `change_logs`

**Instead of storing summaries, calculate dynamically using MongoDB aggregation:**

### Option 1: MongoDB Aggregation Pipeline (Recommended)

Filter properties using `$expr` and `$filter` to calculate reductions on-the-fly:

```javascript
// Filter: properties with price reductions >= $1000 in last 30 days

const cutoffDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000); // 30 days ago
const minReduction = 1000;

const query = {
  $expr: {
    $gte: [
      {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$change_logs",
                as: "log",
                cond: {
                  $and: [
                    { $eq: ["$$log.field", "financial.list_price"] },
                    { $eq: ["$$log.change_type", "decreased"] },
                    { $gte: ["$$log.timestamp", cutoffDate] },
                    {
                      $gte: [
                        { $subtract: ["$$log.old_value", "$$log.new_value"] },
                        minReduction
                      ]
                    }
                  ]
                }
              }
            }
          },
          as: "reduction",
          in: { $subtract: ["$$reduction.old_value", "$$reduction.new_value"] }
        }
      },
      minReduction
    ]
  }
};
```

### Option 2: Simplified Query (Two-Step)

1. **Filter by `has_price_reduction` flag** (quick index check)
2. **Then calculate totals** from `change_logs` array in application code

```javascript
// Step 1: Quick filter using flag
const candidates = db.properties.find({ 
  has_price_reduction: true,
  "change_logs": { $exists: true, $ne: [] }
});

// Step 2: Filter and calculate in app
candidates.forEach(prop => {
  const reductions = prop.change_logs
    .filter(log => 
      log.field === "financial.list_price" &&
      log.change_type === "decreased" &&
      new Date(log.timestamp) >= cutoffDate
    )
    .map(log => log.old_value - log.new_value);
  
  const total = reductions.reduce((a, b) => a + b, 0);
  if (total >= minReduction) {
    // Property matches filter
  }
});
```

### Option 3: MongoDB Aggregation Pipeline (Full Filter)

For backend filter, use aggregation pipeline:

```javascript
// PriceReductionFilter.ts - Dynamic calculation

buildQuery(params: FilterParams): any {
  const query: any = {};
  
  const minReduction = this.toNumber(params.priceReductionMin);
  const daysBack = this.toNumber(params.priceReductionTimeWindow) || 30;
  
  if (minReduction !== null || daysBack !== null) {
    const cutoffDate = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
    
    query.$expr = {
      $gte: [
        {
          $sum: {
            $map: {
              input: {
                $filter: {
                  input: "$change_logs",
                  as: "log",
                  cond: {
                    $and: [
                      { $eq: ["$$log.field", "financial.list_price"] },
                      { $eq: ["$$log.change_type", "decreased"] },
                      { $gte: ["$$log.timestamp", cutoffDate] }
                    ]
                  }
                }
              },
              as: "reduction",
              in: { $subtract: ["$$reduction.old_value", "$$reduction.new_value"] }
            }
          }
        },
        minReduction || 0
      ]
    };
  }
  
  return query;
}
```

## Benefits

1. ✅ **No pre-calculated summaries** - Single source of truth (`change_logs`)
2. ✅ **Flexible time windows** - Calculate for any time period per query
3. ✅ **Always up-to-date** - No stale summaries, calculated fresh from `change_logs`
4. ✅ **No duplication** - Remove `price_history_summary` entirely
5. ✅ **Simpler architecture** - Less code to maintain, less data to sync

## Performance Considerations

**Concerns:**
- Aggregating arrays might be slower than indexed field lookups

**Mitigations:**
1. **Keep `has_price_reduction` flag** - Quick pre-filter before aggregation
2. **Index `change_logs` array** - Already have indexes on `change_logs.field`, `change_logs.timestamp`
3. **MongoDB optimizes** - `$expr` with indexed arrays is reasonably fast
4. **Most queries filtered** - Most properties won't have price reductions, flag filters them out first

## Implementation Plan

1. **Update `PriceReductionFilter`** to use `$expr` aggregation instead of summary fields
2. **Remove `price_history_summary`** from enrichment data
3. **Remove `_build_price_history_summary()`** method (or keep for motivated seller scoring if needed)
4. **Keep `has_price_reduction` flag** - Simple boolean for quick filtering
5. **Test performance** - Verify aggregation is fast enough (should be with indexed arrays)

## Storage Savings

**Remove:**
- `enrichment.price_history_summary` object (~200 bytes per property with reductions)

**Keep:**
- `has_price_reduction` boolean flag (1 byte per property)

**Savings:** ~200 bytes per property with price reductions (~20KB for 100 properties)

Plus removes complexity and potential data inconsistency!

