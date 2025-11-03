# Simple Query: All Properties with Price Reductions

## Recommended Query (Simplest & Fastest)

```javascript
// Option 1: Using the has_price_reduction flag (FASTEST - matches backend)
db.properties.find({
  has_price_reduction: true
})
```

## Query with Time Window (Last 30 Days)

```javascript
const cutoffDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

db.properties.find({
  has_price_reduction: true,
  $expr: {
    $gt: [
      {
        $size: {
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
        }
      },
      0
    ]
  }
})
```

## Query with Minimum Reduction Amount (≥ $1,000)

```javascript
db.properties.find({
  has_price_reduction: true,
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
                    { $eq: ["$$log.change_type", "decreased"] }
                  ]
                }
              }
            },
            as: "reduction",
            in: {
              $subtract: [
                { $ifNull: ["$$reduction.old_value", 0] },
                { $ifNull: ["$$reduction.new_value", 0] }
              ]
            }
          }
        }
      },
      1000  // Minimum $1,000 reduction
    ]
  }
})
```

## Query with Both Minimum Amount and Time Window

```javascript
const minReduction = 1000;
const daysBack = 30;
const cutoffDate = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);

db.properties.find({
  has_price_reduction: true,
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
                    { $gte: ["$$log.timestamp", cutoffDate] }
                  ]
                }
              }
            },
            as: "reduction",
            in: {
              $subtract: [
                { $ifNull: ["$$reduction.old_value", 0] },
                { $ifNull: ["$$reduction.new_value", 0] }
              ]
            }
          }
        }
      },
      minReduction
    ]
  }
})
```

## Note

These queries match the backend implementation and only check the **simple format** entries in `change_logs` (where `field` is not null). 

If you need to also check consolidated format entries (where multiple fields changed on the same date), see [PRICE_REDUCTION_QUERIES.md](./PRICE_REDUCTION_QUERIES.md) for more complex examples.

However, since most recent data uses the simple format after consolidation, these queries should work for most use cases.

