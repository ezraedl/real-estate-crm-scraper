# MongoDB Queries for Properties with Price Reductions

## Quick Examples

### 1. Simple Query: All Properties with Any Price Reduction

**MongoDB Shell:**

```javascript
// Uses the has_price_reduction flag (fastest)
db.properties.find({
  has_price_reduction: true,
});
```

**MongoDB Compass / Query Builder:**

```json
{
  "has_price_reduction": true
}
```

**Python (Motor/PyMongo):**

```python
properties = await db.properties.find({
    "has_price_reduction": True
}).to_list(length=None)
```

---

### 2. Query: Properties with Price Reductions in Last 30 Days

**MongoDB Shell:**

```javascript
// Using aggregation to filter by time window
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
                {
                  $or: [
                    { $eq: ["$$log.field", "financial.list_price"] },
                    { $ne: ["$$log.field_changes.financial.list_price", null] },
                  ],
                },
                {
                  $or: [
                    { $eq: ["$$log.change_type", "decreased"] },
                    {
                      $eq: [
                        "$$log.field_changes.financial.list_price.change_type",
                        "decreased",
                      ],
                    },
                  ],
                },
                { $gte: ["$$log.timestamp", cutoffDate] },
              ],
            },
          },
        },
      },
      0,
    ],
  },
});
```

**Python (Motor):**

```python
from datetime import datetime, timedelta

cutoff_date = datetime.utcnow() - timedelta(days=30)

properties = await db.properties.find({
    "has_price_reduction": True,
    "$expr": {
        "$gt": [
            {
                "$size": {
                    "$filter": {
                        "input": "$change_logs",
                        "as": "log",
                        "cond": {
                            "$and": [
                                        {
                                            "$or": [
                                                {"$eq": ["$$log.field", "financial.list_price"]},
                                                {"$ne": [{"$ifNull": ["$$log.field_changes.financial.list_price", null]}, null]}
                                            ]
                                        },
                                {
                                    "$or": [
                                        {"$eq": ["$$log.change_type", "decreased"]},
                                        {"$eq": ["$$log.field_changes.financial.list_price.change_type", "decreased"]}
                                    ]
                                },
                                {"$gte": ["$$log.timestamp", cutoff_date]}
                            ]
                        }
                    }
                }
            },
            0
        ]
    }
}).to_list(length=None)
```

---

### 3. Query: Properties with Price Reduction ≥ $1,000

**MongoDB Shell:**

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
                    {
                      $or: [
                        { $eq: ["$$log.field", "financial.list_price"] },
                        {
                          $ne: [
                            {
                              $ifNull: [
                                "$$log.field_changes.financial.list_price",
                                null,
                              ],
                            },
                            null,
                          ],
                        },
                      ],
                    },
                    {
                      $or: [
                        { $eq: ["$$log.change_type", "decreased"] },
                        {
                          $eq: [
                            "$$log.field_changes.financial.list_price.change_type",
                            "decreased",
                          ],
                        },
                      ],
                    },
                  ],
                },
              },
            },
            as: "reduction",
            in: {
              $subtract: [
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.old_value",
                        "$$reduction.field_changes.financial.list_price.old_value",
                      ],
                    },
                    0,
                  ],
                },
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.new_value",
                        "$$reduction.field_changes.financial.list_price.new_value",
                      ],
                    },
                    0,
                  ],
                },
              ],
            },
          },
        },
      },
      1000, // Minimum reduction amount
    ],
  },
});
```

**Python (Motor):**

```python
min_reduction = 1000

properties = await db.properties.find({
    "has_price_reduction": True,
    "$expr": {
        "$gte": [
            {
                "$sum": {
                    "$map": {
                        "input": {
                            "$filter": {
                                "input": "$change_logs",
                                "as": "log",
                                "cond": {
                                    "$and": [
                                        {
                                            "$or": [
                                                {"$eq": ["$$log.field", "financial.list_price"]},
                                                {"$ne": ["$$log.field_changes.financial.list_price", None]}
                                            ]
                                        },
                                        {
                                            "$or": [
                                                {"$eq": ["$$log.change_type", "decreased"]},
                                                {"$eq": ["$$log.field_changes.financial.list_price.change_type", "decreased"]}
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        "as": "reduction",
                        "in": {
                            "$subtract": [
                                {
                                    "$ifNull": [
                                        {
                                            "$cond": [
                                                {"$ne": ["$$reduction.field", None]},
                                                "$$reduction.old_value",
                                                "$$reduction.field_changes.financial.list_price.old_value"
                                            ]
                                        },
                                        0
                                    ]
                                },
                                {
                                    "$ifNull": [
                                        {
                                            "$cond": [
                                                {"$ne": ["$$reduction.field", None]},
                                                "$$reduction.new_value",
                                                "$$reduction.field_changes.financial.list_price.new_value"
                                            ]
                                        },
                                        0
                                    ]
                                }
                            ]
                        }
                    }
                }
            },
            min_reduction
        ]
    }
}).to_list(length=None)
```

---

### 4. Query: Price Reduction ≥ $1,000 in Last 30 Days

**MongoDB Shell:**

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
                    {
                      $or: [
                        { $eq: ["$$log.field", "financial.list_price"] },
                        {
                          $ne: [
                            {
                              $ifNull: [
                                "$$log.field_changes.financial.list_price",
                                null,
                              ],
                            },
                            null,
                          ],
                        },
                      ],
                    },
                    {
                      $or: [
                        { $eq: ["$$log.change_type", "decreased"] },
                        {
                          $eq: [
                            "$$log.field_changes.financial.list_price.change_type",
                            "decreased",
                          ],
                        },
                      ],
                    },
                    { $gte: ["$$log.timestamp", cutoffDate] },
                  ],
                },
              },
            },
            as: "reduction",
            in: {
              $subtract: [
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.old_value",
                        "$$reduction.field_changes.financial.list_price.old_value",
                      ],
                    },
                    0,
                  ],
                },
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.new_value",
                        "$$reduction.field_changes.financial.list_price.new_value",
                      ],
                    },
                    0,
                  ],
                },
              ],
            },
          },
        },
      },
      minReduction,
    ],
  },
});
```

---

### 5. Aggregation Pipeline: Get Properties with Price Reduction Details

**MongoDB Shell:**

```javascript
db.properties.aggregate([
  {
    $match: {
      has_price_reduction: true,
    },
  },
  {
    $project: {
      property_id: 1,
      mls_id: 1,
      address: 1,
      "financial.list_price": 1,
      price_reductions: {
        $filter: {
          input: "$change_logs",
          as: "log",
          cond: {
            $and: [
              {
                $or: [
                  { $eq: ["$$log.field", "financial.list_price"] },
                  {
                    $exists: ["$$log.field_changes.financial.list_price", true],
                  },
                ],
              },
              {
                $or: [
                  { $eq: ["$$log.change_type", "decreased"] },
                  {
                    $eq: [
                      "$$log.field_changes.financial.list_price.change_type",
                      "decreased",
                    ],
                  },
                ],
              },
            ],
          },
        },
      },
      total_reduction: {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$change_logs",
                as: "log",
                cond: {
                  $and: [
                    {
                      $or: [
                        { $eq: ["$$log.field", "financial.list_price"] },
                        {
                          $ne: [
                            {
                              $ifNull: [
                                "$$log.field_changes.financial.list_price",
                                null,
                              ],
                            },
                            null,
                          ],
                        },
                      ],
                    },
                    {
                      $or: [
                        { $eq: ["$$log.change_type", "decreased"] },
                        {
                          $eq: [
                            "$$log.field_changes.financial.list_price.change_type",
                            "decreased",
                          ],
                        },
                      ],
                    },
                  ],
                },
              },
            },
            as: "reduction",
            in: {
              $subtract: [
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.old_value",
                        "$$reduction.field_changes.financial.list_price.old_value",
                      ],
                    },
                    0,
                  ],
                },
                {
                  $ifNull: [
                    {
                      $cond: [
                        { $ne: ["$$reduction.field", null] },
                        "$$reduction.new_value",
                        "$$reduction.field_changes.financial.list_price.new_value",
                      ],
                    },
                    0,
                  ],
                },
              ],
            },
          },
        },
      },
    },
  },
  {
    $match: {
      total_reduction: { $gte: 1000 }, // Filter by minimum reduction
    },
  },
  {
    $sort: {
      total_reduction: -1, // Sort by highest reduction first
    },
  },
]);
```

---

## Simplified Queries (Simple Format Only)

If you know your data only uses the simple format (not consolidated), you can use simpler queries:

### Simple Format: Any Price Reduction

```javascript
db.properties.find({
  change_logs: {
    $elemMatch: {
      field: "financial.list_price",
      change_type: "decreased",
    },
  },
});
```

### Simple Format: Price Reduction in Last 30 Days

```javascript
const cutoffDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);

db.properties.find({
  change_logs: {
    $elemMatch: {
      field: "financial.list_price",
      change_type: "decreased",
      timestamp: { $gte: cutoffDate },
    },
  },
});
```

---

## Performance Tips

1. **Always use `has_price_reduction` flag first** - This is indexed and very fast
2. **Limit time windows** - Shorter time windows = faster queries
3. **Use projection** - Only select fields you need: `{property_id: 1, address: 1, change_logs: 1}`
4. **Add indexes** - The following indexes are already created:
   - `change_logs.field`
   - `change_logs.change_type`
   - `change_logs.timestamp`
   - `has_price_reduction`

---

## Backend API Usage

The backend uses these queries via the `PriceReductionFilter` class:

**Request:**

```http
GET /api/properties?priceReductionMin=1000&priceReductionTimeWindow=30
```

This automatically:

1. Filters by `has_price_reduction: true` (quick pre-filter)
2. Calculates total reduction from `change_logs` array
3. Filters by minimum reduction amount
4. Filters by time window if specified
