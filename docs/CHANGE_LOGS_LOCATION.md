# Change Logs Location in Property JSON

## Where Are Price, Status, and Listing Type Changes Stored?

The changes for **price**, **status**, and **listing_type** are stored in the **`change_logs`** array at the **ROOT level** of each property document.

## JSON Structure

```json
{
  "_id": "...",
  "property_id": "4095834546",
  "mls_id": "22056610",
  "status": "FOR_SALE",
  "listing_type": "for_sale",
  "financial": {
    "list_price": 164900,
    "original_list_price": 169900,
    "price_per_sqft": 74
  },

  // ... other property fields ...

  "change_logs": [  <-- HERE: Array at root level
    {
      // SIMPLE FORMAT: Single field change
      "field": "financial.list_price",
      "old_value": 169900,
      "new_value": 164900,
      "change_type": "decreased",
      "timestamp": "2025-01-15T00:00:00",
      "job_id": "scheduled_...",
      "created_at": "2025-01-15T10:00:00"
    },
    {
      // CONSOLIDATED FORMAT: Multiple fields changed on same date
      "field": null,  // null indicates consolidated entry
      "field_changes": {
        "financial.list_price": {
          "old_value": 164900,
          "new_value": 159900,
          "change_type": "decreased"
        },
        "status": {
          "old_value": "FOR_SALE",
          "new_value": "PENDING",
          "change_type": "modified"
        },
        "listing_type": {
          "old_value": "for_sale",
          "new_value": "for_sale",
          "change_type": "unchanged"
        }
      },
      "timestamp": "2025-02-01T00:00:00",
      "change_count": 3,
      "job_id": "scheduled_...",
      "created_at": "2025-02-01T10:00:00"
    }
  ],

  "enrichment": {
    // ... enrichment data ...
  },

  // Root-level flags (for quick filtering)
  "has_price_reduction": true,  <-- Calculated from change_logs
  "is_motivated_seller": false,
  "has_distress_signals": false,
  "last_enriched_at": "2025-11-01T11:13:57.835Z"
}
```

## Key Points

### 1. Location

- **Path**: `property.change_logs[]` (array at root level)
- **Not nested in** `enrichment` or any other object

### 2. Two Formats

#### Simple Format (Single Field)

Used when only one field changed on a specific date:

```json
{
  "field": "financial.list_price",
  "old_value": 169900,
  "new_value": 164900,
  "change_type": "decreased",
  "timestamp": "2025-01-15T00:00:00"
}
```

#### Consolidated Format (Multiple Fields)

Used when multiple fields changed on the same date:

```json
{
  "field": null, // null = consolidated entry
  "field_changes": {
    "financial.list_price": {
      "old_value": 164900,
      "new_value": 159900,
      "change_type": "decreased"
    },
    "status": {
      "old_value": "FOR_SALE",
      "new_value": "PENDING",
      "change_type": "modified"
    }
  },
  "timestamp": "2025-02-01T00:00:00",
  "change_count": 2
}
```

### 3. Tracked Fields

Only these fields are tracked in `change_logs`:

- **Price fields**:
  - `financial.list_price`
  - `financial.original_list_price`
  - `financial.price_per_sqft`
- **Status fields**:
  - `status`
  - `mls_status`
- **Listing type**:
  - `listing_type`

### 4. Quick Access Flags

For performance, these flags are also stored at the root level:

- `has_price_reduction` - `true` if any price reduction exists in `change_logs`
- `is_motivated_seller` - `true` if motivated seller score >= 40
- `has_distress_signals` - `true` if distress signals found

## Querying Change Logs

### Get all change_logs for a property

```javascript
db.properties.findOne({ property_id: "4095834546" }, { change_logs: 1 });
```

### Find properties with price reductions (Simple - fastest)

```javascript
// Option 1: Using the has_price_reduction flag (FASTEST - recommended)
db.properties.find({
  has_price_reduction: true,
});

// Option 2: Querying change_logs directly
db.properties.find({
  change_logs: {
    $elemMatch: {
      $or: [
        { field: "financial.list_price", change_type: "decreased" },
        { "field_changes.financial.list_price": { $ne: null } },
      ],
    },
  },
});
```

> **Note**: For more advanced queries (time windows, minimum amounts, etc.), see [PRICE_REDUCTION_QUERIES.md](./PRICE_REDUCTION_QUERIES.md)

### Using aggregation for dynamic filtering

```javascript
db.properties.find({
  $expr: {
    $gt: [
      {
        $size: {
          $filter: {
            input: "$change_logs",
            cond: {
              $and: [
                {
                  $or: [
                    { $eq: ["$$this.field", "financial.list_price"] },
                    {
                      "$$this.field_changes.financial.list_price": {
                        $ne: null,
                      },
                    },
                  ],
                },
                {
                  $or: [
                    { $eq: ["$$this.change_type", "decreased"] },
                    {
                      $eq: [
                        "$$this.field_changes.financial.list_price.change_type",
                        "decreased",
                      ],
                    },
                  ],
                },
                {
                  $gte: [
                    "$$this.timestamp",
                    new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
                  ],
                },
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

## Limits

- **TTL**: Changes older than 90 days are automatically removed
- **Max entries**: Only the 200 most recent entries are kept per property
- **Grouping**: Multiple changes on the same date are consolidated into one entry
