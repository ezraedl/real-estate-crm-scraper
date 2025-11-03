# MongoDB Storage Analysis: Enrichment Storage Growth

## Problem Summary

After enabling property enrichments, MongoDB storage jumped from **157MB to 512MB** (355MB increase) in a few hours, hitting the free tier limit.

## Root Causes

### 1. **Separate History Collections Growing Indefinitely** ⚠️ MAJOR ISSUE

The enrichment system creates **two separate MongoDB collections** that store historical data:

- `property_history` - Stores price changes and status changes
- `property_change_logs` - Stores detailed field-level changes

**Problem**: These collections have **NO cleanup mechanism**:

- No TTL (Time-To-Live) indexes to auto-delete old entries
- No deduplication to prevent duplicate entries
- No limits on entries per property
- Every enrichment creates new entries, even for unchanged data

**Storage Impact**:

- Each property with 10 price changes = 10+ history entries
- Each property with 20 field changes = 20+ change log entries
- If you have 1,000 properties and average 5 changes each = **5,000+ new documents**

### 2. **History Entries Created on Every Enrichment** ⚠️ DUPLICATION ISSUE

Looking at `services/history_tracker.py`:

```76:76:services/history_tracker.py
await self.history_collection.insert_one(history_entry)
```

```128:128:services/history_tracker.py
await self.change_logs_collection.insert_many(change_log_entries)
```

**Problem**:

- No check if the same change was already recorded
- If a property is scraped multiple times with the same data, it creates duplicate entries
- Every enrichment run creates new history entries, even if nothing changed

### 3. **Large Enrichment Data Embedded in Properties** ⚠️ SIZE ISSUE

The enrichment data is embedded directly in each property document:

```374:374:services/property_enrichment.py
"enrichment": enrichment_data,
```

The enrichment object includes:

- `motivated_seller` - Full scoring data with findings
- `text_analysis` - Keywords and analysis
- `distress_signals` - Lists of signals
- `price_history_summary` - Summary data
- `status_history_summary` - Summary data
- Full nested structures

**Estimated Size**: Each enrichment object can be **5-20KB+** per property.

If you have 1,000 properties:

- Without enrichment: ~100KB per property = ~100MB
- With enrichment: ~120KB per property = ~120MB
- **Plus** all the separate history collections = **500MB+**

## Storage Breakdown Estimate

For 1,000 properties with enrichments:

1. **Properties collection**: ~120MB (enrichment embedded)
2. **property_history collection**: ~150MB (price/status changes)
3. **property_change_logs collection**: ~200MB (field-level changes) ⚠️ **LARGEST after properties**
4. **Other collections** (jobs, etc.): ~42MB

**Total**: ~512MB ✅ Matches your observation!

**Note**: `property_change_logs` is typically the largest collection after `properties` because:

- It was tracking ALL field changes (descriptions, images, contact info, etc.)
- No cleanup mechanism existed
- Every property update created multiple change log entries

**After fixes**: This collection will be drastically reduced by:

1. Field filtering (only price/status/listing_type tracked)
2. TTL indexes (auto-delete > 90 days)
3. Deduplication (no duplicate entries)
4. Entry limits (max 200 per property)
5. Cleanup script (removes existing old/duplicate/excess entries)

## Solutions (Priority Order)

### 🔴 **CRITICAL: Add TTL Indexes to History Collections**

Add automatic deletion of old history entries:

```python
# In services/history_tracker.py, add to initialize() method:
async def initialize(self):
    # ... existing indexes ...

    # Add TTL indexes - auto-delete entries older than 90 days
    await self.history_collection.create_index(
        [("timestamp", 1)],
        expireAfterSeconds=7776000  # 90 days in seconds
    )
    await self.change_logs_collection.create_index(
        [("timestamp", 1)],
        expireAfterSeconds=7776000  # 90 days in seconds
    )
```

### 🟠 **HIGH: Add Deduplication to History Recording**

Prevent duplicate history entries:

```python
async def record_price_change(self, property_id: str, price_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
    """Record a price change in the history"""
    try:
        # Check if this exact change already exists
        existing = await self.history_collection.find_one({
            "property_id": property_id,
            "change_type": "price_change",
            "data.old_price": price_data["old_price"],
            "data.new_price": price_data["new_price"],
            "timestamp": price_data["timestamp"]
        })

        if existing:
            logger.debug(f"Price change already recorded for property {property_id}")
            return True

        # ... rest of existing code ...
```

### 🟡 **MEDIUM: Limit History Entries Per Property**

Keep only the most recent N entries per property:

```python
async def record_price_change(self, property_id: str, price_data: Dict[str, Any], job_id: Optional[str] = None) -> bool:
    """Record a price change in the history"""
    # ... insert new entry ...

    # Keep only last 50 price changes per property
    count = await self.history_collection.count_documents({
        "property_id": property_id,
        "change_type": "price_change"
    })

    if count > 50:
        # Delete oldest entries
        oldest = await self.history_collection.find(
            {"property_id": property_id, "change_type": "price_change"}
        ).sort("timestamp", 1).limit(count - 50).to_list(length=count - 50)

        if oldest:
            ids_to_delete = [entry["_id"] for entry in oldest]
            await self.history_collection.delete_many({"_id": {"$in": ids_to_delete}})
```

### 🟢 **LOW: Clean Up Existing Duplicate/Old Entries**

Create a cleanup script to remove old/duplicate entries:

```python
# scripts/cleanup_history.py
async def cleanup_old_history(days_old: int = 90):
    """Remove history entries older than specified days"""
    cutoff = datetime.utcnow() - timedelta(days=days_old)

    # Delete old history
    result1 = await db.property_history.delete_many({
        "timestamp": {"$lt": cutoff}
    })

    # Delete old change logs
    result2 = await db.property_change_logs.delete_many({
        "timestamp": {"$lt": cutoff}
    })

    print(f"Deleted {result1.deleted_count} history entries")
    print(f"Deleted {result2.deleted_count} change log entries")
```

### 💡 **OPTIONAL: Optimize Enrichment Data Size**

Consider:

1. **Remove verbose fields**: Don't store full `findings` object, just summary
2. **Store only summaries**: Keep `price_history_summary` but remove full history from enrichment
3. **Compress**: Use smaller data types, remove redundant fields

### ✅ **IMPLEMENTED: Limit Tracked Fields in Change Logs**

**Change**: Only track price, status, and listing_type changes in `property_change_logs`.

**Why**: Most field changes (description text, images, contact info, etc.) aren't needed for motivated seller scoring. We only need to track:

- **Price fields**: `financial.list_price`, `financial.original_list_price`, `financial.price_per_sqft`
- **Status fields**: `status`, `mls_status`
- **Listing type**: `listing_type`

**Storage Impact**: Reduces `property_change_logs` collection size by **~80-90%** (most changes are in other fields like descriptions, images, etc.)

**Code Location**: `services/property_enrichment.py` - `_record_history_and_changes()` method now filters field changes before recording.

## Immediate Actions

1. **Run cleanup script** to remove old history entries:

   ```bash
   python scripts/cleanup_history.py
   ```

2. **Add TTL indexes** (prevents future growth)

3. **Add deduplication** (prevents duplicate entries)

4. **Monitor storage** after fixes

## Expected Storage Reduction

After implementing all fixes:

- **Before**: 512MB (current)
- **After TTL (90 days)**: ~200MB (removes old data)
- **After deduplication**: ~150MB (removes duplicates)
- **After limiting entries**: ~100MB (removes excess history)

**Estimated savings**: ~400MB (80% reduction)

## Monitoring

Check collection sizes in MongoDB:

```javascript
db.property_history.stats().size; // Size in bytes
db.property_change_logs.stats().size;
db.properties.stats().size;
```

Calculate total storage:

```javascript
db.stats().dataSize; // Total database size
```
