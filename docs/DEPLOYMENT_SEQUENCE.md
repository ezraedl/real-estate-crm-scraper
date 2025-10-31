# Deployment Sequence for Storage Fixes

## ✅ Code Changes Made

The following changes have been implemented to fix storage bloat:

### 1. **services/history_tracker.py** - Fixed

- ✅ Added TTL indexes (auto-delete entries > 90 days)
- ✅ Added deduplication checks (prevents duplicate entries)
- ✅ Added entry limits per property (100 price changes, 50 status changes, 200 change logs)

### 2. **services/property_enrichment.py** - Fixed

- ✅ Limited change log tracking to only price, status, and listing_type fields
- ✅ Reduces storage by not tracking every field change (descriptions, images, etc.)

### 3. **scripts/cleanup_history.py** - New cleanup script

- ✅ Script to clean up existing old/duplicate entries

## 🚀 Deployment Sequence (IMPORTANT!)

**Yes, you should push to production FIRST** before running cleanup! Here's why:

1. **Code changes create TTL indexes** - These need to exist before cleanup
2. **Deduplication logic** - Prevents new duplicates while cleaning old ones
3. **Entry limits** - Prevents future growth while cleaning excess

### Step-by-Step Deployment

#### Step 1: Commit and Push Code Changes

```bash
git add services/history_tracker.py services/property_enrichment.py scripts/cleanup_history.py docs/
git commit -m "Fix: Add storage optimizations for enrichment history collections

- Add TTL indexes (90 days) for automatic cleanup
- Add deduplication to prevent duplicate history entries
- Add entry limits per property (100 price/50 status/200 logs)
- Limit change logs to only price/status/listing_type fields
- Add cleanup script for immediate storage reduction"
git push origin bug-fix  # or your branch
```

#### Step 2: Deploy to Production

- Deploy the code changes to your production environment
- **Restart the scraper service** so the new code runs and creates TTL indexes

#### Step 3: Verify TTL Indexes Were Created

After restart, check logs for:

```
Created TTL index on property_history.timestamp (90 days)
Created TTL index on property_change_logs.timestamp (90 days)
```

Or verify in MongoDB:

```javascript
// Check if TTL indexes exist
db.property_history.getIndexes();
db.property_change_logs.getIndexes();
// Should see index with "expireAfterSeconds": 7776000
```

#### Step 4: Run Cleanup Script (One-Time)

Once the code is deployed and TTL indexes are created, run the cleanup:

```bash
# Preview what will be cleaned (dry-run)
python scripts/cleanup_history.py --dry-run

# Actually clean up
python scripts/cleanup_history.py
```

The cleanup will:

- Delete entries older than 90 days
- Remove duplicate entries
- Limit entries per property to configured limits

#### Step 5: Monitor Storage

After cleanup, monitor your MongoDB storage:

- Storage should drop significantly (expect ~200-300MB reduction)
- TTL indexes will automatically clean up old entries going forward (every 60 seconds)

## 🔍 What Gets Fixed

### Immediate (Cleanup Script)

- ✅ Removes old entries (> 90 days)
- ✅ Removes duplicate entries
- ✅ Enforces entry limits per property

### Ongoing (Code Changes)

- ✅ **TTL Indexes**: Automatically delete entries older than 90 days
- ✅ **Deduplication**: Skip duplicate entries on insert
- ✅ **Entry Limits**: Keep only last N entries per property

## 📊 Expected Results

After deployment + cleanup:

| Metric                   | Before    | After  | Reduction |
| ------------------------ | --------- | ------ | --------- |
| **Total Storage**        | 512MB     | ~150MB | ~70%      |
| **property_history**     | ~150MB    | ~50MB  | ~67%      |
| **property_change_logs** | ~200MB    | ~50MB  | ~75%      |
| **Future Growth**        | Unbounded | Capped | 100%      |

## ⚠️ Important Notes

1. **TTL cleanup is gradual** - MongoDB deletes expired documents every ~60 seconds
2. **Entry limits are enforced immediately** - Old entries beyond limits are deleted on next insert
3. **Deduplication prevents new duplicates** - Existing duplicates need cleanup script
4. **Monitor for 24-48 hours** after deployment to ensure storage stays stable

## 🧪 Testing Locally First (Optional)

If you want to test locally first:

```bash
# 1. Test TTL indexes creation
python -c "
import asyncio
from database import Database
async def test():
    db = Database()
    await db.connect()
    await db.enrichment_pipeline.initialize()
    print('TTL indexes should be created')
asyncio.run(test())
"

# 2. Test cleanup script (dry-run)
python scripts/cleanup_history.py --dry-run
```

## ✅ Verification Checklist

After deployment, verify:

- [ ] Code deployed successfully
- [ ] Service restarted
- [ ] TTL indexes created (check logs or MongoDB)
- [ ] Cleanup script ran successfully
- [ ] Storage reduced significantly
- [ ] No new duplicates being created (monitor for 24h)
- [ ] Storage growth rate is now controlled

## 🔄 Rollback Plan

If something goes wrong:

1. **Code rollback**: Revert git commit and redeploy
2. **TTL index removal** (if needed):
   ```javascript
   db.property_history.dropIndex("timestamp_1");
   db.property_change_logs.dropIndex("timestamp_1");
   ```
3. **Storage will stabilize** - The cleanup already ran, so immediate rollback won't cause more bloat
