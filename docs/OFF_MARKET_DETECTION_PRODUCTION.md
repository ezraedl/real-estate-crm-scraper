# Off-Market Detection - Production Guide

## What Happens Now

After deployment, the off-market detection feature is **automatically active** for all scheduled jobs that scrape `for_sale` or `pending` properties.

## How It Works

1. **On Each Scrape Run:**
   - Properties are scraped and saved with `scheduled_job_id` and `last_scraped` timestamp
   - After each location is scraped, the system checks for missing properties

2. **Missing Property Detection:**
   - Queries properties with the same `scheduled_job_id`
   - Filters to properties where `last_scraped < job_start_time` (weren't scraped in this run)
   - Only checks properties with `listing_type` in `['for_sale', 'pending']`

3. **Off-Market Check:**
   - Queries each missing property directly by address using HomeHarvest
   - Checks if property has `status='OFF_MARKET'` or off-market `mls_status`
   - Updates property status to `OFF_MARKET` if detected
   - Records status change in `change_logs`

## Monitoring

### 1. Check Job Logs

Look for `[OFF-MARKET]` entries in job progress logs:

```python
# Via API or database
GET /jobs/{job_id}

# Look for entries like:
{
  "event": "off_market_check",
  "scheduled_job_id": "your_job_id",
  "total_missing": 5,
  "checked": 5,
  "off_market_found": 2,
  "errors": 0
}
```

### 2. Check Database

**Properties with OFF_MARKET status:**
```javascript
db.properties.find({"status": "OFF_MARKET"}).count()
```

**Properties tagged with scheduled_job_id:**
```javascript
db.properties.find({"scheduled_job_id": {"$exists": true}}).count()
```

**Properties with last_scraped timestamp:**
```javascript
db.properties.find({"last_scraped": {"$exists": true}}).count()
```

**Recent off-market detections (from change_logs):**
```javascript
db.properties.find({
  "change_logs": {
    "$elemMatch": {
      "field": "status",
      "new_value": "OFF_MARKET",
      "timestamp": {"$gte": ISODate("2025-11-25T00:00:00Z")}
    }
  }
})
```

### 3. Check Scheduled Job Run History

```python
# Via API
GET /scheduled-jobs/{scheduled_job_id}

# Check last_run_at and next_run_at
# Properties should be getting last_scraped updated on each run
```

## What to Expect

### First Scrape Run After Deployment:
- ✅ All properties get `scheduled_job_id` and `last_scraped` set
- ✅ No off-market check (all properties were found)

### Second Scrape Run:
- ✅ Properties found → `last_scraped` updated to current time
- ✅ Properties NOT found → keep old `last_scraped` timestamp
- ✅ Off-market detection runs automatically
- ✅ Missing properties are checked by address
- ✅ Off-market properties are updated

### Typical Behavior:
- Most properties will still be active (not off-market)
- Some properties may be detected as off-market
- Properties are checked in batches of 50
- All missing properties are checked (not just first 50)

## Verification Steps

### Step 1: Verify Properties Are Being Tagged

After first scrape run:
```javascript
// Check if properties have scheduled_job_id
db.properties.aggregate([
  {"$match": {"scheduled_job_id": {"$exists": true}}},
  {"$group": {"_id": "$scheduled_job_id", "count": {"$sum": 1}}}
])
```

### Step 2: Verify Off-Market Detection Is Running

Check job logs for `[OFF-MARKET]` entries:
```python
# Look in job progress_logs for entries with "off_market_check"
```

### Step 3: Verify Off-Market Properties Are Updated

```javascript
// Find properties marked as OFF_MARKET
db.properties.find({
  "status": "OFF_MARKET",
  "scheduled_job_id": "your_job_id"
}).limit(10)

// Check their change_logs
db.properties.find({
  "status": "OFF_MARKET"
}, {
  "property_id": 1,
  "address.formatted_address": 1,
  "status": 1,
  "mls_status": 1,
  "change_logs": {"$slice": -5}
})
```

## Troubleshooting

### Issue: No properties have scheduled_job_id

**Solution:** Properties need to be scraped AFTER deployment. Run a scrape job and properties will get tagged.

### Issue: Off-market detection not running

**Check:**
1. Is the job a scheduled job (has `scheduled_job_id`)?
2. Is it scraping `for_sale` or `pending` listing types?
3. Are there properties with `last_scraped < job_start_time`?

### Issue: Too many properties being checked

**This is normal:** The system checks ALL missing properties, not just a sample. It processes in batches of 50.

### Issue: Properties not being detected as off-market

**Possible reasons:**
1. Properties are still active (not actually off-market)
2. HomeHarvest doesn't return them (may be deleted)
3. Property address format doesn't match

## Performance Considerations

- **Batch Processing:** Properties are checked in batches of 50
- **Delays:** 1 second delay between property queries, 2 seconds between batches
- **Time:** Checking 100 properties takes ~2-3 minutes
- **Rate Limiting:** Delays prevent hitting HomeHarvest rate limits

## Next Steps

1. **Monitor First Few Scrapes:**
   - Watch job logs for `[OFF-MARKET]` entries
   - Check if properties are being tagged correctly
   - Verify off-market detection is running

2. **Check Results:**
   - After a few scrape runs, check for properties with `status='OFF_MARKET'`
   - Verify change_logs are being recorded
   - Check that off-market properties are correctly identified

3. **Adjust if Needed:**
   - If too many properties are being checked, consider increasing batch size
   - If detection is too slow, consider parallel processing (future enhancement)

## Success Indicators

✅ Properties have `scheduled_job_id` after scraping  
✅ Properties have `last_scraped` timestamp after scraping  
✅ Job logs show `[OFF-MARKET]` entries  
✅ Missing properties are being checked  
✅ Off-market properties are being detected and updated  
✅ Status changes are recorded in `change_logs`

## Support

If you encounter issues:
1. Check job logs for error messages
2. Verify database indexes are created (`scheduled_job_id`, `last_scraped`)
3. Check that scheduled jobs have `scheduled_job_id` set
4. Verify properties are being scraped successfully

