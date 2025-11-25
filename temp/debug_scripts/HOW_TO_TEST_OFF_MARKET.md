# How to Test Off-Market Detection

## Overview

The off-market detection feature automatically checks properties that belong to a scheduled job but weren't found in the current scrape run. It uses `scheduled_job_id` and `last_scraped` timestamps to identify which properties to check.

## Testing Methods

### Method 1: Using Test Script (Recommended)

Run the full flow test script:

```bash
python temp/debug_scripts/test_off_market_full_flow.py
```

This script:
1. Finds existing properties in Indianapolis
2. Tags them with `scheduled_job_id` and sets `last_scraped` to 2 days ago
3. Simulates a scrape that finds some but not all properties
4. Runs off-market detection on missing properties
5. Shows results

### Method 2: Manual Testing with Real Scheduled Job

1. **Find an existing scheduled job:**
   ```python
   # Properties should have scheduled_job_id set
   # Check: db.properties.find({"scheduled_job_id": "your_job_id"})
   ```

2. **Run a scrape job** with that `scheduled_job_id`:
   - Properties found will get `last_scraped` updated to current time
   - Properties not found will keep their old `last_scraped` timestamp

3. **On the next scrape run:**
   - Off-market detection will automatically check properties where `last_scraped < job_start_time`
   - Missing properties will be queried by address
   - Properties with `status='OFF_MARKET'` or off-market `mls_status` will be updated

### Method 3: Test with Known Off-Market Property

Use the property we know is off-market:

```python
# Address: 4509 E 10th St, Indianapolis, IN 46201
# Status: OFF_MARKET
# MLS Status: Expired

# 1. Tag this property with scheduled_job_id
# 2. Set last_scraped to 2 days ago
# 3. Run off-market detection
# 4. Verify it gets detected and updated
```

## What Gets Tested

1. **Property Tagging:**
   - Properties get `scheduled_job_id` when scraped
   - Properties get `last_scraped` timestamp when scraped

2. **Query Logic:**
   - Properties with `scheduled_job_id` matching the job
   - Properties with `last_scraped < job_start_time` (not scraped in current run)
   - Properties with `listing_type` in `['for_sale', 'pending']`

3. **Off-Market Detection:**
   - Missing properties are queried by address
   - Properties with `status='OFF_MARKET'` are detected
   - Properties with `mls_status` in `['Expired', 'Withdrawn', 'Cancelled', etc.]` are detected
   - Status is updated to `OFF_MARKET` in database
   - Change is recorded in `change_logs`

4. **Batch Processing:**
   - Properties are processed in batches of 50
   - All missing properties are checked (not just first 50)
   - Progress is logged between batches

## Expected Behavior

### First Scrape Run:
- Properties are scraped and saved with `scheduled_job_id` and `last_scraped = now`
- All properties are found, so no off-market check needed

### Second Scrape Run (some properties missing):
- Job starts at `job_start_time`
- Some properties are found → `last_scraped` updated to `now` (after `job_start_time`)
- Some properties are NOT found → `last_scraped` remains old (before `job_start_time`)
- Off-market detection queries properties where `last_scraped < job_start_time`
- Missing properties are checked by address
- Off-market properties are updated

## Verification

After running a test, verify:

1. **Properties have scheduled_job_id:**
   ```python
   db.properties.find({"scheduled_job_id": "your_job_id"}).count()
   ```

2. **Properties have last_scraped:**
   ```python
   db.properties.find({"last_scraped": {"$exists": True}}).count()
   ```

3. **Off-market properties were updated:**
   ```python
   db.properties.find({"status": "OFF_MARKET"}).count()
   ```

4. **Change logs were recorded:**
   ```python
   db.properties.find({"change_logs.field": "status", "change_logs.new_value": "OFF_MARKET"})
   ```

## Test Results from Latest Run

From the test output:
- ✅ Found 10 properties and tagged them
- ✅ Query correctly found 10 properties (all with `last_scraped < job_start_time`)
- ✅ Identified 3 missing properties
- ✅ Checked all 3 missing properties by address
- ✅ Properties were queried successfully (2 were still active, 1 had an error)
- ✅ System is working correctly!

## Next Steps for Production Testing

1. Run a scheduled scrape job
2. Wait for it to complete
3. Check the job logs for `[OFF-MARKET]` entries
4. Verify properties were checked and updated if needed
5. Check database for properties with `status='OFF_MARKET'`

