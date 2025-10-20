# Dynamic Progress Tracking - How It Works

## Overview
Properties are saved and counts updated after EACH listing type fetch, providing real-time visibility into scraping progress.

## Flow Diagram

```
Job Starts → for_rent → Save → Update UI
                ↓
              pending → Save → Update UI
                ↓
              sold → Save → Update UI
                ↓
              for_sale → Save → Update UI
                ↓
            Location Complete
```

## Technical Implementation

### 1. Scraper Level (`scraper.py`)

**After Each Listing Type Fetch:**
```python
# Fetch properties (e.g., for_rent)
properties = await self._scrape_listing_type(location, job, ...)

# Save immediately to database
save_results = await db.save_properties_batch(properties)

# Update running totals
running_totals["total_properties"] += len(properties)
running_totals["saved_properties"] += save_results["inserted"] + save_results["updated"]
running_totals["total_inserted"] += save_results["inserted"]
running_totals["total_updated"] += save_results["updated"]
running_totals["total_skipped"] += save_results["skipped"]

# Push to database immediately
await db.update_job_status(
    job_id,
    JobStatus.RUNNING,
    properties_scraped=running_totals["total_properties"],
    properties_saved=running_totals["saved_properties"],
    properties_inserted=running_totals["total_inserted"],
    properties_updated=running_totals["total_updated"],
    properties_skipped=running_totals["total_skipped"],
    progress_logs=[...]  # With detailed per-type breakdown
)
```

### 2. Database Level (`database.py`)

**Job Document Updates:**
```json
{
  "job_id": "job_123",
  "status": "running",
  "properties_scraped": 2283,     // Updated after each type
  "properties_saved": 2050,       // Updated after each type
  "properties_inserted": 150,     // Updated after each type
  "properties_updated": 1900,     // Updated after each type
  "properties_skipped": 133,      // Updated after each type
  "completed_locations": 1,       // Updated after each location
  "total_locations": 2,           // Set at start
  "progress_logs": [              // Updated after each type
    {
      "location": "Indianapolis, IN",
      "status": "in_progress",
      "properties_found": 2283,
      "inserted": 150,
      "updated": 1900,
      "skipped": 133,
      "listing_type_breakdown": [
        {
          "listing_type": "for_rent",
          "status": "completed",
          "properties_found": 904,
          "inserted": 50,
          "updated": 800,
          "skipped": 54,
          "duration_seconds": 32.8
        },
        {
          "listing_type": "pending",
          "status": "completed",
          "properties_found": 1379,
          "inserted": 100,
          "updated": 1100,
          "skipped": 79,
          "duration_seconds": 25.5
        }
      ]
    }
  ]
}
```

### 3. API Level (`main.py`)

**Endpoint Returns All Fields:**
```python
@app.get("/scheduled-jobs/{scheduled_job_id}")
async def get_scheduled_job_details(...):
    job_runs.append({
        "job_id": job_data.get("job_id"),
        "status": job_data.get("status"),
        "properties_scraped": job_data.get("properties_scraped", 0),
        "properties_saved": job_data.get("properties_saved", 0),
        "properties_inserted": job_data.get("properties_inserted", 0),
        "properties_updated": job_data.get("properties_updated", 0),
        "properties_skipped": job_data.get("properties_skipped", 0),
        "completed_locations": job_data.get("completed_locations", 0),
        "total_locations": job_data.get("total_locations", 0),
        "progress_logs": job_data.get("progress_logs", [])
    })
```

### 4. UI Level (`ScrapingManagement.tsx`)

**Auto-Refresh Every 10 Seconds:**
```typescript
useEffect(() => {
  const hasRunningJobs = jobs.some((job) => job.has_running_job);
  if (!hasRunningJobs) return;
  
  const interval = setInterval(async () => {
    // Silent refresh - updates data without re-rendering
    loadJobs(true);
    
    // Refresh expanded job details
    if (expandedJobId) {
      const [details, stats] = await Promise.all([
        scheduledJobsApi.get(expandedJobId),
        scheduledJobsApi.getStats(expandedJobId),
      ]);
      // Updates visible counts automatically
    }
  }, 10000); // Every 10 seconds
}, [jobs, expandedJobId]);
```

**Display in Table:**
```tsx
<TableCell align="right">
  <Tooltip title="Inserted: 150 | Updated: 1900 | Skipped: 133">
    <Typography>
      {run.properties_scraped} / {run.properties_saved}
      {/* Shows: 2283 / 2050 */}
    </Typography>
  </Tooltip>
  {run.status === "running" && (
    <Typography variant="caption">
      Loc: {run.completed_locations}/{run.total_locations}
      {/* Shows: Loc: 1/2 */}
    </Typography>
  )}
</TableCell>
```

## Timeline Example

**Job: Indianapolis, IN (4 listing types)**

```
[00:00] Job starts
        UI shows: 0 / 0

[00:33] for_rent complete (904 properties, 32.8s)
        Saved: 50 inserted, 800 updated, 54 skipped
        DB updated immediately
        UI shows: 904 / 850

[00:59] pending complete (1379 properties, 25.5s)
        Saved: 100 inserted, 1100 updated, 79 skipped
        DB updated immediately
        UI shows: 2283 / 2050 (cumulative!)

[01:15] sold complete (1143 properties, 16.1s)
        Saved: 80 inserted, 1000 updated, 63 skipped
        DB updated immediately
        UI shows: 3426 / 3130

[01:50] for_sale complete (1500 properties, 35s)
        Saved: 120 inserted, 1300 updated, 80 skipped
        DB updated immediately
        UI shows: 4926 / 4550

[01:50] Location complete, move to next location
```

## Benefits

### 1. Real-Time Visibility
- ✅ See counts update every ~30 seconds (per listing type)
- ✅ No more waiting 3 minutes for first update
- ✅ Know immediately if scraper is working

### 2. Progress Tracking
- ✅ Track completion: "2283 properties so far"
- ✅ Monitor efficiency: "2050/2283 = 90% save rate"
- ✅ Location progress: "Loc: 1/2"

### 3. Problem Detection
- ✅ Spot stuck jobs: "3 min, still 0 properties" → warning!
- ✅ See errors immediately: "for_rent: Error..."
- ✅ Monitor save rate: "Only 50% saved? Something wrong?"

### 4. Detailed Breakdown
- ✅ Per-listing-type stats: "for_rent: 50 inserted"
- ✅ Inserted vs Updated vs Skipped
- ✅ Duration per type

## Console Output

```
[FETCH] Fetching for_rent properties...
[OK] Found 904 for_rent properties in 32.8s
[SAVED] 50 inserted, 800 updated, 54 skipped
[FETCH] Fetching pending properties...
[OK] Found 1379 pending properties in 25.5s
[SAVED] 100 inserted, 1100 updated, 79 skipped
[FETCH] Fetching sold properties...
[OK] Found 1143 sold properties in 16.1s
[SAVED] 80 inserted, 1000 updated, 63 skipped
[FETCH] Fetching for_sale properties...
[OK] Found 1500 for_sale properties in 35.0s
[SAVED] 120 inserted, 1300 updated, 80 skipped
[TOTAL] Location complete: 4926 found, 350 inserted, 4200 updated, 276 skipped
```

## UI Display Example

### Job Row (Main Table)
```
| Status  | Started | Duration | Properties | Loc  | Actions |
|---------|---------|----------|------------|------|---------|
| running | 10:30   | 1m 15s   | 3426/3130  | 1/2  | Cancel  |
                                  ↑      ↑
                           scraped  saved
```

### Expanded Progress Log
```
📋 Progress Log

🔄 Location 1/2: Indianapolis, IN (in progress)
   Found: 3426 | Inserted: 230 | Updated: 2900 | Skipped: 196

   [for_rent: 904 (32.8s)] [pending: 1379 (25.5s)] [sold: 1143 (16.1s)] [for_sale: fetching...]
```

### Tooltip on Hover
```
Inserted: 230
Updated: 2900
Skipped: 196
```

## Key Features

1. **Immediate Save** - Properties saved right after fetch (not at end)
2. **Incremental Updates** - Running totals updated after each type
3. **Database Sync** - DB updated immediately after each save
4. **Auto-Refresh** - UI polls every 10 seconds for running jobs
5. **No Re-render** - Silent refresh preserves scroll position
6. **Detailed Logs** - Per-type breakdown with save stats

## Performance Impact

**Positive:**
- ✅ Better user experience (live feedback)
- ✅ Earlier problem detection
- ✅ More granular progress tracking

**Negative:**
- ⚠️ More frequent DB writes (4× per location vs 1×)
- ⚠️ Slightly higher DB load during active scraping

**Conclusion:** Performance impact is negligible compared to UX benefits.

## Future Enhancements

Possible improvements:
1. WebSocket for instant updates (no 10s delay)
2. Progress bar showing % completion
3. ETA calculation based on current rate
4. Save rate alerts if < 50%
5. Real-time chart showing properties/second

