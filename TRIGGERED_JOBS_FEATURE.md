# Triggered Jobs Feature - Implementation Summary

## Overview

When you manually trigger a scheduled job (cron job), the resulting job execution is now properly connected to its original scheduled job. This provides full traceability and run history tracking.

## What Changed

### 1. Updated `/scrape/trigger` Endpoint

**File:** `main.py` (lines 485-619)

The trigger endpoint now:
- **First checks** the `scheduled_jobs` collection for the job ID
- If found, creates a `ScrapingJob` with `scheduled_job_id` properly set
- **Falls back** to legacy `jobs` collection for backward compatibility
- Preserves the `scheduled_job_id` chain even for legacy jobs

### 2. Automatic Run History Tracking

**File:** `scraper.py` (lines 108-123, 144-158)

The scraper already had logic to track run history:
- When a job with `scheduled_job_id` completes, it updates the parent scheduled job's run history
- Tracks: `run_count`, `last_run_at`, `last_run_status`, `last_run_job_id`, `next_run_at`
- Works for both auto-scheduled runs and manually triggered runs

### 3. New Query Endpoints

**File:** `main.py` (lines 655-767)

Added two new endpoints to view scheduled jobs and their run history:

#### `GET /scheduled-jobs`
Lists all scheduled jobs (cron jobs) with filtering options.

**Query Parameters:**
- `status` (optional): Filter by status (`active`, `inactive`, `paused`)
- `limit` (default: 50): Maximum number of results

**Response:**
```json
{
  "scheduled_jobs": [
    {
      "scheduled_job_id": "sold_daily_indy",
      "name": "Daily Sold Properties - Indianapolis",
      "status": "active",
      "cron_expression": "0 2 * * *",
      "locations": ["Indianapolis, IN"],
      "listing_type": "sold",
      "run_count": 15,
      "last_run_at": "2025-01-13T02:00:00Z",
      "last_run_status": "completed",
      "next_run_at": "2025-01-14T02:00:00Z"
    }
  ],
  "total": 1
}
```

#### `GET /scheduled-jobs/{scheduled_job_id}`
Gets detailed information about a specific scheduled job including all its runs.

**Query Parameters:**
- `include_runs` (default: true): Include job execution history
- `limit` (default: 50): Maximum number of runs to include

**Response:**
```json
{
  "scheduled_job": {
    "scheduled_job_id": "sold_daily_indy",
    "name": "Daily Sold Properties - Indianapolis",
    "status": "active",
    "cron_expression": "0 2 * * *",
    "locations": ["Indianapolis, IN"],
    "listing_type": "sold",
    "run_count": 15,
    "last_run_at": "2025-01-13T02:00:00Z",
    "last_run_status": "completed",
    "next_run_at": "2025-01-14T02:00:00Z"
  },
  "job_runs": [
    {
      "job_id": "triggered_sold_daily_indy_1736841234_abc123",
      "status": "completed",
      "priority": "immediate",
      "created_at": "2025-01-13T15:30:00Z",
      "started_at": "2025-01-13T15:30:01Z",
      "completed_at": "2025-01-13T15:32:45Z",
      "properties_scraped": 120,
      "properties_saved": 95
    },
    {
      "job_id": "scheduled_sold_daily_indy_1736830800_def456",
      "status": "completed",
      "priority": "normal",
      "created_at": "2025-01-13T02:00:00Z",
      "started_at": "2025-01-13T02:00:01Z",
      "completed_at": "2025-01-13T02:05:23Z",
      "properties_scraped": 115,
      "properties_saved": 92
    }
  ],
  "total_runs": 2
}
```

## Usage Examples

### Triggering a Scheduled Job

```bash
# Trigger a scheduled job to run immediately
curl -X POST http://localhost:8000/scrape/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "sold_daily_indy",
    "priority": "immediate"
  }'
```

**Response:**
```json
{
  "job_id": "triggered_sold_daily_indy_1736841234_abc123",
  "status": "pending",
  "message": "Triggered scheduled job 'Daily Sold Properties - Indianapolis' to run immediately",
  "created_at": "2025-01-13T15:30:00Z"
}
```

### Viewing Scheduled Job History

```bash
# Get all runs for a specific scheduled job
curl http://localhost:8000/scheduled-jobs/sold_daily_indy
```

### Listing All Scheduled Jobs

```bash
# Get all active scheduled jobs
curl http://localhost:8000/scheduled-jobs?status=active

# Get all scheduled jobs (active, inactive, paused)
curl http://localhost:8000/scheduled-jobs
```

## Database Schema

### ScrapingJob Model (jobs collection)

```python
{
  "job_id": "triggered_sold_daily_indy_1736841234_abc123",
  "scheduled_job_id": "sold_daily_indy",  # ← Links to parent scheduled job
  "priority": "immediate",
  "status": "completed",
  "locations": ["Indianapolis, IN"],
  "listing_type": "sold",
  "created_at": "2025-01-13T15:30:00Z",
  "started_at": "2025-01-13T15:30:01Z",
  "completed_at": "2025-01-13T15:32:45Z",
  "properties_scraped": 120,
  "properties_saved": 95
}
```

### ScheduledJob Model (scheduled_jobs collection)

```python
{
  "scheduled_job_id": "sold_daily_indy",
  "name": "Daily Sold Properties - Indianapolis",
  "status": "active",
  "cron_expression": "0 2 * * *",
  "locations": ["Indianapolis, IN"],
  "listing_type": "sold",
  "run_count": 15,  # ← Updated each time a job runs
  "last_run_at": "2025-01-13T02:00:00Z",  # ← Last execution time
  "last_run_status": "completed",  # ← Status of last run
  "last_run_job_id": "scheduled_sold_daily_indy_1736830800_def456",  # ← Last job ID
  "next_run_at": "2025-01-14T02:00:00Z"  # ← Next scheduled run
}
```

## Key Benefits

1. **Full Traceability**: Every job run is linked to its scheduled job via `scheduled_job_id`
2. **Run History**: View all executions (manual + automated) in one place
3. **Statistics**: Track total runs, success rate, last run status
4. **Backward Compatible**: Still works with legacy jobs in the `jobs` collection
5. **Query Support**: New endpoints to easily view scheduled jobs and their history

## Job ID Naming Convention

- **Scheduled runs**: `scheduled_{scheduled_job_id}_{timestamp}_{random}`
- **Triggered runs**: `triggered_{scheduled_job_id}_{timestamp}_{random}`
- **Legacy runs**: `immediate_{timestamp}_{random}` or `recurring_{timestamp}_{random}`

This naming makes it easy to identify the source of each job execution.

## Integration with Frontend

The frontend can now:

1. **List all scheduled jobs** with their status and next run time
2. **Trigger any scheduled job** manually with a button click
3. **View complete run history** for each scheduled job
4. **Monitor job performance** (success rate, average duration, properties scraped)
5. **Filter by status** to see only active, paused, or inactive jobs

## Example Frontend Workflow

```typescript
// 1. Get all active scheduled jobs
const response = await fetch('/scheduled-jobs?status=active');
const { scheduled_jobs } = await response.json();

// 2. Display list with "Trigger Now" button for each job
scheduled_jobs.forEach(job => {
  displayJob(job);
});

// 3. When user clicks "Trigger Now"
const triggerResponse = await fetch('/scrape/trigger', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    job_id: 'sold_daily_indy',
    priority: 'immediate'
  })
});

// 4. View run history
const historyResponse = await fetch('/scheduled-jobs/sold_daily_indy');
const { scheduled_job, job_runs } = await historyResponse.json();

// 5. Display all runs (automatic + manual) in a table
displayRunHistory(job_runs);
```

## Testing

You can test this feature using the FastAPI docs:
1. Open http://localhost:8000/docs
2. Try the new endpoints:
   - `GET /scheduled-jobs` - List all scheduled jobs
   - `GET /scheduled-jobs/{scheduled_job_id}` - View job details and history
   - `POST /scrape/trigger` - Trigger a job manually

## Notes

- Manually triggered jobs are tracked the same way as automatic scheduled runs
- The `run_count` on the scheduled job increases for both manual and automatic runs
- The scheduled job's `next_run_at` is recalculated after each run (manual or automatic)
- Legacy jobs using `original_job_id` are still supported for backward compatibility

