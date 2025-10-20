# New Scheduler Architecture

## Overview

The scraper has been refactored to separate **scheduled jobs** (cron configurations) from **job executions** (individual runs). This provides better organization, clearer status tracking, and easier management of recurring jobs.

## Architecture

### Two Collections

#### 1. `scheduled_jobs` Collection

Stores recurring job configurations (cron jobs)

**Purpose**: Defines WHAT and WHEN to scrape

**Status Values**:

- `active` - Job will run on schedule
- `inactive` - Job is disabled
- `paused` - Temporarily stopped

**Key Fields**:

- `scheduled_job_id` - Unique identifier
- `name` - Human-readable name
- `description` - What this job does
- `status` - active/inactive/paused
- `cron_expression` - When to run (e.g., "0 13 \* \* \*")
- `locations`, `listing_type`, etc. - Scraping parameters (template)
- `run_count` - How many times it has run
- `last_run_at` - When it last ran
- `last_run_status` - Status of last run
- `next_run_at` - Calculated next run time

#### 2. `jobs` Collection

Stores individual job executions (both one-time and from scheduled jobs)

**Purpose**: Tracks actual execution and results

**Status Values**:

- `pending` - Waiting to run
- `running` - Currently executing
- `completed` - Finished successfully
- `failed` - Execution failed
- `cancelled` - Manually stopped

**Key Fields**:

- `job_id` - Unique identifier for this execution
- `scheduled_job_id` - Reference to parent scheduled job (if applicable)
- `status` - Current execution status
- `locations`, `listing_type`, etc. - Scraping parameters for this run
- `properties_scraped` - Results
- `started_at`, `completed_at` - Execution timing
- `error_message` - If failed

## Benefits

### 1. Separation of Concerns

- **Scheduled Jobs**: Configuration and scheduling
- **Job Executions**: Running and results

### 2. Clear Status Tracking

- **Scheduled Job Status**: Is the cron job active or inactive?
- **Job Execution Status**: Is this particular run pending/running/completed/failed?

### 3. Better Management

- Pause/resume scheduled jobs without deleting them
- View all executions of a scheduled job
- Track statistics per scheduled job
- Easy to see which jobs are active vs inactive

### 4. Cleaner Database

- No mixing of cron configurations with execution data
- Clear parent-child relationship
- Easier queries and analytics

## Usage Examples

### Creating a Scheduled Job

```python
from models import ScheduledJob, ScheduledJobStatus, ListingType, JobPriority

# Create a scheduled job
scheduled_job = ScheduledJob(
    scheduled_job_id="daily_for_sale_indianapolis",
    name="Daily FOR_SALE - Indianapolis",
    description="Daily scraping of for_sale properties in Indianapolis",
    status=ScheduledJobStatus.ACTIVE,
    cron_expression="0 13 * * *",  # Daily at 1 PM UTC
    locations=["Indianapolis, IN", "Marion County, IN"],
    listing_type=ListingType.FOR_SALE,
    past_days=30,
    limit=10000,
    priority=JobPriority.NORMAL
)

# Save to database
await db.create_scheduled_job(scheduled_job)
```

### Querying Scheduled Jobs

```python
# Get all active scheduled jobs
active_jobs = await db.get_active_scheduled_jobs()

# Get a specific scheduled job
job = await db.get_scheduled_job("daily_for_sale_indianapolis")

# Get all scheduled jobs (active and inactive)
all_jobs = await db.get_all_scheduled_jobs()
```

### Managing Scheduled Jobs

```python
# Pause a scheduled job
await db.update_scheduled_job_status(
    "daily_for_sale_indianapolis",
    ScheduledJobStatus.PAUSED
)

# Reactivate a scheduled job
await db.update_scheduled_job_status(
    "daily_for_sale_indianapolis",
    ScheduledJobStatus.ACTIVE
)

# Update scheduled job parameters
await db.update_scheduled_job(
    "daily_for_sale_indianapolis",
    {
        "past_days": 60,
        "limit": 5000
    }
)

# Delete a scheduled job
await db.delete_scheduled_job("daily_for_sale_indianapolis")
```

### Viewing Job Executions

```python
# Find all executions of a scheduled job
cursor = db.jobs_collection.find({
    "scheduled_job_id": "daily_for_sale_indianapolis"
}).sort("created_at", -1).limit(10)

executions = []
async for job_data in cursor:
    executions.append(job_data)

# Get the latest execution
latest = executions[0] if executions else None
```

## Scheduler Behavior

### How It Works

1. **Every 60 seconds**, the scheduler:

   - Queries `scheduled_jobs` collection for ACTIVE jobs
   - Checks if each job's next run time has passed
   - If yes, creates a new job instance in `jobs` collection
   - The job instance references the parent via `scheduled_job_id`

2. **When a job completes**:

   - Updates job status in `jobs` collection
   - Updates scheduled job's `last_run_at`, `run_count`, `last_run_status`
   - Calculates and stores `next_run_at` for the scheduled job

3. **Status Independence**:
   - Scheduled job status (`active`/`inactive`) controls whether new executions are created
   - Job execution status (`running`/`completed`/`failed`) tracks individual run status

## Migration

### Migrating Existing Cron Jobs

Run the migration script to convert existing jobs with `cron_expression` to the new architecture:

```bash
python migrate_to_scheduled_jobs.py
```

This script:

1. Finds all jobs with `cron_expression` in the `jobs` collection
2. Creates corresponding entries in `scheduled_jobs` collection
3. Updates existing job instances to reference the new scheduled job
4. Marks old cron jobs as migrated

### Backward Compatibility

The system maintains backward compatibility:

- Legacy jobs with `cron_expression` will still work
- Old `original_job_id` references are still supported
- Deprecated fields are marked but not removed

## Monitoring

### Checking Scheduled Jobs Status

```python
# Get all active scheduled jobs with their status
scheduled_jobs = await db.get_active_scheduled_jobs()

for job in scheduled_jobs:
    print(f"{job.name}:")
    print(f"  Status: {job.status}")
    print(f"  Last run: {job.last_run_at}")
    print(f"  Next run: {job.next_run_at}")
    print(f"  Run count: {job.run_count}")
    print(f"  Last status: {job.last_run_status}")
```

### Checking Recent Executions

```python
# Get recent job executions
cursor = db.jobs_collection.find().sort("created_at", -1).limit(10)

async for job in cursor:
    print(f"{job['job_id']}:")
    print(f"  Status: {job['status']}")
    print(f"  Scheduled Job: {job.get('scheduled_job_id', 'N/A')}")
    print(f"  Properties: {job.get('properties_scraped', 0)} scraped")
```

## Database Indexes

The following indexes are created automatically:

### scheduled_jobs Collection

- `scheduled_job_id` (unique)
- `status`
- `cron_expression`
- `next_run_at`
- `last_run_at`

### jobs Collection

- `job_id` (unique)
- `status`
- `scheduled_job_id` (new index for parent reference)
- `priority`, `created_at`
- `scheduled_at`

## API Updates

The API endpoints remain largely the same, with new endpoints for managing scheduled jobs:

### New Endpoints (to be implemented)

- `GET /scheduled-jobs` - List all scheduled jobs
- `GET /scheduled-jobs/{id}` - Get a specific scheduled job
- `POST /scheduled-jobs` - Create a new scheduled job
- `PUT /scheduled-jobs/{id}` - Update a scheduled job
- `DELETE /scheduled-jobs/{id}` - Delete a scheduled job
- `PUT /scheduled-jobs/{id}/status` - Change scheduled job status (activate/pause/inactivate)
- `GET /scheduled-jobs/{id}/executions` - Get all executions of a scheduled job

## Example: Current Jobs After Migration

After running the migration, your 4 existing cron jobs will become:

### scheduled_jobs Collection

```
{
  "_id": "...",
  "scheduled_job_id": "scheduled_for_sale_...",
  "name": "FOR_SALE - Indianapolis, IN",
  "status": "active",
  "cron_expression": "0 13 * * *",
  "locations": ["Indianapolis, IN", "Marion County, IN"],
  "listing_type": "for_sale",
  "run_count": 0,
  "last_run_at": null,
  "next_run_at": "2025-10-15T13:00:00Z"
}
```

### jobs Collection (when it runs)

```
{
  "_id": "...",
  "job_id": "scheduled_scheduled_for_sale_..._1729003200_abc123",
  "scheduled_job_id": "scheduled_for_sale_...",
  "status": "pending",
  "locations": ["Indianapolis, IN", "Marion County, IN"],
  "listing_type": "for_sale",
  "created_at": "2025-10-15T13:00:00Z"
}
```

## Troubleshooting

### Scheduled Jobs Not Running?

1. Check scheduled job is ACTIVE:

   ```python
   job = await db.get_scheduled_job("job_id")
   print(f"Status: {job.status}")
   ```

2. Check next_run_at is correct:

   ```python
   print(f"Next run: {job.next_run_at}")
   print(f"Current time: {datetime.utcnow()}")
   ```

3. Check scheduler is running:
   ```bash
   # Look for "Job Scheduler started" in logs
   ```

### Job Executions Not Completing?

Check the job execution status:

```python
job = await db.get_job("job_id")
print(f"Status: {job.status}")
print(f"Error: {job.error_message}")
```

---

## Summary

The new architecture provides:
✅ Clear separation between scheduling and execution  
✅ Independent status tracking for cron jobs vs runs  
✅ Better organization and management  
✅ Easier monitoring and analytics  
✅ Backward compatibility with existing code

All existing functionality is preserved while providing a more robust and maintainable system.
