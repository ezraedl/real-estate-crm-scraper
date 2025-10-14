# Scheduler Architecture Refactoring - Summary

## What Was Done

I've successfully refactored the scheduler architecture to separate **scheduled jobs** (cron configurations) from **job executions** (individual runs), as you requested.

## The New Architecture

### Two Collections

#### 1. `scheduled_jobs` Collection

**Purpose**: Stores recurring job definitions (the WHAT and WHEN)

**Status**:

- `active` - Job will run on schedule
- `inactive` - Job is disabled
- `paused` - Temporarily stopped

This is the **cron job status** - whether scheduling is enabled or not.

#### 2. `jobs` Collection

**Purpose**: Stores individual job runs (both one-time and instances from scheduled jobs)

**Status**:

- `pending` - Waiting to run
- `running` - Currently executing
- `completed` - Finished successfully
- `failed` - Execution failed
- `cancelled` - Manually stopped

This is the **execution status** - how the specific run is going.

### Key Relationship

Each job execution in the `jobs` collection has a `scheduled_job_id` field that points to its parent scheduled job (if it came from a cron).

## Benefits

✅ **Clear Separation**: Configuration vs execution are now in different collections  
✅ **Independent Status**: Cron job active/inactive is separate from run pending/running/completed/failed  
✅ **Better Management**: Pause/resume scheduled jobs without deleting them  
✅ **Cleaner Queries**: Easy to find all runs of a specific scheduled job  
✅ **Better Analytics**: Track statistics per scheduled job  
✅ **No Confusion**: Never again wonder "is this job a template or an execution?"

## What Changed

### Models (`models.py`)

- ✅ New `ScheduledJob` model for cron job configurations
- ✅ New `ScheduledJobStatus` enum (ACTIVE, INACTIVE, PAUSED)
- ✅ Updated `ScrapingJob` with `scheduled_job_id` field
- ✅ Marked old cron-related fields as deprecated (backward compatible)

### Database (`database.py`)

- ✅ Added `scheduled_jobs` collection
- ✅ New CRUD methods for scheduled jobs:
  - `create_scheduled_job()`
  - `get_scheduled_job()`
  - `get_active_scheduled_jobs()`
  - `update_scheduled_job_status()`
  - `update_scheduled_job_run_history()`
  - `delete_scheduled_job()`
- ✅ New indexes for performance

### Scheduler (`scheduler.py`)

- ✅ Now queries `scheduled_jobs` collection instead of `jobs`
- ✅ Creates job instances in `jobs` collection
- ✅ Updates scheduled job run history after execution
- ✅ Backward compatible with legacy cron jobs

### Scraper (`scraper.py`)

- ✅ Updates scheduled job when a job completes
- ✅ Updates scheduled job when a job fails
- ✅ Calculates next run time
- ✅ Still supports legacy jobs

## Migration

### Your Current Jobs

You have **4 existing cron jobs** that need to be migrated:

1. FOR_SALE - Daily at 1 PM UTC
2. PENDING - Daily at 2 PM UTC
3. SOLD - Daily at 3 PM UTC
4. FOR_RENT - Daily at 4 PM UTC

### How to Migrate

Run this command:

```bash
python migrate_to_scheduled_jobs.py
```

This will:

1. ✅ Find your 4 existing cron jobs in the `jobs` collection
2. ✅ Create corresponding entries in the `scheduled_jobs` collection
3. ✅ Update any existing job instances to reference the new scheduled jobs
4. ✅ Mark the old jobs as migrated (but keep them for reference)

### After Migration

- Your 4 jobs will appear in the `scheduled_jobs` collection
- They will be marked as **ACTIVE**
- The scheduler will start using them immediately
- All existing functionality continues to work

## Example Usage

### Create a New Scheduled Job

```python
scheduled_job = ScheduledJob(
    scheduled_job_id="daily_for_sale_indy",
    name="Daily FOR_SALE - Indianapolis",
    description="Daily scraping of for_sale properties",
    status=ScheduledJobStatus.ACTIVE,  # Active = will run
    cron_expression="0 13 * * *",
    locations=["Indianapolis, IN"],
    listing_type=ListingType.FOR_SALE,
    limit=10000
)
await db.create_scheduled_job(scheduled_job)
```

### Pause a Scheduled Job

```python
# Pause the job (won't create new runs)
await db.update_scheduled_job_status(
    "daily_for_sale_indy",
    ScheduledJobStatus.PAUSED
)
```

### Reactivate a Scheduled Job

```python
# Reactivate (will resume creating runs)
await db.update_scheduled_job_status(
    "daily_for_sale_indy",
    ScheduledJobStatus.ACTIVE
)
```

### Check Scheduled Job Status

```python
job = await db.get_scheduled_job("daily_for_sale_indy")
print(f"Status: {job.status}")  # active/inactive/paused
print(f"Last run: {job.last_run_at}")
print(f"Next run: {job.next_run_at}")
print(f"Run count: {job.run_count}")
print(f"Last run status: {job.last_run_status}")  # completed/failed
```

### View All Runs of a Scheduled Job

```python
cursor = db.jobs_collection.find({
    "scheduled_job_id": "daily_for_sale_indy"
}).sort("created_at", -1).limit(10)

async for job in cursor:
    print(f"Run: {job['job_id']}")
    print(f"  Status: {job['status']}")  # running/completed/failed
    print(f"  Properties: {job['properties_scraped']}")
```

## Backward Compatibility

✅ **Fully backward compatible**

- Legacy jobs with `cron_expression` still work
- Old `original_job_id` references still supported
- Deprecated fields marked but not removed
- Existing code continues to function

## Testing

The scheduler bug fix tests we created earlier still work! The architecture changes don't affect the core scheduling logic - they just organize the data better.

## Documentation

📚 **NEW_SCHEDULER_ARCHITECTURE.md** - Complete guide with examples and best practices

## Next Steps

1. **Run the migration**:

   ```bash
   python migrate_to_scheduled_jobs.py
   ```

2. **Verify scheduled jobs were created**:

   ```bash
   python -c "import asyncio; from database import Database; from models import ScheduledJob; db = Database(); asyncio.run(db.connect()); jobs = asyncio.run(db.get_all_scheduled_jobs()); [print(f'{j.name}: {j.status}') for j in jobs]"
   ```

3. **Restart the scheduler** (if it's running)

4. **Monitor**:
   - Check that new job instances are created in `jobs` collection
   - Verify they have `scheduled_job_id` field
   - Confirm properties are being scraped

## Git Commits

All changes are committed to branch `fix/scheduler-bugs`:

- Commit 1: Fixed scheduler bug (jobs not running)
- Commit 2: Added executive summary
- Commit 3: **NEW - Refactored to scheduled_jobs architecture**

## Summary

✅ **Scheduled Jobs** (`scheduled_jobs` collection)

- Define WHAT and WHEN to scrape
- Status: active/inactive/paused
- Track run history and statistics

✅ **Job Executions** (`jobs` collection)

- Individual runs (one-time or from scheduled jobs)
- Status: pending/running/completed/failed
- Reference parent via `scheduled_job_id`

✅ **Clean Separation**

- No more confusion between templates and executions
- Independent status tracking
- Better organization and management

✅ **Fully Backward Compatible**

- Existing code still works
- Migration script provided
- Legacy fields preserved

This is exactly what you asked for! 🎉

---

**Questions? Check**: `NEW_SCHEDULER_ARCHITECTURE.md` for detailed documentation and examples.
