# Scheduler Bug Fix - October 14, 2025

## Problem Identified

The scheduler was not running recurring jobs despite having 4 scheduled jobs in the database. An investigation revealed that no new properties had been scraped in the last 2 weeks, even though the scheduler service appeared to be running.

### Root Cause

The bug was in the `should_run_recurring_job` method in `scheduler.py` (lines 100-118). The issue was:

1. **Incorrect base time**: The method used `job.created_at` as the starting point for calculating the next run time
2. **Stale calculations**: For jobs created weeks or months ago, this would calculate the next run time based on an old date
3. **Failed comparisons**: The time difference check `(now - next_run).total_seconds()` would be millions of seconds, never falling within the `-60 to 0` second window

Example:
- Job created on September 12, 2025
- Cron schedule: Daily at 1 PM (13:00)
- Current date: October 14, 2025
- Old logic: Calculate next run after September 12 → September 12 at 13:00
- Time diff: ~30 days (2,592,000 seconds) - never triggers!

## Solution

The fix properly calculates when recurring jobs should run by:

1. **Using the correct base time**: 
   - If the job has run before, use `last_run` as the base time
   - If the job has never run, use a recent time (now - 1 day) instead of `created_at`

2. **Checking if overdue**:
   - Calculate the next scheduled run time from the base time
   - If the next scheduled time has passed (is in the past), the job should run immediately
   - If the next scheduled time is within 60 seconds, the job should run

### Code Changes

**Before (Buggy):**
```python
async def should_run_recurring_job(self, job: ScrapingJob, now: datetime) -> bool:
    if not job.cron_expression:
        return False
    
    # ❌ Using created_at - could be weeks/months ago!
    cron = croniter.croniter(job.cron_expression, job.created_at)
    next_run = cron.get_next(datetime)
    
    # ❌ This will be millions of seconds for old jobs
    time_diff = (now - next_run).total_seconds()
    return -60 <= time_diff <= 0
```

**After (Fixed):**
```python
async def should_run_recurring_job(self, job: ScrapingJob, now: datetime) -> bool:
    if not job.cron_expression:
        return False
    
    # ✅ Use last_run or a recent time
    if job.last_run:
        base_time = job.last_run
    else:
        base_time = now - timedelta(days=1)
    
    # ✅ Calculate from correct base time
    cron = croniter.croniter(job.cron_expression, base_time)
    next_run = cron.get_next(datetime)
    
    # ✅ Check if overdue or upcoming
    if next_run <= now:
        logger.info(f"Job {job.job_id} is overdue")
        return True
    
    time_until = (next_run - now).total_seconds()
    if 0 < time_until <= 60:
        logger.info(f"Job {job.job_id} will run in {time_until:.2f} seconds")
        return True
    
    return False
```

## Verification

### Test Results

Created comprehensive unit tests in `tests/test_scheduler_unit.py`:
- ✅ 9 tests, all passing
- ✅ Tests for jobs that never ran
- ✅ Tests for overdue jobs
- ✅ Tests for jobs that ran recently
- ✅ Edge case testing
- ✅ Error handling tests

### Database Verification

Ran `test_scheduler_fix.py` which confirmed:
- 4 recurring jobs found in database:
  - `for_sale` - scheduled daily at 13:00 UTC (never ran before)
  - `pending` - scheduled daily at 14:00 UTC (last ran 29 days ago)
  - `sold` - scheduled daily at 15:00 UTC (last ran 29 days ago)
  - `for_rent` - scheduled daily at 16:00 UTC (never ran before)

All 4 jobs are now correctly identified as needing to run immediately (they are overdue).

## Impact

### Before Fix
- ❌ No recurring jobs were running
- ❌ No properties scraped in 2 weeks
- ❌ Scheduler appeared to be running but was ineffective
- ❌ Database was becoming stale

### After Fix
- ✅ All 4 recurring jobs correctly identified as overdue
- ✅ Scheduler will create job instances when jobs are due
- ✅ Properties will be scraped daily according to cron schedules
- ✅ Database will stay up-to-date with fresh property data

## Testing the Fix

### Run Unit Tests
```bash
python -m pytest tests/test_scheduler_unit.py -v --asyncio-mode=auto
```

### Run Integration Test
```bash
python test_scheduler_fix.py
```

### Check Recurring Jobs Status
```bash
python check_recurring_jobs.py
```

## Current Recurring Jobs

The system has 4 recurring jobs configured:

| Job Name | Listing Type | Cron Schedule | Description |
|----------|-------------|---------------|-------------|
| `indianapolis_complete_core_indianapolis_for_sale_*` | for_sale | `0 13 * * *` | Daily at 1 PM UTC |
| `indianapolis_complete_core_indianapolis_pending_*` | pending | `0 14 * * *` | Daily at 2 PM UTC |
| `indianapolis_complete_core_indianapolis_sold_*` | sold | `0 15 * * *` | Daily at 3 PM UTC |
| `indianapolis_complete_core_indianapolis_for_rent_*` | for_rent | `0 16 * * *` | Daily at 4 PM UTC |

All jobs scrape properties for:
- Location: Indianapolis, IN
- Location: Marion County, IN

## Next Steps

1. **Deploy the fix**: The fix is ready to be deployed to production
2. **Monitor**: Watch for new job instances being created
3. **Verify scraping**: Check that properties are being scraped daily
4. **Update documentation**: Consider updating the main README with scheduler information

## Files Changed

- `scheduler.py` - Fixed the `should_run_recurring_job` method
- `tests/test_scheduler_unit.py` - New comprehensive unit tests
- `test_scheduler_fix.py` - Integration test to verify fix
- `check_recurring_jobs.py` - Utility to check recurring job status

## Prevention

To prevent this issue in the future:

1. **Monitoring**: Set up alerts for when recurring jobs haven't created instances in 24+ hours
2. **Testing**: The new unit tests will catch similar bugs
3. **Logging**: The fixed code includes better logging to show when jobs should run
4. **Documentation**: This document serves as a reference for the scheduler behavior

## Related Issues

This fix resolves:
- No properties being scraped for 2 weeks
- Recurring jobs not creating job instances
- Scheduler appearing to run but not executing jobs
- Database becoming stale with old property data

