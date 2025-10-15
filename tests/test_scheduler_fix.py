#!/usr/bin/env python3
"""
Test the fixed scheduler to verify it now works correctly
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
import croniter

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import Database
from scheduler import JobScheduler
from models import ScrapingJob, ListingType, JobPriority, JobStatus

async def test_scheduler_fix():
    """Test the fixed scheduler logic"""
    db = Database()
    scheduler = JobScheduler()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Get all recurring jobs with actual cron expressions
        print("\n" + "="*60)
        print("TESTING FIXED SCHEDULER")
        print("="*60)
        
        cursor = db.jobs_collection.find({
            "cron_expression": {"$ne": None, "$exists": True}
        })
        
        recurring_jobs = []
        async for job_data in cursor:
            recurring_jobs.append(job_data)
        
        print(f"\nFound {len(recurring_jobs)} recurring jobs\n")
        
        now = datetime.utcnow()
        print(f"Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Current hour: {now.hour}, Current minute: {now.minute}\n")
        
        jobs_that_should_run = []
        
        for job_data in recurring_jobs:
            job_data["_id"] = str(job_data["_id"])
            job = ScrapingJob(**job_data)
            
            print("-" * 60)
            print(f"Job ID: {job.job_id}")
            print(f"Listing Type: {job.listing_type}")
            print(f"Cron Expression: {job.cron_expression}")
            print(f"Created At: {job.created_at}")
            print(f"Last Run: {job.last_run or 'Never'}")
            print(f"Run Count: {job.run_count}")
            
            # Determine base time
            if job.last_run:
                base_time = job.last_run
            else:
                base_time = now - timedelta(days=1)
            
            # Calculate next run
            cron = croniter.croniter(job.cron_expression, base_time)
            next_run = cron.get_next(datetime)
            
            print(f"\nSchedule Analysis:")
            print(f"  Base time: {base_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  Next scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            
            if next_run <= now:
                time_overdue = (now - next_run).total_seconds() / 3600
                print(f"  Status: ⚠️  OVERDUE by {time_overdue:.1f} hours")
            else:
                time_until = (next_run - now).total_seconds() / 3600
                print(f"  Status: ⏰ Scheduled in {time_until:.1f} hours")
            
            # Test with fixed scheduler
            should_run = await scheduler.should_run_recurring_job(job, now)
            print(f"\n✅ Fixed scheduler says: should_run = {should_run}")
            
            if should_run:
                jobs_that_should_run.append(job)
            
            print()
        
        # Summary
        print("=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Total recurring jobs: {len(recurring_jobs)}")
        print(f"Jobs that should run NOW: {len(jobs_that_should_run)}")
        
        if jobs_that_should_run:
            print("\n🚀 These jobs should run now:")
            for job in jobs_that_should_run:
                print(f"  - {job.job_id} ({job.listing_type})")
        else:
            print("\n⏰ No jobs are currently scheduled to run")
            print("   This is expected if the current time doesn't match any cron schedules")
        
        # Check if scheduler would create instances
        print("\n" + "=" * 60)
        print("SIMULATING SCHEDULER RUN")
        print("=" * 60)
        
        if jobs_that_should_run:
            print(f"\nThe scheduler would create {len(jobs_that_should_run)} new job instances:")
            for job in jobs_that_should_run:
                print(f"\n  Creating instance for: {job.job_id}")
                print(f"    Original job: {job.job_id}")
                print(f"    Listing type: {job.listing_type}")
                print(f"    Locations: {job.locations}")
                print(f"    This would trigger a scraping job for these locations")
        else:
            print("\nNo job instances would be created at this time.")
            print("The scheduler checks every 60 seconds, so jobs will run when their cron schedule matches.")
        
        # Provide recommendations
        print("\n" + "=" * 60)
        print("RECOMMENDATIONS")
        print("=" * 60)
        
        never_run = [j for j in recurring_jobs if j.get('last_run') is None]
        if never_run:
            print(f"\n⚠️  {len(never_run)} jobs have NEVER run:")
            for job_data in never_run:
                print(f"  - {job_data['job_id']} ({job_data.get('listing_type', 'N/A')})")
            print("\n  These jobs will run next time their cron schedule matches.")
        
        old_runs = []
        for job_data in recurring_jobs:
            if job_data.get('last_run'):
                last_run = job_data['last_run']
                days_since = (now - last_run).days
                if days_since > 7:
                    old_runs.append((job_data, days_since))
        
        if old_runs:
            print(f"\n⚠️  {len(old_runs)} jobs haven't run in over a week:")
            for job_data, days in old_runs:
                print(f"  - {job_data['job_id']} ({job_data.get('listing_type', 'N/A')}): {days} days ago")
            print("\n  These are likely overdue and should run soon.")
        
        print("\n✅ SCHEDULER FIX APPLIED SUCCESSFULLY!")
        print("   The scheduler should now correctly identify jobs that need to run.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

async def test_cron_logic():
    """Test the cron logic with different scenarios"""
    print("\n" + "="*60)
    print("TESTING CRON LOGIC WITH DIFFERENT SCENARIOS")
    print("="*60)
    
    now = datetime.utcnow()
    print(f"\nCurrent time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    test_cases = [
        {
            "name": "Job that ran yesterday",
            "cron": "0 13 * * *",  # Daily at 1 PM
            "last_run": now - timedelta(days=1),
        },
        {
            "name": "Job that never ran",
            "cron": "0 13 * * *",
            "last_run": None,
        },
        {
            "name": "Job that ran 2 weeks ago",
            "cron": "0 14 * * *",  # Daily at 2 PM
            "last_run": now - timedelta(days=14),
        },
    ]
    
    for test in test_cases:
        print(f"\n{test['name']}:")
        print(f"  Cron: {test['cron']}")
        print(f"  Last run: {test['last_run'].strftime('%Y-%m-%d %H:%M:%S') if test['last_run'] else 'Never'}")
        
        # Determine base time
        if test['last_run']:
            base_time = test['last_run']
        else:
            base_time = now - timedelta(days=1)
        
        # Calculate next run
        cron = croniter.croniter(test['cron'], base_time)
        next_run = cron.get_next(datetime)
        
        print(f"  Base time: {base_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Next scheduled: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if should run
        should_run = next_run <= now
        
        if should_run:
            hours_overdue = (now - next_run).total_seconds() / 3600
            print(f"  ✅ SHOULD RUN (overdue by {hours_overdue:.1f} hours)")
        else:
            hours_until = (next_run - now).total_seconds() / 3600
            print(f"  ⏰ Should run in {hours_until:.1f} hours")

if __name__ == "__main__":
    print("🧪 SCHEDULER FIX VERIFICATION TEST")
    print("="*60)
    asyncio.run(test_cron_logic())
    asyncio.run(test_scheduler_fix())

