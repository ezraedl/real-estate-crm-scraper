#!/usr/bin/env python3
"""
Test the scheduler to diagnose why recurring jobs aren't running
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

async def test_scheduler():
    """Test the scheduler logic"""
    db = Database()
    scheduler = JobScheduler()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Get all recurring jobs
        print("\n" + "="*60)
        print("CHECKING RECURRING JOBS")
        print("="*60)
        
        cursor = db.jobs_collection.find({
            "cron_expression": {"$exists": True}
        })
        
        recurring_jobs = []
        async for job_data in cursor:
            recurring_jobs.append(job_data)
        
        print(f"Found {len(recurring_jobs)} recurring jobs\n")
        
        now = datetime.utcnow()
        print(f"Current time (UTC): {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        for job_data in recurring_jobs:
            print("-" * 60)
            print(f"Job ID: {job_data['job_id']}")
            print(f"Status: {job_data['status']}")
            print(f"Listing Type: {job_data.get('listing_type', 'N/A')}")
            print(f"Cron Expression: {job_data['cron_expression']}")
            print(f"Created At: {job_data['created_at']}")
            print(f"Run Count: {job_data.get('run_count', 0)}")
            print(f"Last Run: {job_data.get('last_run', 'Never')}")
            print(f"Last Run Status: {job_data.get('last_run_status', 'N/A')}")
            print(f"Last Run Job ID: {job_data.get('last_run_job_id', 'N/A')}")
            
            # Test the cron expression
            cron_expr = job_data['cron_expression']
            created_at = job_data['created_at']
            
            print(f"\nTesting cron logic:")
            print(f"  Using created_at as base: {created_at}")
            
            # Current (buggy) logic
            try:
                cron = croniter.croniter(cron_expr, created_at)
                next_run_old = cron.get_next(datetime)
                time_diff_old = (now - next_run_old).total_seconds()
                should_run_old = -60 <= time_diff_old <= 0
                
                print(f"  ❌ OLD LOGIC (buggy):")
                print(f"     Next run after creation: {next_run_old.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"     Time difference: {time_diff_old:.2f} seconds")
                print(f"     Should run: {should_run_old}")
            except Exception as e:
                print(f"  ❌ Error with old logic: {e}")
            
            # Correct logic - start from last_run or a recent time
            try:
                # Start from last_run if available, otherwise from 1 day ago
                base_time = job_data.get('last_run', now - timedelta(days=1))
                cron_new = croniter.croniter(cron_expr, base_time)
                next_run_new = cron_new.get_next(datetime)
                
                # Check if next_run is in the past (meaning we missed it)
                if next_run_new < now:
                    should_run_new = True
                    print(f"  ✅ NEW LOGIC (fixed):")
                    print(f"     Base time: {base_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"     Next scheduled run: {next_run_new.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"     Status: OVERDUE (should run now!)")
                else:
                    should_run_new = False
                    time_until = (next_run_new - now).total_seconds()
                    print(f"  ✅ NEW LOGIC (fixed):")
                    print(f"     Base time: {base_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"     Next scheduled run: {next_run_new.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"     Status: Waiting ({time_until:.2f} seconds until next run)")
                
            except Exception as e:
                print(f"  ❌ Error with new logic: {e}")
            
            print()
        
        # Test the actual scheduler method
        print("\n" + "="*60)
        print("TESTING SCHEDULER.should_run_recurring_job()")
        print("="*60)
        
        for job_data in recurring_jobs:
            job_data["_id"] = str(job_data["_id"])
            job = ScrapingJob(**job_data)
            
            should_run = await scheduler.should_run_recurring_job(job, now)
            print(f"Job {job.job_id}: should_run = {should_run}")
        
        # Check for job instances created by the scheduler
        print("\n" + "="*60)
        print("CHECKING FOR SCHEDULED JOB INSTANCES")
        print("="*60)
        
        # Look for jobs created in the last 2 weeks
        two_weeks_ago = now - timedelta(days=14)
        
        cursor = db.jobs_collection.find({
            "original_job_id": {"$exists": True},
            "created_at": {"$gte": two_weeks_ago}
        }).sort("created_at", -1)
        
        instances = []
        async for job_data in cursor:
            instances.append(job_data)
        
        print(f"Found {len(instances)} job instances created in the last 2 weeks")
        
        if instances:
            print("\nRecent instances:")
            for inst in instances[:10]:  # Show last 10
                print(f"  {inst['job_id']}")
                print(f"    Created: {inst['created_at']}")
                print(f"    Status: {inst['status']}")
                print(f"    Original Job: {inst.get('original_job_id', 'N/A')}")
                print(f"    Properties: {inst.get('properties_scraped', 0)} scraped, {inst.get('properties_saved', 0)} saved")
                print()
        else:
            print("\n⚠️  NO JOB INSTANCES FOUND - Scheduler is not creating new jobs!")
        
        # Check properties scraped in last 2 weeks
        print("\n" + "="*60)
        print("CHECKING PROPERTIES SCRAPED IN LAST 2 WEEKS")
        print("="*60)
        
        recent_props = await db.properties_collection.count_documents({
            "scraped_at": {"$gte": two_weeks_ago}
        })
        
        print(f"Properties scraped in last 2 weeks: {recent_props}")
        
        if recent_props == 0:
            print("⚠️  NO PROPERTIES SCRAPED - Confirming scheduler is NOT running jobs!")
        
        # Aggregate properties by day for the last 2 weeks
        print("\n" + "="*60)
        print("PROPERTIES SCRAPED PER DAY (LAST 14 DAYS)")
        print("="*60)
        
        pipeline = [
            {
                "$match": {
                    "scraped_at": {"$gte": two_weeks_ago}
                }
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$scraped_at"
                        }
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": -1}
            }
        ]
        
        daily_counts = await db.properties_collection.aggregate(pipeline).to_list(length=14)
        
        if daily_counts:
            for day in daily_counts:
                print(f"  {day['_id']}: {day['count']} properties")
        else:
            print("  No properties scraped in the last 14 days")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

async def test_cron_expressions():
    """Test various cron expressions"""
    print("\n" + "="*60)
    print("TESTING CRON EXPRESSIONS")
    print("="*60)
    
    test_cases = [
        ("0 6 * * *", "Daily at 6:00 AM UTC"),  # FOR_SALE
        ("0 7 * * *", "Daily at 7:00 AM UTC"),  # SOLD
        ("0 8 * * *", "Daily at 8:00 AM UTC"),  # PENDING
    ]
    
    now = datetime.utcnow()
    print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
    
    for cron_expr, description in test_cases:
        print(f"Cron: {cron_expr} ({description})")
        
        # Get next 3 run times
        cron = croniter.croniter(cron_expr, now)
        print("  Next 3 runs:")
        for i in range(3):
            next_run = cron.get_next(datetime)
            time_until = (next_run - now).total_seconds() / 3600  # hours
            print(f"    {i+1}. {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {time_until:.1f} hours)")
        print()

if __name__ == "__main__":
    print("🧪 SCHEDULER DIAGNOSTIC TEST")
    print("="*60)
    asyncio.run(test_cron_expressions())
    asyncio.run(test_scheduler())

