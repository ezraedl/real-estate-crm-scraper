#!/usr/bin/env python3
"""
Check specifically for recurring jobs with valid cron expressions
"""

import asyncio
from datetime import datetime
from database import Database

async def check_recurring_jobs():
    """Check for recurring jobs"""
    db = Database()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Query for jobs with non-null cron expressions
        print("\n" + "="*60)
        print("LOOKING FOR JOBS WITH ACTUAL CRON EXPRESSIONS")
        print("="*60)
        
        cursor = db.jobs_collection.find({
            "cron_expression": {"$ne": None, "$exists": True}
        })
        
        recurring_jobs = []
        async for job_data in cursor:
            recurring_jobs.append(job_data)
        
        print(f"\nFound {len(recurring_jobs)} jobs with actual cron expressions\n")
        
        for job_data in recurring_jobs:
            print("-" * 60)
            print(f"Job ID: {job_data['job_id']}")
            print(f"Cron Expression: {job_data['cron_expression']}")
            print(f"Status: {job_data['status']}")
            print(f"Listing Type: {job_data.get('listing_type', 'N/A')}")
            print(f"Locations: {job_data['locations']}")
            print(f"Created At: {job_data['created_at']}")
            print(f"Run Count: {job_data.get('run_count', 0)}")
            print(f"Last Run: {job_data.get('last_run', 'Never')}")
            print(f"Last Run Status: {job_data.get('last_run_status', 'N/A')}")
            print(f"Last Run Job ID: {job_data.get('last_run_job_id', 'N/A')}")
            print()
        
        # Check if scheduler is actually running
        print("\n" + "="*60)
        print("CHECKING IF SCHEDULER IS RUNNING")
        print("="*60)
        
        # Look for evidence that scheduler has been running
        # by checking for recent job instances
        two_weeks_ago = datetime.utcnow().replace(day=1)  # Start of month
        
        cursor = db.jobs_collection.find({
            "original_job_id": {"$exists": True, "$ne": None},
            "created_at": {"$gte": two_weeks_ago}
        }).sort("created_at", -1).limit(20)
        
        instances = []
        async for job_data in cursor:
            instances.append(job_data)
        
        if instances:
            print(f"Found {len(instances)} job instances created since {two_weeks_ago}")
            print("\nRecent instances:")
            for inst in instances:
                print(f"  {inst['job_id']} - Created: {inst['created_at']} - Status: {inst['status']}")
        else:
            print(f"❌ NO JOB INSTANCES found since {two_weeks_ago}")
            print("   This confirms the scheduler is NOT creating new jobs!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(check_recurring_jobs())

