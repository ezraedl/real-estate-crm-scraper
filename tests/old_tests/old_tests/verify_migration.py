#!/usr/bin/env python3
"""Verify the migration was successful"""

import asyncio
from database import Database

async def verify():
    db = Database()
    await db.connect()
    
    jobs = await db.get_all_scheduled_jobs()
    
    print(f"\n✅ Found {len(jobs)} scheduled jobs in scheduled_jobs collection\n")
    
    for i, job in enumerate(jobs, 1):
        print(f"{i}. {job.name}")
        print(f"   ID: {job.scheduled_job_id}")
        print(f"   Status: {job.status}")
        print(f"   Cron: {job.cron_expression}")
        print(f"   Locations: {', '.join(job.locations)}")
        print(f"   Listing Type: {job.listing_type}")
        print(f"   Last run: {job.last_run_at or 'Never'}")
        print(f"   Next run: {job.next_run_at or 'Not calculated yet'}")
        print(f"   Run count: {job.run_count}")
        print()
    
    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(verify())

