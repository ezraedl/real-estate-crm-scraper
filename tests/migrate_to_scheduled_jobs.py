#!/usr/bin/env python3
"""
Migration script to move existing cron jobs from jobs collection to scheduled_jobs collection.

This script:
1. Finds all existing jobs with cron_expression in the jobs collection
2. Creates corresponding entries in the scheduled_jobs collection
3. Updates existing job instances to reference the new scheduled job
4. Optionally marks old cron jobs as inactive
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import Database
from models import ScheduledJob, ScheduledJobStatus, JobStatus, ListingType, JobPriority
import croniter

async def migrate_cron_jobs_to_scheduled_jobs():
    """Migrate existing cron jobs to the new scheduled_jobs architecture"""
    db = Database()
    
    try:
        await db.connect()
        print("=" * 60)
        print("MIGRATION: Cron Jobs → Scheduled Jobs")
        print("=" * 60)
        print()
        
        # Find all jobs with cron_expression
        cursor = db.jobs_collection.find({
            "cron_expression": {"$ne": None, "$exists": True}
        })
        
        cron_jobs = []
        async for job_data in cursor:
            cron_jobs.append(job_data)
        
        print(f"Found {len(cron_jobs)} jobs with cron expressions")
        print()
        
        if not cron_jobs:
            print("✅ No cron jobs to migrate. Database is already using the new architecture.")
            return
        
        migrated_count = 0
        skipped_count = 0
        
        for job_data in cron_jobs:
            job_id = job_data.get('job_id')
            cron_expr = job_data.get('cron_expression')
            
            print(f"Processing job: {job_id}")
            print(f"  Cron: {cron_expr}")
            print(f"  Status: {job_data.get('status')}")
            print(f"  Listing Type: {job_data.get('listing_type')}")
            
            # Create a friendly name
            listing_type = job_data.get('listing_type', 'unknown')
            locations = job_data.get('locations', [])
            location_str = locations[0] if locations else 'unknown'
            name = f"{listing_type.upper()} - {location_str}"
            description = f"Daily scraping of {listing_type} properties in {', '.join(locations)}"
            
            # Generate scheduled_job_id
            scheduled_job_id = f"scheduled_{listing_type}_{int(datetime.utcnow().timestamp())}"
            
            # Check if this scheduled job already exists
            existing = await db.get_scheduled_job(scheduled_job_id)
            if existing:
                print(f"  ⚠️  Scheduled job already exists, skipping")
                skipped_count += 1
                continue
            
            # Calculate next run time
            try:
                cron = croniter.croniter(cron_expr, datetime.utcnow())
                next_run = cron.get_next(datetime)
            except:
                next_run = None
            
            # Create ScheduledJob
            scheduled_job = ScheduledJob(
                scheduled_job_id=scheduled_job_id,
                name=name,
                description=description,
                status=ScheduledJobStatus.ACTIVE,
                cron_expression=cron_expr,
                timezone="UTC",
                locations=job_data.get('locations', []),
                listing_type=job_data.get('listing_type'),
                property_types=job_data.get('property_types'),
                past_days=job_data.get('past_days'),
                date_from=job_data.get('date_from'),
                date_to=job_data.get('date_to'),
                radius=job_data.get('radius'),
                mls_only=job_data.get('mls_only', False),
                foreclosure=job_data.get('foreclosure', False),
                exclude_pending=job_data.get('exclude_pending', False),
                limit=job_data.get('limit', 10000),
                proxy_config=job_data.get('proxy_config'),
                user_agent=job_data.get('user_agent'),
                request_delay=job_data.get('request_delay', 1.0),
                priority=JobPriority(job_data.get('priority', JobPriority.NORMAL.value)),
                run_count=job_data.get('run_count', 0),
                last_run_at=job_data.get('last_run'),
                last_run_status=JobStatus(job_data.get('last_run_status')) if job_data.get('last_run_status') else None,
                last_run_job_id=job_data.get('last_run_job_id'),
                next_run_at=next_run,
                created_at=job_data.get('created_at', datetime.utcnow()),
                created_by="migration_script"
            )
            
            # Save to scheduled_jobs collection
            try:
                await db.create_scheduled_job(scheduled_job)
                print(f"  ✅ Created scheduled job: {scheduled_job_id}")
                migrated_count += 1
                
                # Find and update any job instances that reference this original job
                cursor = db.jobs_collection.find({
                    "original_job_id": job_id
                })
                
                update_count = 0
                async for instance in cursor:
                    await db.jobs_collection.update_one(
                        {"_id": instance["_id"]},
                        {"$set": {"scheduled_job_id": scheduled_job_id}}
                    )
                    update_count += 1
                
                if update_count > 0:
                    print(f"  📝 Updated {update_count} job instances to reference new scheduled job")
                
                # Mark the original cron job as migrated (add a flag)
                await db.jobs_collection.update_one(
                    {"job_id": job_id},
                    {
                        "$set": {
                            "migrated_to_scheduled_job": scheduled_job_id,
                            "migration_date": datetime.utcnow()
                        }
                    }
                )
                
            except Exception as e:
                print(f"  ❌ Error creating scheduled job: {e}")
                continue
            
            print()
        
        print("=" * 60)
        print("MIGRATION SUMMARY")
        print("=" * 60)
        print(f"Total cron jobs found: {len(cron_jobs)}")
        print(f"Successfully migrated: {migrated_count}")
        print(f"Skipped (already exists): {skipped_count}")
        print()
        
        if migrated_count > 0:
            print("✅ Migration completed successfully!")
            print()
            print("Next steps:")
            print("1. Verify the scheduled jobs in the scheduled_jobs collection")
            print("2. Restart the scheduler service")
            print("3. Monitor that jobs are being created and running")
            print()
            print("To verify:")
            print("  python -c 'import asyncio; from database import Database; db = Database(); asyncio.run(db.connect()); jobs = asyncio.run(db.get_all_scheduled_jobs()); print(f\"Found {len(jobs)} scheduled jobs\")'")
        else:
            print("⚠️  No jobs were migrated. Please check for errors above.")
        
        print()
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Migration error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print("🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    print("🚀 Starting migration of cron jobs to scheduled jobs architecture")
    print()
    asyncio.run(migrate_cron_jobs_to_scheduled_jobs())

