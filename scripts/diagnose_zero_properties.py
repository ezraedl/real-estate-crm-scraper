"""
Diagnostic script to investigate why scraping is returning 0 properties.
Checks recent jobs, their parameters, and potential issues.
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import db
from models import JobStatus
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def diagnose_zero_properties():
    """Diagnose why scraping is returning 0 properties"""
    try:
        await db.connect()
        
        # Get jobs from last 2 days
        two_days_ago = datetime.utcnow() - timedelta(days=2)
        
        print("\n" + "="*80)
        print("DIAGNOSING ZERO PROPERTIES ISSUE")
        print("="*80)
        
        # Query recent jobs
        cursor = db.jobs_collection.find({
            "created_at": {"$gte": two_days_ago}
        }).sort("created_at", -1).limit(20)
        
        jobs = []
        async for job_data in cursor:
            jobs.append(job_data)
        
        print(f"\nFound {len(jobs)} jobs in the last 2 days\n")
        
        if not jobs:
            print("[ERROR] No jobs found in the last 2 days!")
            return
        
        # Analyze each job
        zero_property_jobs = []
        for job in jobs:
            job_id = job.get("job_id") or str(job.get("_id", ""))
            status = job.get("status")
            properties_scraped = job.get("properties_scraped", 0)
            properties_saved = job.get("properties_saved", 0)
            created_at = job.get("created_at")
            updated_at = job.get("updated_at")
            
            # Get progress logs
            progress_logs = job.get("progress_logs", {})
            locations = progress_logs.get("locations", [])
            
            print(f"\n{'='*80}")
            print(f"Job ID: {job_id}")
            print(f"Status: {status}")
            print(f"Created: {created_at}")
            print(f"Updated: {updated_at}")
            print(f"Properties Scraped: {properties_scraped}")
            print(f"Properties Saved: {properties_saved}")
            print(f"Locations: {len(locations)}")
            
            # Check job parameters
            scheduled_job_id = job.get("scheduled_job_id")
            listing_types = job.get("listing_types") or [job.get("listing_type")] if job.get("listing_type") else []
            past_days = job.get("past_days")
            limit = job.get("limit")
            locations_list = job.get("locations", [])
            
            print(f"\nJob Parameters:")
            print(f"  - Scheduled Job ID: {scheduled_job_id}")
            print(f"  - Listing Types: {listing_types}")
            print(f"  - Past Days: {past_days}")
            print(f"  - Limit: {limit}")
            print(f"  - Locations: {locations_list[:3]}{'...' if len(locations_list) > 3 else ''}")
            
            # Check if this is an incremental scrape
            if scheduled_job_id:
                try:
                    scheduled_job = await db.get_scheduled_job(scheduled_job_id)
                    if scheduled_job:
                        print(f"\nScheduled Job Info:")
                        print(f"  - Name: {scheduled_job.name}")
                        print(f"  - Last Run At: {scheduled_job.last_run_at}")
                        print(f"  - Incremental Runs Before Full: {scheduled_job.incremental_runs_before_full}")
                        print(f"  - Incremental Runs Count: {scheduled_job.incremental_runs_count}")
                        
                        if scheduled_job.last_run_at:
                            time_since = datetime.utcnow() - scheduled_job.last_run_at
                            hours_since = time_since.total_seconds() / 3600
                            print(f"  - Hours Since Last Run: {hours_since:.2f}")
                            
                            # Check if incremental logic would filter
                            if hours_since <= (30 * 24):
                                updated_in_past_hours = max(1, int(hours_since) + 1)
                                print(f"  [WARNING] INCREMENTAL MODE: Would use updated_in_past_hours={updated_in_past_hours}")
                                print(f"     This might be filtering out all properties!")
                except Exception as e:
                    print(f"  [WARNING] Error fetching scheduled job: {e}")
            
            # Check location results
            if locations:
                print(f"\nLocation Results:")
                for loc in locations[:5]:  # Show first 5
                    loc_name = loc.get("location", "Unknown")
                    loc_status = loc.get("status", "Unknown")
                    listing_types_data = loc.get("listing_types", {})
                    
                    total_found = sum(lt.get("found", 0) for lt in listing_types_data.values())
                    
                    print(f"  - {loc_name}: {loc_status}, Found: {total_found}")
                    for lt, data in listing_types_data.items():
                        found = data.get("found", 0)
                        if found == 0:
                            error = data.get("error")
                            if error:
                                print(f"    ⚠️  {lt}: 0 found - ERROR: {error}")
                            else:
                                print(f"    ⚠️  {lt}: 0 found")
            
            # Check for errors
            error_message = job.get("error_message")
            if error_message:
                print(f"\n[ERROR] {error_message}")
            
            # Check if listing_types is empty - this is a critical issue!
            if not listing_types or len(listing_types) == 0:
                print(f"\n[CRITICAL] Listing types is EMPTY! This job will scrape 0 properties!")
            
            if properties_scraped == 0:
                zero_property_jobs.append({
                    "job_id": job_id,
                    "status": status,
                    "scheduled_job_id": scheduled_job_id,
                    "created_at": created_at
                })
        
        # Summary
        print(f"\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        print(f"Total jobs analyzed: {len(jobs)}")
        print(f"Jobs with 0 properties: {len(zero_property_jobs)}")
        
        if zero_property_jobs:
            print(f"\n[WARNING] Jobs with 0 properties:")
            for zj in zero_property_jobs:
                print(f"  - {zj['job_id']} (Status: {zj['status']}, Scheduled: {zj['scheduled_job_id']})")
            
            print(f"\n[INFO] POTENTIAL ISSUES:")
            print(f"  1. Listing types might be empty (CRITICAL)")
            print(f"  2. Incremental scraping might be filtering too aggressively")
            print(f"  3. HomeHarvest API might be returning empty results")
            print(f"  4. Location format might be incorrect")
            print(f"  5. Proxy/rate limiting issues")
        
        # Check scheduled jobs
        print(f"\n{'='*80}")
        print("ACTIVE SCHEDULED JOBS")
        print(f"{'='*80}")
        scheduled_jobs = await db.get_active_scheduled_jobs()
        for sj in scheduled_jobs:
            print(f"\nScheduled Job: {sj.scheduled_job_id}")
            print(f"  - Name: {sj.name}")
            print(f"  - Status: {sj.status}")
            print(f"  - Cron: {sj.cron_expression}")
            print(f"  - Last Run: {sj.last_run_at}")
            print(f"  - Next Run: {sj.next_run_at}")
            print(f"  - Incremental Config: {sj.incremental_runs_before_full}")
            print(f"  - Incremental Count: {sj.incremental_runs_count}")
            print(f"  - Locations: {len(sj.locations or [])}")
            
            if sj.last_run_at:
                time_since = datetime.utcnow() - sj.last_run_at
                hours_since = time_since.total_seconds() / 3600
                if hours_since <= (30 * 24):
                    updated_in_past_hours = max(1, int(hours_since) + 1)
                    print(f"  [WARNING] Would use updated_in_past_hours={updated_in_past_hours} (hours since last run: {hours_since:.2f})")
        
        await db.disconnect()
        
    except Exception as e:
        logger.error(f"Error during diagnosis: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(diagnose_zero_properties())

