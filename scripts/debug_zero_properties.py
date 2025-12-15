"""
Debug script to identify why scraper returns 0 properties since 12/13/2025.

This script will:
1. Test direct homeharvest calls with different parameters
2. Check scheduled job configurations
3. Test incremental vs full scrape logic
4. Verify date filtering parameters
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from homeharvest import scrape_property
from database import db
from models import ScrapingJob, JobPriority
from scraper import scraper
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_direct_homeharvest():
    """Test direct homeharvest calls with different parameters"""
    print("\n" + "="*80)
    print("TEST 1: Direct HomeHarvest API Calls")
    print("="*80)
    
    test_location = "Indianapolis, IN"  # Change to your test location
    test_cases = [
        {
            "name": "Full scrape (no date filters)",
            "params": {
                "location": test_location,
                "listing_type": ["for_sale"],
                "limit": 100
            }
        },
        {
            "name": "With past_days=90",
            "params": {
                "location": test_location,
                "listing_type": ["for_sale"],
                "past_days": 90,
                "limit": 100
            }
        },
        {
            "name": "With date_from (30 days ago)",
            "params": {
                "location": test_location,
                "listing_type": ["for_sale"],
                "date_from": datetime.utcnow() - timedelta(days=30),
                "limit": 100
            }
        },
        {
            "name": "With updated_in_past_hours=24",
            "params": {
                "location": test_location,
                "listing_type": ["for_sale"],
                "updated_in_past_hours": 24,
                "limit": 100
            }
        },
        {
            "name": "With updated_in_past_hours=1 (very restrictive)",
            "params": {
                "location": test_location,
                "listing_type": ["for_sale"],
                "updated_in_past_hours": 1,
                "limit": 100
            }
        }
    ]
    
    for test_case in test_cases:
        print(f"\n--- {test_case['name']} ---")
        try:
            params = test_case['params'].copy()
            log_params = {k: v for k, v in params.items() if k != "proxy"}
            print(f"Parameters: {log_params}")
            
            df = scrape_property(**params)
            num_properties = len(df) if df is not None and not df.empty else 0
            print(f"Result: {num_properties} properties")
            
            if num_properties > 0:
                print(f"  ✅ SUCCESS: Got {num_properties} properties")
                # Show sample columns
                print(f"  Columns: {list(df.columns)[:10]}")
                if 'status' in df.columns:
                    print(f"  Status values: {df['status'].value_counts().head().to_dict()}")
            else:
                print(f"  ⚠️  WARNING: Got 0 properties")
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()

async def check_scheduled_jobs():
    """Check scheduled job configurations and last_run_at values"""
    print("\n" + "="*80)
    print("TEST 2: Scheduled Job Configurations")
    print("="*80)
    
    try:
        scheduled_jobs = await db.get_active_scheduled_jobs()
        print(f"\nFound {len(scheduled_jobs)} active scheduled jobs:")
        
        for sj in scheduled_jobs:
            print(f"\n  Scheduled Job ID: {sj.scheduled_job_id}")
            print(f"    Name: {sj.name}")
            print(f"    Status: {sj.status}")
            print(f"    Last run: {sj.last_run_at}")
            print(f"    Next run: {sj.next_run_at}")
            print(f"    Incremental runs before full: {sj.incremental_runs_before_full}")
            print(f"    Incremental runs count: {sj.incremental_runs_count}")
            print(f"    Locations: {sj.locations[:3] if sj.locations else 'None'}...")
            
            if sj.last_run_at:
                time_since_last_run = datetime.utcnow() - sj.last_run_at
                hours_since = time_since_last_run.total_seconds() / 3600
                print(f"    Time since last run: {hours_since:.1f} hours ({hours_since/24:.1f} days)")
                
                # Check what parameters would be used
                if 0 < hours_since <= (30 * 24):
                    updated_hours = max(1, int(hours_since) + 1)
                    print(f"    ⚠️  Would use updated_in_past_hours={updated_hours} (very restrictive!)")
                elif hours_since > (30 * 24):
                    print(f"    ⚠️  Would use date_from={sj.last_run_at} (might be too old)")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

async def test_scraper_method():
    """Test the scraper's _scrape_all_listing_types method"""
    print("\n" + "="*80)
    print("TEST 3: Scraper Method Test")
    print("="*80)
    
    try:
        # Create a test job
        test_job = ScrapingJob(
            job_id="test_debug_job",
            priority=JobPriority.NORMAL,
            locations=["Indianapolis, IN"],  # Change to your test location
            listing_types=["for_sale"],
            past_days=90,
            limit=100
        )
        
        print(f"\nTest job created:")
        print(f"  Locations: {test_job.locations}")
        print(f"  Listing types: {test_job.listing_types}")
        print(f"  Past days: {test_job.past_days}")
        print(f"  Limit: {test_job.limit}")
        print(f"  Scheduled job ID: {test_job.scheduled_job_id}")
        
        # Test without scheduled_job_id (should use past_days)
        print(f"\n--- Testing without scheduled_job_id (should use past_days) ---")
        properties_by_type = await scraper._scrape_all_listing_types(
            location=test_job.locations[0],
            job=test_job,
            proxy_config=None,
            listing_types=test_job.listing_types,
            limit=test_job.limit,
            past_days=test_job.past_days
        )
        
        for listing_type, properties in properties_by_type.items():
            print(f"  {listing_type}: {len(properties)} properties")
        
        # Test with a scheduled_job_id that has last_run_at set to the problematic date
        print(f"\n--- Testing with scheduled_job_id (incremental update) ---")
        # First, let's check if there are any scheduled jobs
        scheduled_jobs = await db.get_active_scheduled_jobs()
        if scheduled_jobs:
            test_scheduled_job = scheduled_jobs[0]
            test_job.scheduled_job_id = test_scheduled_job.scheduled_job_id
            
            print(f"  Using scheduled job: {test_scheduled_job.scheduled_job_id}")
            print(f"  Last run: {test_scheduled_job.last_run_at}")
            
            properties_by_type = await scraper._scrape_all_listing_types(
                location=test_job.locations[0],
                job=test_job,
                proxy_config=None,
                listing_types=test_job.listing_types,
                limit=test_job.limit,
                past_days=test_job.past_days
            )
            
            for listing_type, properties in properties_by_type.items():
                print(f"  {listing_type}: {len(properties)} properties")
        else:
            print("  No scheduled jobs found to test with")
            
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

async def check_recent_jobs():
    """Check recent job results to see when they started returning 0"""
    print("\n" + "="*80)
    print("TEST 4: Recent Job Results")
    print("="*80)
    
    try:
        # Get recent jobs
        cutoff_date = datetime(2025, 12, 13, 5, 59, 52)  # 12/13/2025 7:59:52 AM Israel time = ~5:59:52 UTC
        print(f"\nChecking jobs since {cutoff_date} (12/13/2025 7:59:52 AM Israel time)")
        
        cursor = db.jobs_collection.find({
            "created_at": {"$gte": cutoff_date}
        }).sort("created_at", -1).limit(10)
        
        jobs = []
        async for job_data in cursor:
            jobs.append(job_data)
        
        print(f"\nFound {len(jobs)} recent jobs:")
        for job_data in jobs:
            job_id = job_data.get("job_id", "unknown")
            created_at = job_data.get("created_at")
            status = job_data.get("status")
            properties_scraped = job_data.get("properties_scraped", 0)
            properties_saved = job_data.get("properties_saved", 0)
            scheduled_job_id = job_data.get("scheduled_job_id")
            last_run_at = None
            
            if scheduled_job_id:
                scheduled_job = await db.get_scheduled_job(scheduled_job_id)
                if scheduled_job:
                    last_run_at = scheduled_job.last_run_at
            
            print(f"\n  Job: {job_id}")
            print(f"    Created: {created_at}")
            print(f"    Status: {status}")
            print(f"    Properties scraped: {properties_scraped}")
            print(f"    Properties saved: {properties_saved}")
            print(f"    Scheduled job ID: {scheduled_job_id}")
            if last_run_at:
                print(f"    Scheduled job last_run_at: {last_run_at}")
                time_since = (created_at - last_run_at).total_seconds() / 3600 if created_at else 0
                print(f"    Hours since last_run_at: {time_since:.1f}")
                
    except Exception as e:
        print(f"  ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Run all diagnostic tests"""
    print("\n" + "="*80)
    print("SCRAPER DIAGNOSTIC - Zero Properties Issue")
    print("="*80)
    print(f"Current time: {datetime.utcnow()}")
    print(f"Testing location: Indianapolis, IN (change in script if needed)")
    
    # Initialize database connection
    try:
        await db.connect()
        print("✅ Database connected")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Run tests
    await test_direct_homeharvest()
    await check_scheduled_jobs()
    await check_recent_jobs()
    await test_scraper_method()
    
    print("\n" + "="*80)
    print("DIAGNOSTIC COMPLETE")
    print("="*80)
    print("\nCheck the output above to identify the issue:")
    print("1. If direct homeharvest calls work → issue is in scraper logic")
    print("2. If updated_in_past_hours is too restrictive → incremental update issue")
    print("3. If date_from is too old → scheduled job last_run_at issue")
    print("4. If all tests return 0 → homeharvest API or proxy issue")

if __name__ == "__main__":
    asyncio.run(main())

