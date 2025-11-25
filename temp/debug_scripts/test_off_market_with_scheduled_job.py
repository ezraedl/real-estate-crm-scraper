"""
Test script for off-market detection using scheduled_job_id and last_scraped.

This script tests:
1. Properties are tagged with scheduled_job_id and last_scraped when scraped
2. Off-market detection queries properties by scheduled_job_id and job_start_time
3. Properties with last_scraped < job_start_time are checked
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import db
from scraper import scraper
from models import ScrapingJob, JobPriority, JobStatus, ScheduledJob, ScheduledJobStatus

# Test configuration
TEST_SCHEDULED_JOB_ID = "test_off_market_detection"
TEST_LOCATION = "Indianapolis, IN"

async def test_scheduled_job_id_tracking():
    """Test that properties get scheduled_job_id and last_scraped when scraped"""
    print("="*80)
    print("Test 1: Scheduled Job ID and Last Scraped Tracking")
    print("="*80)
    
    # Connect to database
    print("\n[STEP 1] Connecting to database...")
    await db.connect()
    print("[OK] Connected to database")
    
    # Check if test scheduled job exists
    print(f"\n[STEP 2] Checking for scheduled job: {TEST_SCHEDULED_JOB_ID}")
    scheduled_job = await db.get_scheduled_job(TEST_SCHEDULED_JOB_ID)
    
    if not scheduled_job:
        print(f"[INFO] Creating test scheduled job: {TEST_SCHEDULED_JOB_ID}")
        # Create a test scheduled job
        test_scheduled_job = ScheduledJob(
            scheduled_job_id=TEST_SCHEDULED_JOB_ID,
            name="Test Off-Market Detection",
            description="Test job for off-market detection",
            status=ScheduledJobStatus.ACTIVE,
            cron_expression="0 0 * * *",  # Daily at midnight
            locations=[TEST_LOCATION],
            listing_types=["for_sale", "pending"]
        )
        await db.create_scheduled_job(test_scheduled_job)
        print("[OK] Test scheduled job created")
    else:
        print(f"[OK] Scheduled job exists: {scheduled_job.name}")
    
    # Find existing properties with this scheduled_job_id
    print(f"\n[STEP 3] Finding properties with scheduled_job_id={TEST_SCHEDULED_JOB_ID}...")
    query = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]}
    }
    
    cursor = db.properties_collection.find(query).limit(10)
    existing_properties = []
    async for prop_data in cursor:
        existing_properties.append(prop_data)
    
    if existing_properties:
        print(f"[OK] Found {len(existing_properties)} existing properties")
        for i, prop in enumerate(existing_properties[:3], 1):
            address = prop.get('address', {}).get('formatted_address', 'N/A')
            scheduled_job_id = prop.get('scheduled_job_id', 'N/A')
            last_scraped = prop.get('last_scraped', 'N/A')
            print(f"   {i}. {address}")
            print(f"      scheduled_job_id: {scheduled_job_id}")
            print(f"      last_scraped: {last_scraped}")
    else:
        print("[INFO] No existing properties found. Properties will be tagged on next scrape.")
        print("       Run a scrape job with scheduled_job_id to populate test data.")
    
    return existing_properties

async def test_off_market_detection():
    """Test off-market detection using scheduled_job_id"""
    print("\n" + "="*80)
    print("Test 2: Off-Market Detection with Scheduled Job ID")
    print("="*80)
    
    # Connect to database
    await db.connect()
    
    # Get job start time (simulate job starting now)
    job_start_time = datetime.utcnow()
    print(f"\n[STEP 1] Job start time: {job_start_time.isoformat()}")
    
    # Find properties that should be checked (last_scraped < job_start_time)
    print(f"\n[STEP 2] Finding properties to check...")
    query = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]},
        "$or": [
            {"last_scraped": {"$lt": job_start_time}},
            {"last_scraped": None},
            {"last_scraped": {"$exists": False}}
        ]
    }
    
    cursor = db.properties_collection.find(query).limit(10)
    properties_to_check = []
    async for prop_data in cursor:
        properties_to_check.append(prop_data)
    
    if not properties_to_check:
        print("[INFO] No properties found to check.")
        print("       This could mean:")
        print("       1. No properties exist with this scheduled_job_id")
        print("       2. All properties were scraped after job_start_time")
        print("       3. Properties need to be scraped first to get scheduled_job_id")
        return
    
    print(f"[OK] Found {len(properties_to_check)} properties to check")
    
    # Show properties that will be checked
    print("\n[STEP 3] Properties that will be checked:")
    for i, prop in enumerate(properties_to_check[:5], 1):
        address = prop.get('address', {}).get('formatted_address', 'N/A')
        last_scraped = prop.get('last_scraped', 'Never')
        property_id = prop.get('property_id', 'N/A')
        print(f"   {i}. {address}")
        print(f"      Property ID: {property_id}")
        print(f"      Last Scraped: {last_scraped}")
        if last_scraped and isinstance(last_scraped, datetime):
            time_diff = (job_start_time - last_scraped).total_seconds() / 3600
            print(f"      Hours since last scraped: {time_diff:.1f}")
    
    # Simulate found property IDs (some properties were found in current scrape)
    # Exclude first 2 properties to simulate they went missing
    found_property_ids = {prop.get('property_id') for prop in properties_to_check[2:]}
    missing_property_ids = {prop.get('property_id') for prop in properties_to_check[:2]}
    
    print(f"\n[STEP 4] Simulating scrape scenario:")
    print(f"   Total properties: {len(properties_to_check)}")
    print(f"   Properties 'found' in scrape: {len(found_property_ids)}")
    print(f"   Properties 'missing' (will be checked): {len(missing_property_ids)}")
    
    if not missing_property_ids:
        print("[WARN] No missing properties to test with. Need at least 1 property.")
        return
    
    # Test off-market detection
    print(f"\n[STEP 5] Running off-market detection...")
    print("   This will query missing properties by address to check if they're off-market.")
    
    try:
        # Create a dummy job for testing
        test_job = ScrapingJob(
            job_id=f"test_off_market_{int(datetime.utcnow().timestamp())}",
            priority=JobPriority.NORMAL,
            locations=[TEST_LOCATION],
            listing_types=['for_sale', 'pending'],
            scheduled_job_id=TEST_SCHEDULED_JOB_ID
        )
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(test_job)
        
        # Run off-market detection
        result = await scraper.check_missing_properties_for_off_market(
            scheduled_job_id=TEST_SCHEDULED_JOB_ID,
            listing_types_scraped=['for_sale', 'pending'],
            found_property_ids=found_property_ids,
            job_start_time=job_start_time,
            proxy_config=proxy_config,
            cancel_flag={"cancelled": False},
            batch_size=50,
            progress_logs=None,
            job_id=None
        )
        
        print("\n[STEP 6] Off-market detection results:")
        print(f"   Total missing properties: {result.get('missing_total', 0)}")
        print(f"   Properties checked: {result.get('missing_checked', 0)}")
        print(f"   Off-market found: {result.get('off_market_found', 0)}")
        print(f"   Errors: {result.get('errors', 0)}")
        print(f"   Batches processed: {result.get('batches_processed', 0)}")
        
        if result.get('off_market_found', 0) > 0:
            print("\n[SUCCESS] Off-market properties were detected and updated!")
        else:
            print("\n[OK] Test completed: No off-market properties found (they may still be active)")
        
        # Verify database updates
        print("\n[STEP 7] Verifying database updates...")
        for prop_id in missing_property_ids:
            updated_prop = await db.properties_collection.find_one({"property_id": prop_id})
            if updated_prop:
                status = updated_prop.get('status', 'N/A')
                mls_status = updated_prop.get('mls_status', 'N/A')
                address = updated_prop.get('address', {}).get('formatted_address', 'N/A')
                print(f"   Property {prop_id}:")
                print(f"      Address: {address}")
                print(f"      Status: {status}")
                print(f"      MLS Status: {mls_status}")
                if status == 'OFF_MARKET':
                    print(f"      [OK] Status updated to OFF_MARKET")
        
    except Exception as e:
        print(f"\n[ERROR] Error during off-market detection: {e}")
        import traceback
        traceback.print_exc()

async def test_query_by_scheduled_job():
    """Test querying properties by scheduled_job_id and last_scraped"""
    print("\n" + "="*80)
    print("Test 3: Query Properties by Scheduled Job ID")
    print("="*80)
    
    await db.connect()
    
    # Test different scenarios
    job_start_time = datetime.utcnow()
    one_hour_ago = job_start_time - timedelta(hours=1)
    one_day_ago = job_start_time - timedelta(days=1)
    
    print(f"\n[TEST] Job start time: {job_start_time.isoformat()}")
    
    # Test 1: Properties with last_scraped < job_start_time
    print("\n[TEST 1] Properties with last_scraped < job_start_time:")
    query1 = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]},
        "$or": [
            {"last_scraped": {"$lt": job_start_time}},
            {"last_scraped": None},
            {"last_scraped": {"$exists": False}}
        ]
    }
    count1 = await db.properties_collection.count_documents(query1)
    print(f"   Found: {count1} properties")
    
    # Test 2: Properties with last_scraped >= job_start_time (should be 0)
    print("\n[TEST 2] Properties with last_scraped >= job_start_time (should be excluded):")
    query2 = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]},
        "last_scraped": {"$gte": job_start_time}
    }
    count2 = await db.properties_collection.count_documents(query2)
    print(f"   Found: {count2} properties (should be 0 or very few)")
    
    # Test 3: Properties scraped more than 1 day ago
    print("\n[TEST 3] Properties scraped more than 1 day ago:")
    query3 = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]},
        "last_scraped": {"$lt": one_day_ago}
    }
    count3 = await db.properties_collection.count_documents(query3)
    print(f"   Found: {count3} properties")
    
    print("\n[OK] Query tests complete")

if __name__ == "__main__":
    print("Starting off-market detection tests with scheduled_job_id...")
    
    # Run tests
    asyncio.run(test_scheduled_job_id_tracking())
    asyncio.run(test_query_by_scheduled_job())
    asyncio.run(test_off_market_detection())
    
    print("\n" + "="*80)
    print("All Tests Complete")
    print("="*80)
    print("\nNext Steps:")
    print("1. Run a scrape job with scheduled_job_id to populate test data")
    print("2. Properties will be tagged with scheduled_job_id and last_scraped")
    print("3. On next scrape run, off-market detection will check missing properties")
    print("4. Properties with last_scraped < job_start_time will be checked")

