"""
Full flow test for off-market detection.

This script demonstrates the complete flow:
1. Finds existing properties (or you can manually set scheduled_job_id)
2. Simulates a scrape job that finds some properties but not others
3. Tests off-market detection on missing properties
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import db
from scraper import scraper
from models import ScrapingJob, JobPriority

# Test configuration
TEST_SCHEDULED_JOB_ID = "test_off_market_detection"
TEST_LOCATION = "Indianapolis, IN"

async def setup_test_data():
    """Set up test data by finding existing properties and tagging them"""
    print("="*80)
    print("Setting Up Test Data")
    print("="*80)
    
    await db.connect()
    
    # Find some existing for_sale/pending properties
    print("\n[STEP 1] Finding existing for_sale/pending properties...")
    query = {
        "listing_type": {"$in": ["for_sale", "pending"]},
        "address.city": {"$regex": "Indianapolis", "$options": "i"},
        "address.state": "IN"
    }
    
    cursor = db.properties_collection.find(query).limit(10)
    existing_properties = []
    async for prop_data in cursor:
        existing_properties.append(prop_data)
    
    if not existing_properties:
        print("[WARN] No properties found. You may need to run a scrape first.")
        return []
    
    print(f"[OK] Found {len(existing_properties)} properties")
    
    # Tag them with scheduled_job_id and set last_scraped to 2 days ago
    # This simulates they were scraped 2 days ago
    two_days_ago = datetime.utcnow() - timedelta(days=2)
    
    print(f"\n[STEP 2] Tagging properties with scheduled_job_id and last_scraped...")
    print(f"   Setting last_scraped to: {two_days_ago.isoformat()}")
    
    updated_count = 0
    for prop in existing_properties:
        property_id = prop.get('property_id')
        if property_id:
            await db.properties_collection.update_one(
                {"property_id": property_id},
                {
                    "$set": {
                        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
                        "last_scraped": two_days_ago
                    }
                }
            )
            updated_count += 1
    
    print(f"[OK] Tagged {updated_count} properties")
    
    # Show tagged properties
    print("\n[STEP 3] Tagged properties:")
    for i, prop in enumerate(existing_properties[:5], 1):
        address = prop.get('address', {}).get('formatted_address', 'N/A')
        property_id = prop.get('property_id', 'N/A')
        print(f"   {i}. {address}")
        print(f"      Property ID: {property_id}")
    
    return [p.get('property_id') for p in existing_properties]

async def test_off_market_detection_flow():
    """Test the complete off-market detection flow"""
    print("\n" + "="*80)
    print("Testing Off-Market Detection Flow")
    print("="*80)
    
    await db.connect()
    
    # Set up test data
    all_property_ids = await setup_test_data()
    
    if not all_property_ids:
        print("\n[SKIP] No test data available. Cannot proceed with test.")
        return
    
    # Simulate job start time (now)
    job_start_time = datetime.utcnow()
    print(f"\n[STEP 4] Simulating new scrape job starting at: {job_start_time.isoformat()}")
    
    # Simulate that some properties were found in the current scrape
    # Exclude first 3 properties to simulate they went missing
    found_property_ids = set(all_property_ids[3:])
    missing_property_ids = set(all_property_ids[:3])
    
    print(f"\n[STEP 5] Simulating scrape results:")
    print(f"   Total properties in database: {len(all_property_ids)}")
    print(f"   Properties 'found' in current scrape: {len(found_property_ids)}")
    print(f"   Properties 'missing' (will be checked): {len(missing_property_ids)}")
    
    print("\n[STEP 6] Missing properties that will be checked:")
    for prop_id in missing_property_ids:
        prop = await db.properties_collection.find_one({"property_id": prop_id})
        if prop:
            address = prop.get('address', {}).get('formatted_address', 'N/A')
            last_scraped = prop.get('last_scraped', 'Never')
            print(f"   - {address}")
            print(f"     Last scraped: {last_scraped}")
    
    # Verify query will find these properties
    print(f"\n[STEP 7] Verifying query will find missing properties...")
    query = {
        "scheduled_job_id": TEST_SCHEDULED_JOB_ID,
        "listing_type": {"$in": ["for_sale", "pending"]},
        "$or": [
            {"last_scraped": {"$lt": job_start_time}},
            {"last_scraped": None},
            {"last_scraped": {"$exists": False}}
        ]
    }
    
    cursor = db.properties_collection.find(query)
    query_results = []
    async for prop_data in cursor:
        query_results.append(prop_data.get('property_id'))
    
    print(f"   Query found {len(query_results)} properties")
    print(f"   Missing property IDs: {missing_property_ids}")
    print(f"   Query results include missing: {missing_property_ids.issubset(set(query_results))}")
    
    # Run off-market detection
    print(f"\n[STEP 8] Running off-market detection...")
    print("   This will query each missing property by address.")
    
    try:
        test_job = ScrapingJob(
            job_id=f"test_off_market_{int(datetime.utcnow().timestamp())}",
            priority=JobPriority.NORMAL,
            locations=[TEST_LOCATION],
            listing_types=['for_sale', 'pending'],
            scheduled_job_id=TEST_SCHEDULED_JOB_ID
        )
        
        proxy_config = await scraper.get_proxy_config(test_job)
        
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
        
        print("\n[STEP 9] Results:")
        print(f"   Total missing: {result.get('missing_total', 0)}")
        print(f"   Checked: {result.get('missing_checked', 0)}")
        print(f"   Off-market found: {result.get('off_market_found', 0)}")
        print(f"   Errors: {result.get('errors', 0)}")
        print(f"   Batches: {result.get('batches_processed', 0)}")
        
        # Check updated properties
        print("\n[STEP 10] Checking updated properties...")
        for prop_id in missing_property_ids:
            prop = await db.properties_collection.find_one({"property_id": prop_id})
            if prop:
                status = prop.get('status', 'N/A')
                address = prop.get('address', {}).get('formatted_address', 'N/A')
                print(f"   {address}: {status}")
                if status == 'OFF_MARKET':
                    print(f"      [OK] Marked as OFF_MARKET")
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

async def test_with_real_scheduled_job():
    """Test using an actual scheduled job from the database"""
    print("\n" + "="*80)
    print("Test: Using Real Scheduled Job")
    print("="*80)
    
    await db.connect()
    
    # Find an actual scheduled job
    print("\n[STEP 1] Finding scheduled jobs...")
    cursor = db.scheduled_jobs_collection.find({"status": "active"}).limit(5)
    scheduled_jobs = []
    async for job_data in cursor:
        scheduled_jobs.append(job_data)
    
    if not scheduled_jobs:
        print("[INFO] No active scheduled jobs found.")
        print("       Create a scheduled job first, or use the test scheduled job.")
        return
    
    print(f"[OK] Found {len(scheduled_jobs)} scheduled jobs")
    for i, job in enumerate(scheduled_jobs[:3], 1):
        job_id = job.get('scheduled_job_id', 'N/A')
        name = job.get('name', 'N/A')
        print(f"   {i}. {name} (ID: {job_id})")
    
    # Use the first scheduled job
    test_job_id = scheduled_jobs[0].get('scheduled_job_id')
    print(f"\n[STEP 2] Using scheduled job: {test_job_id}")
    
    # Find properties with this scheduled_job_id
    query = {
        "scheduled_job_id": test_job_id,
        "listing_type": {"$in": ["for_sale", "pending"]}
    }
    
    count = await db.properties_collection.count_documents(query)
    print(f"[OK] Found {count} properties with scheduled_job_id={test_job_id}")
    
    if count > 0:
        # Show sample properties
        cursor = db.properties_collection.find(query).limit(3)
        print("\n[STEP 3] Sample properties:")
        async for prop_data in cursor:
            address = prop_data.get('address', {}).get('formatted_address', 'N/A')
            last_scraped = prop_data.get('last_scraped', 'Never')
            print(f"   - {address}")
            print(f"     Last scraped: {last_scraped}")
        
        print("\n[INFO] To test off-market detection:")
        print("   1. Run a scrape job with this scheduled_job_id")
        print("   2. Some properties will be found and get last_scraped updated")
        print("   3. Properties not found will have last_scraped < job_start_time")
        print("   4. Off-market detection will check those missing properties")

if __name__ == "__main__":
    print("Starting full flow test for off-market detection...")
    
    # Run tests
    asyncio.run(test_off_market_detection_flow())
    asyncio.run(test_with_real_scheduled_job())
    
    print("\n" + "="*80)
    print("Testing Complete")
    print("="*80)
    print("\nTo test in production:")
    print("1. Create/use a scheduled job with scheduled_job_id")
    print("2. Run a scrape - properties will get scheduled_job_id and last_scraped")
    print("3. On next scrape run, missing properties will be checked for off-market")
    print("4. Properties with last_scraped < job_start_time will be queried by address")

