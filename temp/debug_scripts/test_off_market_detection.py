"""
Test script for off-market detection feature.

This script tests the off-market detection by:
1. Finding properties in the database that are for_sale or pending
2. Running a scrape job that won't find them (simulating they went missing)
3. Verifying the off-market detection checks them and updates their status
"""

import asyncio
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import db
from scraper import scraper
from models import ScrapingJob, JobPriority, JobStatus
from homeharvest import scrape_property

# Test configuration
TEST_LOCATION = "Indianapolis, IN"
TEST_CITY = "Indianapolis"
TEST_STATE = "IN"

async def test_off_market_detection():
    """Test the off-market detection feature"""
    print("="*80)
    print("Off-Market Detection Test")
    print("="*80)
    
    # Connect to database
    print("\n[STEP 1] Connecting to database...")
    await db.connect()
    print("[OK] Connected to database")
    
    # Step 1: Find existing for_sale/pending properties in Indianapolis
    print(f"\n[STEP 2] Finding existing for_sale/pending properties in {TEST_LOCATION}...")
    print("   NOTE: The actual implementation checks up to 10,000 properties.")
    print("   This test uses limit=10 for quick testing.")
    existing_properties = await db.find_properties_by_city_state(
        city=TEST_CITY,
        state=TEST_STATE,
        listing_types=['for_sale', 'pending'],
        limit=10  # Just get a few for testing (actual implementation uses 10000)
    )
    
    if not existing_properties:
        print("[WARN] No existing properties found. Creating a test scenario...")
        print("   You may need to run a scrape first to populate some properties.")
        return
    
    print(f"[OK] Found {len(existing_properties)} existing properties")
    
    # Show some examples
    print("\n[STEP 3] Sample properties found:")
    for i, prop in enumerate(existing_properties[:3], 1):
        address = prop.address.formatted_address if prop.address else "N/A"
        status = prop.status or "N/A"
        listing_type = prop.listing_type or "N/A"
        print(f"   {i}. {address}")
        print(f"      Status: {status}, Listing Type: {listing_type}, Property ID: {prop.property_id}")
    
    # Step 2: Create a set of property IDs that we'll "pretend" were found in scrape
    # We'll exclude some properties to simulate they went missing
    print(f"\n[STEP 4] Simulating scrape scenario...")
    
    # Exclude first 3 properties (simulate they went missing)
    found_property_ids = {prop.property_id for prop in existing_properties[3:]}
    missing_property_ids = {prop.property_id for prop in existing_properties[:3]}
    
    print(f"   Total properties: {len(existing_properties)}")
    print(f"   Properties 'found' in scrape: {len(found_property_ids)}")
    print(f"   Properties 'missing' (will be checked): {len(missing_property_ids)}")
    
    if not missing_property_ids:
        print("   [WARN] No missing properties to test with. Need at least 1 property to test.")
        return
    
    # Show which properties will be checked
    print("\n[STEP 5] Properties that will be checked for off-market status:")
    for prop in existing_properties[:3]:
        address = prop.address.formatted_address if prop.address else "N/A"
        print(f"   - {address} (ID: {prop.property_id})")
    
    # Step 3: Test the off-market detection directly
    print(f"\n[STEP 6] Running off-market detection...")
    print("   This will query each missing property by address to check if it's off-market.")
    
    try:
        # Create a dummy job for testing
        test_job = ScrapingJob(
            job_id=f"test_off_market_{int(datetime.utcnow().timestamp())}",
            priority=JobPriority.NORMAL,
            locations=[TEST_LOCATION],
            listing_types=['for_sale', 'pending']
        )
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(test_job)
        
        # Run off-market detection
        result = await scraper.check_missing_properties_for_off_market(
            location=TEST_LOCATION,
            listing_types_scraped=['for_sale', 'pending'],
            found_property_ids=found_property_ids,
            proxy_config=proxy_config,
            cancel_flag={"cancelled": False},
            batch_size=50,
            progress_logs=None,
            job_id=None
        )
        
        print("\n[STEP 7] Off-market detection results:")
        print(f"   Total missing properties: {result.get('missing_total', 0)}")
        print(f"   Properties checked: {result.get('missing_checked', 0)}")
        print(f"   Off-market found: {result.get('off_market_found', 0)}")
        print(f"   Errors: {result.get('errors', 0)}")
        print(f"   Batches processed: {result.get('batches_processed', 0)}")
        
        if result.get('off_market_found', 0) > 0:
            print("\n[SUCCESS] Off-market properties were detected and updated!")
        else:
            print("\n[OK] Test completed: No off-market properties found (they may still be active)")
        
        # Step 4: Verify the updates in database
        print("\n[STEP 8] Verifying database updates...")
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
    
    print("\n" + "="*80)
    print("Test Complete")
    print("="*80)

async def test_direct_address_query():
    """Test querying a specific property by address"""
    print("\n" + "="*80)
    print("Direct Address Query Test")
    print("="*80)
    
    # Use the test address we know is off-market
    test_address = "4509 E 10th St, Indianapolis, IN 46201"
    
    print(f"\n[TEST] Querying property by address: {test_address}")
    
    try:
        queried_property = await scraper.query_property_by_address(test_address, None)
        
        if queried_property:
            print(f"\n[OK] Property found:")
            print(f"   Address: {queried_property.address.formatted_address if queried_property.address else 'N/A'}")
            print(f"   Status: {queried_property.status}")
            print(f"   MLS Status: {queried_property.mls_status}")
            
            is_off_market = scraper.is_off_market_status(queried_property.status, queried_property.mls_status)
            print(f"   Is Off-Market: {is_off_market}")
            
            if is_off_market:
                print("   [OK] Off-market detection working correctly!")
        else:
            print("   [NO] Property not found")
    except Exception as e:
        print(f"   [ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting off-market detection tests...")
    
    # Run tests
    asyncio.run(test_direct_address_query())
    asyncio.run(test_off_market_detection())

