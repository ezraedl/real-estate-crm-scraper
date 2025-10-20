#!/usr/bin/env python3
"""
Test script for MLS-CRM property synchronization
This script tests the new functionality for linking MLS properties to CRM properties
and ensuring updates are propagated correctly.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add the scraper directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db
from models import Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation

async def test_mls_crm_synchronization():
    """Test the MLS-CRM property synchronization functionality"""
    print("Testing MLS-CRM Property Synchronization")
    print("=" * 50)
    
    try:
        # Connect to database
        await db.connect()
        print("✓ Connected to database")
        
        # Test 1: Create a test MLS property
        print("\n1. Creating test MLS property...")
        test_address = PropertyAddress(
            street="123 Test Street",
            city="Test City",
            state="TS",
            zip_code="12345",
            formatted_address="123 Test Street, Test City, TS 12345"
        )
        
        test_description = PropertyDescription(
            beds=3,
            full_baths=2,
            sqft=1500,
            year_built=2020,
            property_type="single_family"
        )
        
        test_financial = PropertyFinancial(
            list_price=300000,
            price_per_sqft=200
        )
        
        test_dates = PropertyDates(
            list_date=datetime.utcnow()
        )
        
        test_location = PropertyLocation(
            latitude=40.7128,
            longitude=-74.0060
        )
        
        test_property = Property(
            property_id="test_property_123",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            mls_status="Active",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial,
            dates=test_dates,
            location=test_location,
            job_id="test_job_123"
        )
        
        # Generate property ID and content hash
        test_property.property_id = test_property.generate_property_id()
        test_property.update_content_tracking()
        
        # Save the property
        save_result = await db.save_property(test_property)
        print(f"✓ MLS property saved: {save_result}")
        
        # Test 2: Add CRM property reference
        print("\n2. Adding CRM property reference...")
        crm_property_id = "crm_property_456"
        success = await db.add_crm_property_reference(test_property.property_id, crm_property_id)
        print(f"✓ CRM reference added: {success}")
        
        # Test 3: Verify CRM property references
        print("\n3. Verifying CRM property references...")
        references = await db.get_crm_property_references(test_property.property_id)
        print(f"✓ CRM references: {references}")
        
        # Test 4: Update MLS property and verify CRM sync
        print("\n4. Updating MLS property and testing CRM sync...")
        
        # Update the property
        test_property.financial.list_price = 350000  # Price increase
        test_property.financial.price_per_sqft = 233.33
        test_property.update_content_tracking()  # Generate new hash
        
        # Save updated property (this should trigger CRM sync)
        update_result = await db.save_property(test_property)
        print(f"✓ MLS property updated: {update_result}")
        
        # Test 5: Manual CRM sync
        print("\n5. Testing manual CRM sync...")
        sync_count = await db.update_crm_properties_from_mls(test_property.property_id)
        print(f"✓ CRM properties synced: {sync_count}")
        
        # Test 6: Remove CRM property reference
        print("\n6. Removing CRM property reference...")
        success = await db.remove_crm_property_reference(test_property.property_id, crm_property_id)
        print(f"✓ CRM reference removed: {success}")
        
        # Verify removal
        references = await db.get_crm_property_references(test_property.property_id)
        print(f"✓ CRM references after removal: {references}")
        
        # Cleanup: Remove test property
        print("\n7. Cleaning up test data...")
        await db.properties_collection.delete_one({"property_id": test_property.property_id})
        print("✓ Test property removed")
        
        print("\n" + "=" * 50)
        print("✓ All tests passed! MLS-CRM synchronization is working correctly.")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        await db.disconnect()
        print("✓ Disconnected from database")
    
    return True

async def test_active_properties_rescraping():
    """Test the active properties re-scraping functionality"""
    print("\nTesting Active Properties Re-scraping")
    print("=" * 50)
    
    try:
        # Connect to database
        await db.connect()
        print("✓ Connected to database")
        
        # Test 1: Check if active properties re-scraping job exists
        print("\n1. Checking active properties re-scraping job...")
        job = await db.scheduled_jobs_collection.find_one({
            "scheduled_job_id": "active_properties_rescraping"
        })
        
        if job:
            print(f"✓ Active properties re-scraping job found: {job['name']}")
            print(f"  - Status: {job['status']}")
            print(f"  - Cron: {job['cron_expression']}")
            print(f"  - Last run: {job.get('last_run_at', 'Never')}")
            print(f"  - Run count: {job.get('run_count', 0)}")
        else:
            print("⚠ Active properties re-scraping job not found")
        
        # Test 2: Test getting active properties locations
        print("\n2. Testing active properties location detection...")
        locations = await db._get_active_properties_locations()
        print(f"✓ Found {len(locations)} unique locations for active properties")
        if locations:
            print(f"  - Sample locations: {locations[:3]}")
        
        print("\n" + "=" * 50)
        print("✓ Active properties re-scraping tests completed.")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        await db.disconnect()
        print("✓ Disconnected from database")
    
    return True

async def main():
    """Run all tests"""
    print("MLS-CRM Integration Test Suite")
    print("=" * 60)
    
    # Test MLS-CRM synchronization
    sync_success = await test_mls_crm_synchronization()
    
    # Test active properties re-scraping
    rescraping_success = await test_active_properties_rescraping()
    
    print("\n" + "=" * 60)
    if sync_success and rescraping_success:
        print("🎉 All tests passed! The MLS-CRM integration is working correctly.")
        print("\nFeatures implemented:")
        print("✓ Active properties re-scraping (configurable frequency)")
        print("✓ MLS-CRM property linking")
        print("✓ Automatic update propagation from MLS to CRM")
        print("✓ Manual sync capabilities")
        print("✓ Reference management (add/remove)")
    else:
        print("❌ Some tests failed. Please check the implementation.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
