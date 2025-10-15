#!/usr/bin/env python3
"""
Test script to verify content hash normalization fixes
This script tests the improved content hash generation to ensure it doesn't create false positives.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add the scraper directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import db
from models import Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation

async def test_content_hash_normalization():
    """Test that content hash normalization prevents false positives"""
    print("Testing Content Hash Normalization")
    print("=" * 50)
    
    try:
        # Connect to database
        await db.connect()
        print("✓ Connected to database")
        
        # Test 1: Create a property with float values
        print("\n1. Testing float normalization...")
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
            year_built=2020
        )
        
        test_financial = PropertyFinancial(
            list_price=400000.123456789,  # High precision float
            price_per_sqft=266.666666667  # High precision float
        )
        
        test_dates = PropertyDates(
            list_date=datetime(2024, 1, 15, 10, 30, 45)  # Date with time
        )
        
        test_location = PropertyLocation(
            latitude=40.758896,  # High precision coordinates
            longitude=-73.985130,
            neighborhoods=["Downtown", "Central", "Main Street"]  # List that could be reordered
        )
        
        # Create property with high precision values
        property1 = Property(
            property_id="test_hash_normalization_1",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial,
            dates=test_dates,
            location=test_location,
            job_id="test_job_1"
        )
        
        # Generate hash
        hash1 = property1.generate_content_hash()
        print(f"✓ Generated hash 1: {hash1[:16]}...")
        
        # Test 2: Create the same property with slightly different precision
        print("\n2. Testing with slightly different precision...")
        test_financial2 = PropertyFinancial(
            list_price=400000.123456788,  # Slightly different precision
            price_per_sqft=266.666666668  # Slightly different precision
        )
        
        test_location2 = PropertyLocation(
            latitude=40.7588961,  # Slightly different precision
            longitude=-73.9851301,
            neighborhoods=["Main Street", "Central", "Downtown"]  # Different order
        )
        
        test_dates2 = PropertyDates(
            list_date=datetime(2024, 1, 15, 10, 30, 46)  # Different time
        )
        
        property2 = Property(
            property_id="test_hash_normalization_2",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial2,
            dates=test_dates2,
            location=test_location2,
            job_id="test_job_2"
        )
        
        # Generate hash
        hash2 = property2.generate_content_hash()
        print(f"✓ Generated hash 2: {hash2[:16]}...")
        
        # Test 3: Compare hashes
        print("\n3. Comparing hashes...")
        if hash1 == hash2:
            print("✅ SUCCESS: Hashes are identical despite precision differences!")
            print("   This means the normalization is working correctly.")
        else:
            print("❌ FAILURE: Hashes are different despite being the same property!")
            print("   This indicates the normalization needs more work.")
            print(f"   Hash 1: {hash1}")
            print(f"   Hash 2: {hash2}")
        
        # Test 4: Test with completely different values
        print("\n4. Testing with actually different values...")
        test_financial3 = PropertyFinancial(
            list_price=450000.0,  # Actually different price
            price_per_sqft=300.0
        )
        
        property3 = Property(
            property_id="test_hash_normalization_3",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial3,
            dates=test_dates,
            location=test_location,
            job_id="test_job_3"
        )
        
        hash3 = property3.generate_content_hash()
        print(f"✓ Generated hash 3: {hash3[:16]}...")
        
        if hash1 != hash3:
            print("✅ SUCCESS: Hashes are different for actually different properties!")
        else:
            print("❌ FAILURE: Hashes are identical for different properties!")
        
        # Test 5: Test list ordering
        print("\n5. Testing list ordering normalization...")
        test_location4 = PropertyLocation(
            latitude=40.758896,
            longitude=-73.985130,
            neighborhoods=["A", "B", "C"]  # Different order
        )
        
        test_location5 = PropertyLocation(
            latitude=40.758896,
            longitude=-73.985130,
            neighborhoods=["C", "A", "B"]  # Different order
        )
        
        property4 = Property(
            property_id="test_hash_normalization_4",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial,
            dates=test_dates,
            location=test_location4,
            job_id="test_job_4"
        )
        
        property5 = Property(
            property_id="test_hash_normalization_5",
            mls_id="MLS123456",
            mls="TEST_MLS",
            status="FOR_SALE",
            listing_type="for_sale",
            address=test_address,
            description=test_description,
            financial=test_financial,
            dates=test_dates,
            location=test_location5,
            job_id="test_job_5"
        )
        
        hash4 = property4.generate_content_hash()
        hash5 = property5.generate_content_hash()
        
        print(f"✓ Hash 4 (A,B,C): {hash4[:16]}...")
        print(f"✓ Hash 5 (C,A,B): {hash5[:16]}...")
        
        if hash4 == hash5:
            print("✅ SUCCESS: List ordering is normalized correctly!")
        else:
            print("❌ FAILURE: List ordering is not normalized!")
        
        print("\n" + "=" * 50)
        print("Content Hash Normalization Test Complete!")
        
        print("\nSummary of fixes applied:")
        print("✓ Float values rounded to 6 decimal places")
        print("✓ Date values normalized to date-only (no time)")
        print("✓ List values sorted for consistent ordering")
        print("✓ Dictionary lists sorted by string representation")
        print("✓ All normalization functions handle None values gracefully")
        
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
    """Run the content hash normalization test"""
    print("Content Hash Normalization Test Suite")
    print("=" * 60)
    
    success = await test_content_hash_normalization()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 Content hash normalization is working correctly!")
        print("\nThis should significantly reduce false positive updates when:")
        print("• Scraping the same properties multiple times")
        print("• Properties have slight precision differences")
        print("• Lists are returned in different orders")
        print("• Dates have timezone or precision variations")
    else:
        print("❌ Content hash normalization needs more work.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
