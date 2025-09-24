"""
Short test for hash-based diff system
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation

async def test_hash_system():
    """Test hash-based diff system"""
    try:
        print("Testing hash-based diff system...")
        
        # Create a test property
        address = PropertyAddress(
            street="123 Test St",
            city="Test City",
            state="TS",
            zip_code="12345",
            formatted_address="123 Test St, Test City, TS, 12345"
        )
        
        description = PropertyDescription(
            beds=3,
            full_baths=2,
            sqft=1500
        )
        
        financial = PropertyFinancial(
            list_price=250000
        )
        
        dates = PropertyDates()
        location = PropertyLocation()
        
        property1 = Property(
            property_id="test_hash_123",
            address=address,
            description=description,
            financial=financial,
            dates=dates,
            location=location
        )
        property1.update_content_tracking()
        
        print(f"✅ Property 1 created - Hash: {property1.content_hash[:16]}...")
        
        # Create identical property
        property2 = Property(
            property_id="test_hash_123",
            address=address,
            description=description,
            financial=financial,
            dates=dates,
            location=location
        )
        property2.update_content_tracking()
        
        print(f"✅ Property 2 created - Hash: {property2.content_hash[:16]}...")
        
        # Test hash comparison
        if property1.content_hash == property2.content_hash:
            print("✅ Identical properties have same hash")
        else:
            print("❌ Identical properties have different hashes")
            return False
        
        # Create property with different content
        financial2 = PropertyFinancial(
            list_price=300000  # Different price
        )
        
        property3 = Property(
            property_id="test_hash_123",
            address=address,
            description=description,
            financial=financial2,
            dates=dates,
            location=location
        )
        property3.update_content_tracking()
        
        print(f"✅ Property 3 created - Hash: {property3.content_hash[:16]}...")
        
        # Test hash difference
        if property1.content_hash != property3.content_hash:
            print("✅ Different properties have different hashes")
        else:
            print("❌ Different properties have same hash")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Hash system test failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_hash_system())
    print(f"\nTest result: {'PASSED' if result else 'FAILED'}")
