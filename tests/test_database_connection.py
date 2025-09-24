"""
Short test for database connection and basic operations
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import db
from models import Property, PropertyAddress, PropertyDescription, PropertyFinancial, PropertyDates, PropertyLocation

async def test_database_connection():
    """Test database connection and basic operations"""
    try:
        print("Testing database connection...")
        
        # Connect to database
        await db.connect()
        print("✅ Database connected")
        
        # Test creating a simple property
        test_address = PropertyAddress(
            street="123 Test St",
            city="Test City",
            state="TS",
            zip_code="12345",
            formatted_address="123 Test St, Test City, TS, 12345"
        )
        
        test_description = PropertyDescription(
            beds=3,
            full_baths=2,
            sqft=1500
        )
        
        test_financial = PropertyFinancial(
            list_price=250000
        )
        
        test_dates = PropertyDates()
        
        test_location = PropertyLocation()
        
        test_property = Property(
            property_id="test_prop_123",
            address=test_address,
            description=test_description,
            financial=test_financial,
            dates=test_dates,
            location=test_location
        )
        test_property.update_content_tracking()
        
        # Save property
        result = await db.save_property(test_property)
        print(f"✅ Property saved: {result['action']}")
        
        # Search for property
        properties = await db.search_properties({"property_id": "test_prop_123"})
        print(f"✅ Property found: {len(properties)} results")
        
        # Clean up - delete test property
        await db.properties_collection.delete_one({"property_id": "test_prop_123"})
        print("✅ Test property cleaned up")
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_database_connection())
    print(f"\nTest result: {'PASSED' if result else 'FAILED'}")
