"""
Quick connection test script to diagnose connection issues
"""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from database import db
from config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_connections():
    """Test all required connections"""
    print("="*60)
    print("CONNECTION DIAGNOSTIC TEST")
    print("="*60)
    
    # Test 1: MongoDB Connection
    print("\n[1] Testing MongoDB Connection...")
    print(f"   MongoDB URI: {settings.MONGODB_URI[:50]}..." if len(settings.MONGODB_URI) > 50 else f"   MongoDB URI: {settings.MONGODB_URI}")
    try:
        await db.connect()
        print("   ✅ MongoDB connection: SUCCESS")
        
        # Try a simple query
        count = await db.jobs_collection.count_documents({})
        print(f"   ✅ MongoDB query test: SUCCESS (found {count} jobs)")
    except Exception as e:
        print(f"   ❌ MongoDB connection: FAILED")
        print(f"   Error: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
        return False
    
    # Test 2: HomeHarvest API (if available)
    print("\n[2] Testing HomeHarvest API...")
    try:
        from homeharvest import scrape_property
        # Try a minimal test
        df = scrape_property(location="Indianapolis, IN", listing_type=["for_sale"], limit=1)
        if df is not None and not df.empty:
            print("   ✅ HomeHarvest API: SUCCESS")
        else:
            print("   ⚠️  HomeHarvest API: Connected but returned 0 results")
    except Exception as e:
        print(f"   ❌ HomeHarvest API: FAILED")
        print(f"   Error: {str(e)}")
        print(f"   Error type: {type(e).__name__}")
    
    # Test 3: DataImpulse Proxy (if configured)
    print("\n[3] Testing DataImpulse Proxy...")
    if settings.USE_DATAIMPULSE and settings.DATAIMPULSE_ENDPOINT:
        print(f"   DataImpulse Endpoint: {settings.DATAIMPULSE_ENDPOINT}")
        print(f"   DataImpulse Login: {'Set' if settings.DATAIMPULSE_LOGIN else 'Not set'}")
        print("   ⚠️  DataImpulse proxy test requires actual scraping (skipped)")
    else:
        print("   ℹ️  DataImpulse proxy: Not configured")
    
    print("\n" + "="*60)
    print("DIAGNOSTIC COMPLETE")
    print("="*60)
    
    return True

if __name__ == "__main__":
    try:
        result = asyncio.run(test_connections())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

