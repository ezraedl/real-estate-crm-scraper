#!/usr/bin/env python3
"""
Test the fix for listing_type field
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import Database
from scraper import MLSScraper
from models import ScrapingJob, ListingType, JobPriority

async def test_fix():
    """Test the fix for listing_type field"""
    db = Database()
    scraper = MLSScraper()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Create a test job for sold properties
        test_job = ScrapingJob(
            job_id="test_fix_job",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=30,  # Shorter period for testing
            mls_only=True,
            limit=50  # Smaller limit for testing
        )
        
        print(f"🧪 Testing scraping with fixed listing_type...")
        
        # Test scraping one location
        location = "Indianapolis, IN"
        print(f"   Scraping location: {location}")
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(test_job)
        
        # Scrape properties
        properties = await scraper.scrape_location(location, test_job, proxy_config)
        print(f"   Properties scraped: {len(properties)}")
        
        if properties:
            print(f"   Sample property:")
            sample_prop = properties[0]
            print(f"     Property ID: {sample_prop.property_id}")
            print(f"     Address: {sample_prop.address.formatted_address if sample_prop.address else 'No address'}")
            print(f"     MLS ID: {sample_prop.mls_id}")
            print(f"     Status: {sample_prop.status}")
            print(f"     Listing Type: {sample_prop.listing_type}")
            print(f"     Content hash: {sample_prop.content_hash}")
            
            # Test saving one property
            print(f"\n💾 Testing property save...")
            save_result = await db.save_property(properties[0])
            print(f"   Save result: {save_result}")
            
            # Test batch save
            print(f"\n💾 Testing batch save...")
            batch_result = await db.save_properties_batch(properties[:5])  # Test with first 5
            print(f"   Batch save result: {batch_result}")
            
            # Check if the properties are now queryable by listing_type
            print(f"\n🔍 Checking if properties are queryable by listing_type...")
            sold_count = await db.properties_collection.count_documents({
                "listing_type": "sold",
                "address.city": "Indianapolis"
            })
            print(f"   Indianapolis sold properties: {sold_count}")
            
            # Check recent properties with listing_type
            recent_sold = await db.properties_collection.find({
                "listing_type": "sold",
                "scraped_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)}
            }).to_list(length=3)
            
            print(f"   Recent sold properties (today):")
            for prop in recent_sold:
                print(f"     {prop.get('address', {}).get('formatted_address', 'No address')} - {prop.get('listing_type', 'Unknown')}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(test_fix())
