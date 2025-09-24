#!/usr/bin/env python3
"""
Test full scraping to get more Indianapolis sold properties
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

async def test_full_scrape():
    """Test full scraping to get more Indianapolis sold properties"""
    db = Database()
    scraper = MLSScraper()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Create a comprehensive test job for sold properties
        test_job = ScrapingJob(
            job_id="test_full_scrape_job",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=365,  # Full year
            mls_only=True,
            limit=5000  # Higher limit
        )
        
        print(f"🧪 Testing full scraping for Indianapolis sold properties...")
        print(f"   Past days: {test_job.past_days}")
        print(f"   Limit: {test_job.limit}")
        print(f"   MLS only: {test_job.mls_only}")
        
        # Test scraping one location
        location = "Indianapolis, IN"
        print(f"\n   Scraping location: {location}")
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(test_job)
        print(f"   Proxy config: {proxy_config}")
        
        # Scrape properties
        properties = await scraper.scrape_location(location, test_job, proxy_config)
        print(f"   Properties scraped: {len(properties)}")
        
        if properties:
            print(f"\n   Sample properties:")
            for i, prop in enumerate(properties[:5]):  # Show first 5
                print(f"     {i+1}. {prop.address.formatted_address if prop.address else 'No address'}")
                print(f"        Status: {prop.status}, Listing Type: {prop.listing_type}")
                print(f"        MLS ID: {prop.mls_id}, Sold Date: {prop.dates.last_sold_date if prop.dates else 'Unknown'}")
            
            # Test batch save
            print(f"\n💾 Testing batch save...")
            batch_result = await db.save_properties_batch(properties)
            print(f"   Batch save result: {batch_result}")
            
            # Check total sold properties now
            print(f"\n🔍 Checking total sold properties...")
            total_sold = await db.properties_collection.count_documents({
                "listing_type": "sold"
            })
            print(f"   Total sold properties in database: {total_sold}")
            
            indy_sold = await db.properties_collection.count_documents({
                "listing_type": "sold",
                "address.city": "Indianapolis"
            })
            print(f"   Indianapolis sold properties: {indy_sold}")
            
            # Check by date range
            one_year_ago = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
            recent_sold = await db.properties_collection.count_documents({
                "listing_type": "sold",
                "address.city": "Indianapolis",
                "dates.last_sold_date": {"$gte": one_year_ago}
            })
            print(f"   Indianapolis sold properties (past year): {recent_sold}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(test_full_scrape())
