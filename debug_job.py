#!/usr/bin/env python3
"""
Debug the specific job that only scraped 24 properties
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

async def debug_job():
    """Debug the specific job that only scraped 24 properties"""
    db = Database()
    scraper = MLSScraper()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Get the specific job
        job_data = await db.jobs_collection.find_one({"job_id": "immediate_sold_test_1757920978"})
        if not job_data:
            print("❌ Job not found")
            return
        
        print(f"📋 Found job: {job_data['job_id']}")
        print(f"   Status: {job_data['status']}")
        print(f"   Properties scraped: {job_data['properties_scraped']}")
        print(f"   Properties saved: {job_data['properties_saved']}")
        print(f"   Locations: {job_data['locations']}")
        print(f"   Listing type: {job_data['listing_type']}")
        print(f"   Past days: {job_data['past_days']}")
        print(f"   Limit: {job_data['limit']}")
        print(f"   MLS only: {job_data['mls_only']}")
        
        # Test scraping with the same parameters
        print(f"\n🧪 Testing scraping with same parameters...")
        
        # Create a test job with the same parameters
        test_job = ScrapingJob(
            job_id="debug_test_job",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=180,
            mls_only=True,
            limit=1000
        )
        
        # Test scraping one location
        location = "Indianapolis, IN"
        print(f"   Scraping location: {location}")
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(test_job)
        print(f"   Proxy config: {proxy_config}")
        
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
            print(f"     Content hash: {sample_prop.content_hash}")
            
            # Test saving one property
            print(f"\n💾 Testing property save...")
            save_result = await db.save_property(properties[0])
            print(f"   Save result: {save_result}")
            
            # Test batch save
            print(f"\n💾 Testing batch save...")
            batch_result = await db.save_properties_batch(properties[:5])  # Test with first 5
            print(f"   Batch save result: {batch_result}")
        
        # Check if there are any existing properties with similar characteristics
        print(f"\n🔍 Checking existing properties...")
        existing_count = await db.properties_collection.count_documents({
            "address.city": "Indianapolis",
            "listing_type": "sold"
        })
        print(f"   Existing Indianapolis sold properties: {existing_count}")
        
        # Check recent properties
        recent_count = await db.properties_collection.count_documents({
            "scraped_at": {"$gte": datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)}
        })
        print(f"   Properties scraped today: {recent_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(debug_job())
