#!/usr/bin/env python3
"""
Comprehensive scraper to get all Indianapolis sold properties
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from database import Database
from scraper import MLSScraper
from models import ScrapingJob, ListingType, JobPriority
from homeharvest import scrape_property
import pandas as pd

async def comprehensive_scrape():
    """Comprehensive scraping to get all Indianapolis sold properties"""
    db = Database()
    scraper = MLSScraper()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Strategy 1: Multiple location-based scraping
        locations = [
            "Indianapolis, IN",
            "Marion County, IN",
            # Add specific zip codes for more comprehensive coverage
            "46201, Indianapolis, IN",
            "46202, Indianapolis, IN", 
            "46203, Indianapolis, IN",
            "46204, Indianapolis, IN",
            "46205, Indianapolis, IN",
            "46208, Indianapolis, IN",
            "46218, Indianapolis, IN",
            "46219, Indianapolis, IN",
            "46220, Indianapolis, IN",
            "46221, Indianapolis, IN",
            "46222, Indianapolis, IN",
            "46224, Indianapolis, IN",
            "46225, Indianapolis, IN",
            "46226, Indianapolis, IN",
            "46227, Indianapolis, IN",
            "46228, Indianapolis, IN",
            "46229, Indianapolis, IN",
            "46230, Indianapolis, IN",
            "46231, Indianapolis, IN",
            "46234, Indianapolis, IN",
            "46235, Indianapolis, IN",
            "46236, Indianapolis, IN",
            "46237, Indianapolis, IN",
            "46239, Indianapolis, IN",
            "46240, Indianapolis, IN",
            "46241, Indianapolis, IN",
            "46254, Indianapolis, IN",
            "46260, Indianapolis, IN",
            "46268, Indianapolis, IN",
            "46278, Indianapolis, IN",
            "46280, Indianapolis, IN",
            "46282, Indianapolis, IN",
            "46283, Indianapolis, IN",
            "46285, Indianapolis, IN",
            "46290, Indianapolis, IN",
            "46291, Indianapolis, IN",
            "46295, Indianapolis, IN",
            "46296, Indianapolis, IN",
            "46298, Indianapolis, IN"
        ]
        
        print(f"🎯 Comprehensive scraping strategy:")
        print(f"   📍 {len(locations)} locations to scrape")
        print(f"   📅 Time period: Last 365 days")
        print(f"   🔢 Limit per location: 1000 properties")
        print(f"   📊 Expected total: Up to {len(locations) * 1000} properties")
        
        total_properties_scraped = 0
        total_properties_saved = 0
        all_properties = []
        
        # Process each location
        for i, location in enumerate(locations):
            print(f"\n📍 Scraping location {i+1}/{len(locations)}: {location}")
            
            try:
                # Create a job for this location
                job = ScrapingJob(
                    job_id=f"comprehensive_{i}_{int(datetime.utcnow().timestamp())}",
                    priority=JobPriority.IMMEDIATE,
                    locations=[location],
                    listing_type=ListingType.SOLD,
                    past_days=365,
                    mls_only=False,  # Use all sources, not just MLS
                    limit=1000
                )
                
                # Get proxy config
                proxy_config = await scraper.get_proxy_config(job)
                
                # Scrape properties
                properties = await scraper.scrape_location(location, job, proxy_config)
                
                if properties:
                    print(f"   ✅ Scraped {len(properties)} properties")
                    
                    # Save properties to database
                    save_results = await db.save_properties_batch(properties)
                    saved_count = save_results["inserted"] + save_results["updated"]
                    
                    print(f"   💾 Saved: {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
                    
                    total_properties_scraped += len(properties)
                    total_properties_saved += saved_count
                    all_properties.extend(properties)
                    
                    # Add delay between locations to avoid rate limiting
                    await asyncio.sleep(2)
                else:
                    print(f"   ⚠️  No properties found for this location")
                    
            except Exception as e:
                print(f"   ❌ Error scraping {location}: {e}")
                continue
        
        # Final statistics
        print(f"\n📊 Comprehensive scraping results:")
        print(f"   🏠 Total properties scraped: {total_properties_scraped}")
        print(f"   💾 Total properties saved: {total_properties_saved}")
        print(f"   📍 Locations processed: {len(locations)}")
        
        # Check final database counts
        print(f"\n🔍 Final database statistics:")
        
        # Total sold properties
        total_sold = await db.properties_collection.count_documents({
            "listing_type": "sold"
        })
        print(f"   🏠 Total sold properties in database: {total_sold}")
        
        # Indianapolis sold properties
        indy_sold = await db.properties_collection.count_documents({
            "listing_type": "sold",
            "address.city": "Indianapolis"
        })
        print(f"   🏙️ Indianapolis sold properties: {indy_sold}")
        
        # Recent properties (last 6 months)
        six_months_ago = datetime.utcnow() - timedelta(days=180)
        recent_sold = await db.properties_collection.count_documents({
            "listing_type": "sold",
            "address.city": "Indianapolis",
            "dates.last_sold_date": {"$gte": six_months_ago}
        })
        print(f"   📅 Indianapolis sold properties (last 6 months): {recent_sold}")
        
        # Compare with Zillow's reported 8,036
        if recent_sold > 0:
            coverage_percentage = (recent_sold / 8036) * 100
            print(f"   📊 Coverage vs Zillow (8,036): {coverage_percentage:.1f}%")
        
        # Show sample of recent properties
        recent_properties = await db.properties_collection.find({
            "listing_type": "sold",
            "address.city": "Indianapolis",
            "dates.last_sold_date": {"$gte": six_months_ago}
        }).sort("dates.last_sold_date", -1).limit(5).to_list(length=5)
        
        print(f"\n🏠 Sample recent sold properties:")
        for prop in recent_properties:
            address = prop.get('address', {}).get('formatted_address', 'No address')
            sold_date = prop.get('dates', {}).get('last_sold_date', 'Unknown')
            price = prop.get('financial', {}).get('sold_price', 'Unknown')
            print(f"   • {address} - Sold: {sold_date} - Price: ${price}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(comprehensive_scrape())
