#!/usr/bin/env python3
"""
Optimized scraper to get maximum Indianapolis sold properties from homeharvest
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

async def optimized_scrape():
    """Optimized scraping to get maximum Indianapolis sold properties"""
    db = Database()
    scraper = MLSScraper()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        print(f"🎯 Optimized scraping strategy:")
        print(f"   📊 Target: Get all available Indianapolis sold properties")
        print(f"   📅 Time period: Last 365 days (maximum available: ~2,464 properties)")
        print(f"   🔢 Limit: 10,000 (to ensure we get all available data)")
        print(f"   📍 Sources: All sources (not just MLS)")
        
        # Create optimized job
        job = ScrapingJob(
            job_id=f"optimized_indy_sold_{int(datetime.utcnow().timestamp())}",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=365,  # Full year to get maximum data
            mls_only=False,  # Use all sources
            limit=10000,  # High limit to get all available data
            exclude_pending=True  # Focus on sold properties
        )
        
        print(f"\n📍 Scraping Indianapolis, IN...")
        
        # Get proxy config
        proxy_config = await scraper.get_proxy_config(job)
        
        # Scrape properties
        properties = await scraper.scrape_location("Indianapolis, IN", job, proxy_config)
        
        if properties:
            print(f"   ✅ Scraped {len(properties)} properties")
            
            # Save properties to database
            save_results = await db.save_properties_batch(properties)
            saved_count = save_results["inserted"] + save_results["updated"]
            
            print(f"   💾 Saved: {save_results['inserted']} inserted, {save_results['updated']} updated, {save_results['skipped']} skipped")
            
            # Show sample properties
            print(f"\n🏠 Sample properties:")
            for i, prop in enumerate(properties[:5]):
                address = prop.address.formatted_address if prop.address else 'No address'
                sold_date = prop.dates.last_sold_date if prop.dates and prop.dates.last_sold_date else 'Unknown'
                price = prop.financial.sold_price if prop.financial and prop.financial.sold_price else 'Unknown'
                print(f"   {i+1}. {address}")
                print(f"      Sold: {sold_date} - Price: ${price}")
            
            # Final statistics
            print(f"\n📊 Final results:")
            print(f"   🏠 Properties scraped: {len(properties)}")
            print(f"   💾 Properties saved: {saved_count}")
            
            # Check database counts
            total_sold = await db.properties_collection.count_documents({
                "listing_type": "sold"
            })
            print(f"   🗄️ Total sold properties in database: {total_sold}")
            
            indy_sold = await db.properties_collection.count_documents({
                "listing_type": "sold",
                "address.city": "Indianapolis"
            })
            print(f"   🏙️ Indianapolis sold properties: {indy_sold}")
            
            # Check coverage vs Zillow's 8,036 (6 months)
            six_months_ago = datetime.utcnow() - timedelta(days=180)
            recent_sold = await db.properties_collection.count_documents({
                "listing_type": "sold",
                "address.city": "Indianapolis",
                "dates.last_sold_date": {"$gte": six_months_ago}
            })
            print(f"   📅 Indianapolis sold properties (last 6 months): {recent_sold}")
            
            if recent_sold > 0:
                coverage_percentage = (recent_sold / 8036) * 100
                print(f"   📊 Coverage vs Zillow (8,036): {coverage_percentage:.1f}%")
            
            # Show date range of scraped data
            if properties:
                sold_dates = [prop.dates.last_sold_date for prop in properties if prop.dates and prop.dates.last_sold_date]
                if sold_dates:
                    min_date = min(sold_dates)
                    max_date = max(sold_dates)
                    print(f"   📅 Date range: {min_date} to {max_date}")
            
            print(f"\n💡 Analysis:")
            print(f"   • Homeharvest provides {len(properties)} sold properties for Indianapolis")
            print(f"   • This is significantly less than Zillow's reported 8,036 (6 months)")
            print(f"   • The homeharvest library appears to have limited access to Zillow's full dataset")
            print(f"   • For comprehensive data, consider direct Zillow scraping or alternative data sources")
            
        else:
            print(f"   ⚠️  No properties found")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(optimized_scrape())
