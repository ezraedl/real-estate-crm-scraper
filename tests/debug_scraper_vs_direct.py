#!/usr/bin/env python3
"""
Debug why scraper gets 68 properties vs direct homeharvest gets 2,464
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from homeharvest import scrape_property
from scraper import MLSScraper
from models import ScrapingJob, ListingType, JobPriority

async def debug_scraper_vs_direct():
    """Debug the difference between scraper and direct homeharvest calls"""
    
    print("🔍 Debugging scraper vs direct homeharvest calls...")
    
    # Test 1: Direct homeharvest call
    print("\n1. Direct homeharvest call...")
    try:
        df = scrape_property(
            location="Indianapolis, IN",
            listing_type="sold",
            limit=10000,
            past_days=365,
            mls_only=False
        )
        print(f"   ✅ Direct call: {len(df)} properties")
        
        if len(df) > 0:
            print(f"   📅 Date range: {df['last_sold_date'].min()} to {df['last_sold_date'].max()}")
            print(f"   📊 Sample addresses:")
            for i, row in df.head(3).iterrows():
                print(f"      {i+1}. {row.get('formatted_address', 'No address')}")
                
    except Exception as e:
        print(f"   ❌ Direct call error: {e}")
    
    # Test 2: Scraper call
    print("\n2. Scraper call...")
    try:
        scraper = MLSScraper()
        
        # Create job
        job = ScrapingJob(
            job_id="debug_job",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=365,
            mls_only=False,
            limit=10000
        )
        
        # Scrape using scraper
        properties = await scraper.scrape_location("Indianapolis, IN", job, None)
        print(f"   ✅ Scraper call: {len(properties)} properties")
        
        if properties:
            print(f"   📅 Date range: {min([p.dates.last_sold_date for p in properties if p.dates and p.dates.last_sold_date])} to {max([p.dates.last_sold_date for p in properties if p.dates and p.dates.last_sold_date])}")
            print(f"   📊 Sample addresses:")
            for i, prop in enumerate(properties[:3]):
                address = prop.address.formatted_address if prop.address else 'No address'
                print(f"      {i+1}. {address}")
                
    except Exception as e:
        print(f"   ❌ Scraper call error: {e}")
        import traceback
        traceback.print_exc()
    
    # Test 3: Check scraper's _scrape_listing_type method directly
    print("\n3. Testing scraper's _scrape_listing_type method...")
    try:
        scraper = MLSScraper()
        
        # Create job
        job = ScrapingJob(
            job_id="debug_job_2",
            priority=JobPriority.IMMEDIATE,
            locations=["Indianapolis, IN"],
            listing_type=ListingType.SOLD,
            past_days=365,
            mls_only=False,
            limit=10000
        )
        
        # Call _scrape_listing_type directly
        properties = await scraper._scrape_listing_type("Indianapolis, IN", job, None, "sold")
        print(f"   ✅ _scrape_listing_type: {len(properties)} properties")
        
        if properties:
            print(f"   📅 Date range: {min([p.dates.last_sold_date for p in properties if p.dates and p.dates.last_sold_date])} to {max([p.dates.last_sold_date for p in properties if p.dates and p.dates.last_sold_date])}")
                
    except Exception as e:
        print(f"   ❌ _scrape_listing_type error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_scraper_vs_direct())
