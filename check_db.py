#!/usr/bin/env python3
"""
Check database contents for Indianapolis sold properties
"""

import asyncio
import sys
from datetime import datetime, timedelta

from database import Database

async def check_database():
    """Check the database for Indianapolis sold properties"""
    db = Database()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Count total sold properties
        total_sold = await db.properties_collection.count_documents({'listing_type': 'sold'})
        print(f"\n🏠 Total SOLD properties in database: {total_sold}")
        
        # Count Indianapolis sold properties
        indy_sold = await db.properties_collection.count_documents({
            'listing_type': 'sold',
            'address.city': 'Indianapolis'
        })
        print(f"🏠 Indianapolis SOLD properties: {indy_sold}")
        
        # Count by past year
        one_year_ago = datetime.utcnow() - timedelta(days=365)
        recent_sold = await db.properties_collection.count_documents({
            'listing_type': 'sold',
            'address.city': 'Indianapolis',
            'dates.last_sold_date': {'$gte': one_year_ago}
        })
        print(f"📅 Indianapolis SOLD properties (past year): {recent_sold}")
        
        # Check job status
        jobs = await db.jobs_collection.find({'locations': {'$regex': 'Indianapolis'}}).to_list(length=10)
        print(f"\n📋 Indianapolis jobs: {len(jobs)}")
        for job in jobs:
            print(f"   Job {job['job_id']}: {job['status']} - {job['properties_scraped']} scraped")
        
        # Check for any recent scraping activity
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_properties = await db.properties_collection.count_documents({
            'scraped_at': {'$gte': today}
        })
        print(f"\n🆕 Properties scraped today: {today_properties}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(check_database())
