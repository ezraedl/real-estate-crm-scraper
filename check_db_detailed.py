#!/usr/bin/env python3
"""
Detailed database check for Indianapolis sold properties
"""

import asyncio
import sys
from datetime import datetime, timedelta

from database import Database

async def check_database_detailed():
    """Detailed check of the database for Indianapolis sold properties"""
    db = Database()
    
    try:
        await db.connect()
        print("✅ Connected to MongoDB")
        
        # Count total properties
        total_properties = await db.properties_collection.count_documents({})
        print(f"\n🏠 Total properties in database: {total_properties}")
        
        # Count by listing_type
        pipeline = [
            {"$group": {
                "_id": "$listing_type",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        listing_type_counts = await db.properties_collection.aggregate(pipeline).to_list(length=10)
        print(f"\n📊 Properties by listing type:")
        for item in listing_type_counts:
            print(f"   {item['_id']}: {item['count']}")
        
        # Count by city
        city_pipeline = [
            {"$group": {
                "_id": "$address.city",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        city_counts = await db.properties_collection.aggregate(city_pipeline).to_list(length=10)
        print(f"\n🏙️ Properties by city:")
        for item in city_counts:
            print(f"   {item['_id']}: {item['count']}")
        
        # Check for Indianapolis properties specifically
        indy_properties = await db.properties_collection.find({
            "address.city": "Indianapolis"
        }).to_list(length=5)
        
        print(f"\n🏠 Indianapolis properties (sample):")
        for prop in indy_properties:
            print(f"   Property ID: {prop.get('property_id')}")
            print(f"   Address: {prop.get('address', {}).get('formatted_address', 'No address')}")
            print(f"   Listing type: {prop.get('listing_type', 'Unknown')}")
            print(f"   Status: {prop.get('status', 'Unknown')}")
            print(f"   MLS ID: {prop.get('mls_id', 'No MLS ID')}")
            print(f"   Scraped at: {prop.get('scraped_at', 'Unknown')}")
            print()
        
        # Check for sold properties specifically
        sold_properties = await db.properties_collection.find({
            "listing_type": "sold"
        }).to_list(length=5)
        
        print(f"\n💰 Sold properties (sample):")
        for prop in sold_properties:
            print(f"   Property ID: {prop.get('property_id')}")
            print(f"   Address: {prop.get('address', {}).get('formatted_address', 'No address')}")
            print(f"   City: {prop.get('address', {}).get('city', 'Unknown')}")
            print(f"   Status: {prop.get('status', 'Unknown')}")
            print(f"   MLS ID: {prop.get('mls_id', 'No MLS ID')}")
            print(f"   Sold date: {prop.get('dates', {}).get('last_sold_date', 'Unknown')}")
            print()
        
        # Check recent scraping activity
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        recent_properties = await db.properties_collection.find({
            "scraped_at": {"$gte": today}
        }).to_list(length=5)
        
        print(f"\n🆕 Recent properties (today):")
        for prop in recent_properties:
            print(f"   Property ID: {prop.get('property_id')}")
            print(f"   Address: {prop.get('address', {}).get('formatted_address', 'No address')}")
            print(f"   Listing type: {prop.get('listing_type', 'Unknown')}")
            print(f"   Scraped at: {prop.get('scraped_at', 'Unknown')}")
            print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await db.disconnect()
        print(f"\n🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    asyncio.run(check_database_detailed())
