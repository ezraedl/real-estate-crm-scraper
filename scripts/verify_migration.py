"""Quick verification script to check migration results"""
import asyncio
import sys
import os
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import urlparse

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from config import settings

async def verify():
    parsed = urlparse(settings.MONGODB_URI)
    db_name = parsed.path.lstrip('/').split('?')[0] or 'mls_scraper'
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client[db_name]
    
    # Check a few sample properties
    prop = await db.properties.find_one(
        {'financial.list_price_old': {'$exists': True}},
        {'property_id': 1, 'financial': 1, 'status_old': 1, '_old_values_scraped_at': 1}
    )
    
    if prop:
        financial = prop.get('financial', {})
        print(f"[OK] Sample property {prop.get('property_id')}:")
        print(f"  - list_price: {financial.get('list_price')}")
        print(f"  - list_price_old: {financial.get('list_price_old')}")
        print(f"  - price_per_sqft_old: {financial.get('price_per_sqft_old')}")
        print(f"  - status_old: {prop.get('status_old')}")
        print(f"  - _old_values_scraped_at: {prop.get('_old_values_scraped_at')}")
    else:
        print("No migrated properties found")
    
    # Count properties with _old values
    count_with_old = await db.properties.count_documents({'financial.list_price_old': {'$exists': True}})
    print(f"\n[OK] Total properties with _old values: {count_with_old}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(verify())

