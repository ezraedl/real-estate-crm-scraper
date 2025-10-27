#!/usr/bin/env python
"""Check enrichment progress"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings

async def check_progress():
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    
    total = await db.properties.count_documents({'status': 'FOR_SALE'})
    enriched = await db.properties.count_documents({'status': 'FOR_SALE', 'enrichment': {'$exists': True}})
    
    print(f"Total FOR_SALE properties: {total}")
    print(f"Properties with enrichment: {enriched}")
    print(f"Progress: {enriched*100//total if total > 0 else 0}%")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(check_progress())

