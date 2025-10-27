#!/usr/bin/env python
"""Find properties with enrichment data and high motivated seller scores"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings

async def find_properties_with_insights():
    """Find properties that have interesting insights to display"""
    
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    
    # Find properties with enrichment scores >= 40
    query = {'enrichment.motivated_seller.score': {'$gte': 40}}
    
    count = await db.properties.count_documents(query)
    print(f"Properties with high motivated seller scores (>=40): {count}")
    
    # Get a sample
    async for prop in db.properties.find(query).limit(5):
        prop_id = prop.get('property_id')
        address = prop.get('address', {}).get('formatted_address', 'Unknown')
        score = prop.get('enrichment', {}).get('motivated_seller', {}).get('score', 0)
        
        print(f"\nProperty: {prop_id}")
        print(f"  Address: {address}")
        print(f"  Score: {score}/100")
        
        # Check for price reductions
        price_history = prop.get('enrichment', {}).get('price_history_summary', {})
        if price_history:
            print(f"  Price Reductions: {price_history.get('number_of_reductions', 0)}")
            print(f"  Total Reduced: ${price_history.get('total_reductions', 0):,.0f}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(find_properties_with_insights())

