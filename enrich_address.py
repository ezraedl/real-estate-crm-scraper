#!/usr/bin/env python
"""
Enrich a property by address using the API endpoint
"""
import requests
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import settings

async def enrich_by_address(address: str):
    """Find property by address and enrich it"""
    
    # Connect to database
    print(f"Connecting to MongoDB...")
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    properties_collection = db.properties
    
    # Search for property
    print(f"Searching for property with address: {address}")
    property_found = await properties_collection.find_one({
        "address.formatted_address": {"$regex": address, "$options": "i"}
    })
    
    if not property_found:
        print(f"Property not found with address: {address}")
        return
    
    property_id = property_found.get('property_id')
    print(f"Found property ID: {property_id}")
    
    # Call enrichment endpoint
    print(f"Calling enrichment endpoint...")
    response = requests.post(
        f"http://localhost:8000/properties/{property_id}/enrich",
        timeout=60
    )
    
    if response.status_code == 200:
        data = response.json()
        print(f"\n✅ Enrichment successful!")
        print(f"Score: {data['enrichment']['motivated_seller']['score']}")
        print(f"Confidence: {data['enrichment']['motivated_seller']['confidence']}")
    else:
        print(f"\n❌ Enrichment failed: {response.status_code}")
        print(response.text)
    
    client.close()

if __name__ == "__main__":
    import sys
    address = sys.argv[1] if len(sys.argv) > 1 else "7039 Lantern Rd, Indianapolis, IN, 46256"
    asyncio.run(enrich_by_address(address))

