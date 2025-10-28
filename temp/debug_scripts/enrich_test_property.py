#!/usr/bin/env python
"""
Find and enrich a property by address
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from services import PropertyEnrichmentPipeline
from config import settings

async def find_and_enrich_property():
    # Connect to database using config settings
    MONGODB_URI = settings.MONGODB_URI
    print(f"Connecting to MongoDB at: {MONGODB_URI}")
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client['mls_scraper']
    properties_collection = db.properties
    
    # Search for property by address
    search_address = "7039 Lantern Rd, Indianapolis, IN, 46256"
    
    # Try different search patterns
    search_patterns = [
        {"address.formatted_address": {"$regex": search_address, "$options": "i"}},
        {"address.street": {"$regex": "7039 Lantern", "$options": "i"}},
        {"address.zip_code": "46256"},
    ]
    
    property_found = None
    for pattern in search_patterns:
        property_found = await properties_collection.find_one(pattern)
        if property_found:
            print(f"Found property with pattern: {pattern}")
            break
    
    if not property_found:
        print("Property not found in database")
        # List some properties to see what's in the DB
        print("\nSample properties in database:")
        async for prop in properties_collection.find().limit(5):
            print(f"  - {prop.get('address', {}).get('formatted_address', 'No address')} (ID: {prop.get('property_id', 'No ID')})")
        return
    
    property_id = property_found.get('property_id')
    print(f"Found property ID: {property_id}")
    print(f"Address: {property_found.get('address', {}).get('formatted_address', 'N/A')}")
    
    # Initialize enrichment pipeline
    enrichment_pipeline = PropertyEnrichmentPipeline(db)
    await enrichment_pipeline.initialize()
    
    # Enrich the property
    print("\nEnriching property...")
    enrichment_data = await enrichment_pipeline.enrich_property(
        property_id=property_id,
        property_dict=property_found,
        existing_property=None,
        job_id=None
    )
    
    print("\nEnrichment completed!")
    print(f"Motivated Seller Score: {enrichment_data.get('motivated_seller', {}).get('score', 'N/A')}")
    print(f"Confidence: {enrichment_data.get('motivated_seller', {}).get('confidence', 'N/A')}")
    print(f"\nFull enrichment data:")
    print(enrichment_data)
    
    # Close connection
    client.close()

if __name__ == "__main__":
    asyncio.run(find_and_enrich_property())

