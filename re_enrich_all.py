#!/usr/bin/env python
"""
Re-enrich all properties in the database with the new scoring configuration
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from services import PropertyEnrichmentPipeline
from config import settings

async def re_enrich_all_properties():
    """Re-enrich all properties in the database"""
    
    # Connect to database
    print(f"Connecting to MongoDB at: {settings.MONGODB_URI}")
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    properties_collection = db.properties
    
    # Count total properties
    total_count = await properties_collection.count_documents({})
    print(f"\nFound {total_count} properties to re-enrich\n")
    
    if total_count == 0:
        print("No properties found in database")
        return
    
    # Initialize enrichment pipeline
    enrichment_pipeline = PropertyEnrichmentPipeline(db)
    await enrichment_pipeline.initialize()
    
    # Process properties in batches
    batch_size = 10
    processed = 0
    failed = 0
    
    async for property_doc in properties_collection.find({}):
        try:
            property_id = property_doc.get('property_id')
            address = property_doc.get('address', {}).get('formatted_address', 'Unknown')
            
            print(f"Processing property {processed + 1}/{total_count}: {address}")
            
            # Re-enrich the property
            enrichment_data = await enrichment_pipeline.enrich_property(
                property_id=property_id,
                property_dict=property_doc,
                existing_property=None,
                job_id=None
            )
            
            score = enrichment_data.get('motivated_seller', {}).get('score', 0)
            print(f"  Score: {score}/100")
            
            processed += 1
            
            # Progress update every 10 properties
            if processed % 10 == 0:
                print(f"\nProgress: {processed}/{total_count} ({processed*100//total_count}%)\n")
                
        except Exception as e:
            print(f"  Error: {e}")
            failed += 1
            continue
    
    print(f"\n{'='*60}")
    print(f"Re-enrichment complete!")
    print(f"  Total properties: {total_count}")
    print(f"  Successfully enriched: {processed}")
    print(f"  Failed: {failed}")
    print(f"{'='*60}")
    
    # Close connection
    client.close()

if __name__ == "__main__":
    asyncio.run(re_enrich_all_properties())

