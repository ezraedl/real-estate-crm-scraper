#!/usr/bin/env python
"""
Re-enrich all FOR_SALE properties in the database with the new scoring configuration
"""
import asyncio
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from services import PropertyEnrichmentPipeline
from config import settings

def format_time(seconds):
    """Format seconds into human-readable time"""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"

async def re_enrich_all_properties():
    """Re-enrich all FOR_SALE properties in the database"""
    
    # Connect to database
    print(f"Connecting to MongoDB at: {settings.MONGODB_URI}")
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    db = client['mls_scraper']
    properties_collection = db.properties
    
    # Define the cutoff date for re-enrichment
    cutoff_date = datetime.fromisoformat("2025-10-26T17:41:10.605+00:00")
    
    # Filter for FOR_SALE properties that need enrichment:
    # - No enrichment exists, OR
    # - last_enriched_at is older than cutoff date
    filter_query = {
        "status": "FOR_SALE",
        "$or": [
            {"enrichment": {"$exists": False}},
            {"last_enriched_at": {"$lt": cutoff_date}}
        ]
    }
    
    # Count total FOR_SALE properties that need enrichment
    total_count = await properties_collection.count_documents(filter_query)
    
    # Also count total FOR_SALE properties for reference
    total_for_sale = await properties_collection.count_documents({"status": "FOR_SALE"})
    
    print(f"\nTotal FOR_SALE properties: {total_for_sale}")
    print(f"Properties needing enrichment: {total_count}")
    print(f"Cutoff date: {cutoff_date}\n")
    
    if total_count == 0:
        print("No properties need enrichment (all are already enriched or up-to-date)")
        return
    
    # Initialize enrichment pipeline
    print("Initializing enrichment pipeline...")
    enrichment_pipeline = PropertyEnrichmentPipeline(db)
    await enrichment_pipeline.initialize()
    
    # Start timing
    start_time = datetime.now()
    print(f"Starting enrichment at {start_time.strftime('%H:%M:%S')}\n")
    
    # Process properties in batches
    processed = 0
    failed = 0
    
    async for property_doc in properties_collection.find(filter_query):
        try:
            property_id = property_doc.get('property_id')
            address = property_doc.get('address', {}).get('formatted_address', 'Unknown')
            
            # Re-enrich the property
            enrichment_data = await enrichment_pipeline.enrich_property(
                property_id=property_id,
                property_dict=property_doc,
                existing_property=None,
                job_id=None
            )
            
            score = enrichment_data.get('motivated_seller', {}).get('score', 0)
            
            processed += 1
            
            # Progress update every 50 properties
            if processed % 50 == 0:
                elapsed_time = (datetime.now() - start_time).total_seconds()
                avg_time_per_property = elapsed_time / processed if processed > 0 else 0
                remaining_properties = total_count - processed
                estimated_remaining_time = avg_time_per_property * remaining_properties
                
                print(f"Progress: {processed}/{total_count} ({processed*100//total_count}%) | "
                      f"Elapsed: {format_time(elapsed_time)} | "
                      f"ETA: {format_time(estimated_remaining_time)} | "
                      f"Last: {address} (Score: {score})")
                
        except Exception as e:
            print(f"  Error: {e}")
            failed += 1
            continue
    
    # Calculate final statistics
    elapsed_time = (datetime.now() - start_time).total_seconds()
    avg_time_per_property = elapsed_time / processed if processed > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"Re-enrichment complete!")
    print(f"  Total properties: {total_count}")
    print(f"  Successfully enriched: {processed}")
    print(f"  Failed: {failed}")
    print(f"  Total time: {format_time(elapsed_time)}")
    print(f"  Average time per property: {avg_time_per_property:.2f}s")
    print(f"{'='*60}")
    
    # Close connection
    client.close()

if __name__ == "__main__":
    asyncio.run(re_enrich_all_properties())

