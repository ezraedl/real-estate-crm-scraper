#!/usr/bin/env python
"""
Find properties with price reductions and manually enrich them to populate price_history_summary
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from services import PropertyEnrichmentPipeline
from config import settings

async def find_and_enrich_price_reduction_properties():
    """Find properties with price reductions and enrich them"""
    
    # Connect to database
    MONGODB_URI = settings.MONGODB_URI
    print(f"Connecting to MongoDB at: {MONGODB_URI}")
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client['mls_scraper']
    
    # Query property_history collection for price reductions
    print("\nQuerying property_history for price reductions...")
    history_query = {
        "change_type": "price_change",
        "data.change_type": "reduction"
    }
    
    # Get unique property_ids with price reductions
    property_ids_with_reductions = set()
    reduction_details = {}  # Store details for each property
    
    print("Collecting all properties with price reductions...")
    async for entry in db.property_history.find(history_query):
        property_id = entry.get('property_id')
        if property_id:
            property_ids_with_reductions.add(property_id)
            if property_id not in reduction_details:
                reduction_details[property_id] = []
            
            entry_data = entry.get('data', {})
            reduction_details[property_id].append({
                'old_price': entry_data.get('old_price'),
                'new_price': entry_data.get('new_price'),
                'price_difference': entry_data.get('price_difference'),
                'percent_change': entry_data.get('percent_change'),
                'timestamp': entry.get('timestamp')
            })
    
    print(f"Found {len(property_ids_with_reductions)} unique properties with price reductions")
    
    if not property_ids_with_reductions:
        print("No properties with price reductions found in property_history collection")
        client.close()
        return
    
    # Verify which properties actually exist in the properties collection
    existing_property_ids = []
    for property_id in property_ids_with_reductions:
        property_exists = await db.properties.count_documents({"property_id": property_id}, limit=1)
        if property_exists > 0:
            existing_property_ids.append(property_id)
    
    print(f"Found {len(existing_property_ids)} properties that exist in the properties collection")
    print(f"Will skip {len(property_ids_with_reductions) - len(existing_property_ids)} properties that don't exist")
    print(f"\nEnriching {len(existing_property_ids)} properties...")
    
    property_ids_to_enrich = existing_property_ids
    
    # Initialize enrichment pipeline
    enrichment_pipeline = PropertyEnrichmentPipeline(db)
    await enrichment_pipeline.initialize()
    
    enriched_count = 0
    failed_count = 0
    skipped_count = 0
    already_has_summary_count = 0
    
    for idx, property_id in enumerate(property_ids_to_enrich, 1):
        print(f"\n{'='*80}")
        print(f"Processing Property {idx}/{len(property_ids_to_enrich)}: {property_id}")
        print(f"{'='*80}")
        
        # Get property document
        property_doc = await db.properties.find_one({"property_id": property_id})
        
        if not property_doc:
            print(f"  [WARN] Property not found in properties collection (should not happen)")
            failed_count += 1
            continue
        
        address = property_doc.get('address', {}).get('formatted_address', 'Unknown')
        current_price = property_doc.get('financial', {}).get('list_price', 'N/A')
        print(f"  Address: {address}")
        print(f"  Current Price: ${current_price:,}" if isinstance(current_price, (int, float)) else f"  Current Price: {current_price}")
        
        # Show reduction details
        if property_id in reduction_details:
            reductions = reduction_details[property_id]
            total_reduction = sum(abs(r['price_difference']) for r in reductions if r.get('price_difference'))
            print(f"  Price Reductions in History: {len(reductions)}")
            print(f"  Total Reduction Amount: ${total_reduction:,.2f}")
            for i, reduction in enumerate(reductions[:3], 1):  # Show up to 3 reductions
                print(f"    Reduction {i}: ${reduction.get('old_price', 0):,.0f} -> ${reduction.get('new_price', 0):,.0f} ({reduction.get('percent_change', 0):.2f}%)")
        
        # Check existing enrichment
        existing_enrichment = property_doc.get('enrichment', {})
        existing_summary = existing_enrichment.get('price_history_summary')
        
        if existing_summary:
            print(f"\n  [INFO] Existing price_history_summary found:")
            print(f"     Total Reductions: ${existing_summary.get('total_reductions', 0):,.2f}")
            print(f"     Number of Reductions: {existing_summary.get('number_of_reductions', 0)}")
            print(f"     Days Since Last Reduction: {existing_summary.get('days_since_last_reduction', 'N/A')}")
            # Check if we should skip re-enrichment
            if existing_summary.get('total_reductions', 0) > 0:
                print(f"  [INFO] Skipping - already has valid price_history_summary")
                already_has_summary_count += 1
                continue
        else:
            print(f"\n  [WARN] No existing price_history_summary found")
        
        # Enrich the property
        print(f"\n  [INFO] Enriching property...")
        try:
            enrichment_data = await enrichment_pipeline.enrich_property(
                property_id=property_id,
                property_dict=property_doc,
                existing_property=None,  # We're treating it as current state
                job_id=None
            )
            
            # Verify the enrichment was successful
            updated_property = await db.properties.find_one({"property_id": property_id})
            updated_enrichment = updated_property.get('enrichment', {}) if updated_property else {}
            new_summary = updated_enrichment.get('price_history_summary')
            
            if new_summary:
                print(f"\n  [SUCCESS] Enrichment completed successfully!")
                print(f"     [INFO] New price_history_summary:")
                print(f"        Original Price: ${new_summary.get('original_price', 0):,.2f}")
                print(f"        Current Price: ${new_summary.get('current_price', 0):,.2f}")
                print(f"        Total Reductions: ${new_summary.get('total_reductions', 0):,.2f}")
                print(f"        Total Reduction Percent: {new_summary.get('total_reduction_percent', 0):.2f}%")
                print(f"        Number of Reductions: {new_summary.get('number_of_reductions', 0)}")
                print(f"        Days Since Last Reduction: {new_summary.get('days_since_last_reduction', 'N/A')}")
                print(f"        Average Reduction: ${new_summary.get('average_reduction_amount', 0):,.2f}")
                
                # Verify the filter would work
                total_reductions = new_summary.get('total_reductions', 0)
                days_since = new_summary.get('days_since_last_reduction')
                
                if total_reductions > 0:
                    print(f"\n     [SUCCESS] Filter test: This property would match:")
                    print(f"        - priceReductionMin <= ${total_reductions:,.2f}")
                    if days_since is not None:
                        print(f"        - priceReductionTimeWindow >= {days_since} days")
                else:
                    print(f"\n     [WARN] Warning: total_reductions is 0, filter may not work")
                
                enriched_count += 1
            else:
                print(f"\n  [ERROR] Enrichment completed but price_history_summary is still missing!")
                failed_count += 1
                
        except Exception as e:
            print(f"\n  [ERROR] Error enriching property: {e}")
            import traceback
            traceback.print_exc()
            failed_count += 1
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"Total properties with price reductions in history: {len(property_ids_with_reductions)}")
    print(f"Properties found in properties collection: {len(existing_property_ids)}")
    print(f"Properties processed: {len(property_ids_to_enrich)}")
    print(f"  - Successfully enriched: {enriched_count}")
    print(f"  - Already had price_history_summary: {already_has_summary_count}")
    print(f"  - Failed/Error: {failed_count}")
    print(f"  - Skipped (not in collection): {len(property_ids_with_reductions) - len(existing_property_ids)}")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(find_and_enrich_price_reduction_properties())

