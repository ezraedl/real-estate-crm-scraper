"""
Simulate the price change detection to see if it works when price changes from 99k to 89k.
"""

import asyncio
import sys
import os
from datetime import datetime
from bson import ObjectId

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import db
from services.property_differ import PropertyDiffer
from services.property_enrichment import PropertyEnrichmentPipeline
from services.history_tracker import HistoryTracker

PROPERTY_ID = "4915688430"

async def simulate_price_change():
    """Simulate what happens when price changes from 99k to 89k"""
    print("\n" + "="*80)
    print("SIMULATING PRICE CHANGE FROM $99,000 TO $89,000")
    print("="*80)
    
    # Connect to database
    await db.connect()
    
    # Get current property
    property_doc = await db.properties_collection.find_one({"property_id": PROPERTY_ID})
    if not property_doc:
        print("[ERROR] Property not found")
        return
    
    print(f"\n[STEP 1] Current property in database:")
    print(f"   List Price: ${property_doc.get('financial', {}).get('list_price', 'N/A'):,}")
    print(f"   Content Hash: {property_doc.get('content_hash', 'N/A')[:32]}...")
    
    # Create old property (simulating previous state with $99k)
    import copy
    old_property = copy.deepcopy(property_doc)
    if 'financial' not in old_property:
        old_property['financial'] = {}
    old_property['financial']['list_price'] = 99000.0
    old_property['financial']['price_per_sqft'] = 52  # 99000 / 1908 = 51.88
    # Remove enrichment and metadata that wouldn't exist in old version
    old_property.pop('enrichment', None)
    old_property.pop('has_price_reduction', None)
    old_property.pop('is_motivated_seller', None)
    old_property.pop('has_distress_signals', None)
    old_property.pop('last_enriched_at', None)
    old_property.pop('last_content_updated', None)
    old_property.pop('content_hash', None)
    old_property.pop('scraped_at', None)
    old_property.pop('job_id', None)
    old_property.pop('_id', None)  # Remove MongoDB _id for comparison
    
    # Create new property (simulating current state with $89k)
    new_property = copy.deepcopy(property_doc)
    # Ensure it has the $89k price (might already be 89k)
    if 'financial' not in new_property:
        new_property['financial'] = {}
    new_property['financial']['list_price'] = 89000.0
    new_property['financial']['price_per_sqft'] = 47  # 89000 / 1908 = 46.65
    # Remove enrichment and metadata that will be regenerated
    new_property.pop('enrichment', None)
    new_property.pop('has_price_reduction', None)
    new_property.pop('is_motivated_seller', None)
    new_property.pop('has_distress_signals', None)
    new_property.pop('last_enriched_at', None)
    new_property.pop('_id', None)  # Remove MongoDB _id for comparison
    
    print(f"\n[STEP 2] Testing PropertyDiffer with:")
    print(f"   Old Price: ${old_property['financial']['list_price']:,}")
    print(f"   New Price: ${new_property['financial']['list_price']:,}")
    
    # Test PropertyDiffer
    property_differ = PropertyDiffer()
    changes = property_differ.detect_changes(old_property, new_property)
    
    print(f"\n[STEP 3] Change Detection Results:")
    print(f"   Has Changes: {changes.get('has_changes', False)}")
    print(f"   Total Changes: {changes.get('summary', {}).get('total_changes', 0)}")
    print(f"   Price Changes Count: {changes.get('summary', {}).get('price_changes_count', 0)}")
    
    if changes.get('price_changes'):
        print(f"\n   [OK] Price changes detected:")
        for change in changes.get('price_changes', []):
            print(f"      Field: {change.get('field')}")
            print(f"      Old: ${change.get('old_value'):,}" if isinstance(change.get('old_value'), (int, float)) else f"      Old: {change.get('old_value')}")
            print(f"      New: ${change.get('new_value'):,}" if isinstance(change.get('new_value'), (int, float)) else f"      New: {change.get('new_value')}")
            print(f"      Type: {change.get('change_type')}")
        
        # Get price summary
        price_summary = property_differ.get_price_change_summary(changes)
        if price_summary:
            print(f"\n   [PRICE SUMMARY]:")
            print(f"      Old Price: ${price_summary.get('old_price'):,}")
            print(f"      New Price: ${price_summary.get('new_price'):,}")
            print(f"      Difference: ${price_summary.get('price_difference'):,}")
            print(f"      Percent Change: {price_summary.get('percent_change', 0):.2f}%")
            print(f"      Change Type: {price_summary.get('change_type')}")
            
            # Test recording history
            print(f"\n[STEP 4] Testing History Recording:")
            history_tracker = HistoryTracker(db.db)
            
            # Add timestamp to price_summary
            price_summary['timestamp'] = datetime.utcnow()
            
            recorded = await history_tracker.record_price_change(PROPERTY_ID, price_summary, "test_job")
            if recorded:
                print(f"   [OK] Price change recorded successfully")
                
                # Verify it was recorded
                price_history = await history_tracker.get_price_history(PROPERTY_ID, limit=5)
                print(f"   Price history entries: {len(price_history)}")
                for entry in price_history:
                    data = entry.get('data', {})
                    print(f"      - {entry.get('timestamp')}: ${data.get('old_price', 'N/A'):,} -> ${data.get('new_price', 'N/A'):,} ({data.get('percent_change', 0):.2f}%)")
            else:
                print(f"   [ERROR] Failed to record price change")
    else:
        print(f"\n   [ERROR] No price changes detected!")
        print(f"   This indicates a problem with PropertyDiffer")
        
        # Debug: check what fields PropertyDiffer is looking for
        print(f"\n   [DEBUG] PropertyDiffer price_fields: {property_differ.price_fields}")
        print(f"   [DEBUG] Checking if financial.list_price changed:")
        old_price = old_property.get('financial', {}).get('list_price')
        new_price = new_property.get('financial', {}).get('list_price')
        print(f"      Old: {old_price} (type: {type(old_price)})")
        print(f"      New: {new_price} (type: {type(new_price)})")
        print(f"      Equal: {old_price == new_price}")
        
        # Check all field changes
        if changes.get('field_changes'):
            print(f"\n   [DEBUG] Field changes detected:")
            for change in changes.get('field_changes', []):
                print(f"      {change.get('field')}: {change.get('old_value')} -> {change.get('new_value')}")
    
    # Test enrichment pipeline
    print(f"\n[STEP 5] Testing Enrichment Pipeline:")
    enrichment_pipeline = PropertyEnrichmentPipeline(db.db)
    await enrichment_pipeline.initialize()
    
    enrichment_data = await enrichment_pipeline.enrich_property(
        property_id=PROPERTY_ID,
        property_dict=new_property,
        existing_property=old_property,
        job_id="test_job_simulation"
    )
    
    print(f"   Enrichment completed")
    print(f"   Has Price Reduction: {enrichment_data.get('quick_access_flags', {}).get('has_price_reduction', False)}")
    print(f"   Price Changes Count: {enrichment_data.get('change_summary', {}).get('price_changes_count', 0)}")
    
    await db.disconnect()
    print("\n[OK] Done")

if __name__ == "__main__":
    asyncio.run(simulate_price_change())

